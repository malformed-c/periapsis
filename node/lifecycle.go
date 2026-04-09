package node

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/malformed-c/periapsis/internal/image"
	"github.com/malformed-c/periapsis/internal/volume"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/malformed-c/periapsis/internal/manager"
	"github.com/malformed-c/periapsis/internal/podutils"
	perigeos "github.com/malformed-c/periapsis/internal/runtime"
)

func (g *Gambit) CreatePod(ctx context.Context, pod *corev1.Pod) error {
	g.Logger.Info("CreatePod", "pawn", g.Config.Name, "namespace", pod.Namespace, "pod", pod.Name)

	if len(pod.Spec.Containers) == 0 {
		return nil
	}

	// Pod admission: reject if the pod's resource requests exceed available
	// node capacity. Prevents overcommit that cascading OOM kills.
	if reason := g.admitPod(pod); reason != "" {
		g.Logger.Warn("Pod admission rejected", "pod", pod.Name, "reason", reason)
		if g.EventRecorder != nil {
			g.EventRecorder.Eventf(pod, corev1.EventTypeWarning, "FailedAdmission", reason)
		}
		return fmt.Errorf("pod admission: %s", reason)
	}

	uid := string(pod.UID)

	// Check if already in-flight
	if g.store.AlreadyInFlight(uid) {
		g.Logger.Info("CreatePod: already in-flight, skipping", "pod", pod.Name)
		return nil
	}

	// After a restart HydrateFromRuntime re-populates store from running
	// systemd units. When the informer reconnects it calls CreatePod for
	// every pod it knows about — skip pods that are already running so we
	// don't tear down and recreate a healthy machine.
	if exists, wasStub := g.store.AlreadyRunning(uid, pod); exists {
		if wasStub {
			// Also initialize probe state — it was never set because
			// createPodSync was skipped for already-running pods.
			g.store.InitRestartState(pod)
		}
		g.Logger.Info("CreatePod: already running (hydrated from runtime), skipping", "pod", pod.Name)
		return nil
	}

	sagaCtx, cancel := context.WithCancel(context.Background())
	saga := &podSaga{cancel: cancel, done: make(chan struct{})}
	// Register immediately as Pending so GetPodStatus never returns NotFound
	// while the pod is queued waiting for a createSem slot. Without this,
	// VK interprets NotFound as the pod not existing and may issue DeletePod,
	// cancelling the saga and causing a create/cancel loop.
	g.store.RegisterPending(uid, pod, saga)

	go func() {
		defer close(saga.done)
		defer cancel()

		// Wait for a creation slot. If the saga is cancelled (DeletePod
		// arrived while we were queued) bail out without starting work.
		createSem := g.store.CreateSem()
		select {
		case createSem <- struct{}{}:
		case <-sagaCtx.Done():
			return
		}
		defer func() { <-createSem }()

		neverRestart := pod.Spec.RestartPolicy == corev1.RestartPolicyNever
		backoff := createBackoffInit
		for attempt := 1; ; attempt++ {
			err := g.createPodSync(sagaCtx, pod)
			if err == nil {
				return
			}
			g.Logger.Warn("CreatePod attempt failed",
				"pod", pod.Name, "attempt", attempt, "err", err)
			g.EventRecorder.Eventf(pod, corev1.EventTypeWarning, "FailedCreate",
				"creation attempt %d failed: %v", attempt, err)

			// restartPolicy: Never → don't retry, mark Failed immediately.
			if neverRestart {
				g.Logger.Error("CreatePod failed (restartPolicy=Never)", "pod", pod.Name, "err", err)
				g.markPodFailed(uid, pod, err)
				return
			}

			// After 5 consecutive failures push a CrashLoopBackOff waiting
			// status so kubectl describe / k8s events reflect reality.
			// The pod stays Pending (we keep retrying) but the container
			// statuses tell operators why it isn't starting.
			if attempt >= 5 {
				g.pushCrashLoopStatus(uid, pod, err)
			}

			// Pod stays in Pending phase during retries — k8s sees it as
			// "still being created" and won't schedule an overshoot replacement.
			select {
			case <-time.After(backoff):
			case <-sagaCtx.Done():
				return
			}
			backoff *= 2
			if backoff > createBackoffMax {
				backoff = createBackoffMax
			}
		}
	}()

	return nil
}

// extractResourceLimits reads memory and CPU limits from a container spec.
func extractResourceLimits(c *corev1.Container) (memBytes uint64, cpuMillis int64) {
	if c.Resources.Limits != nil {
		if mem, ok := c.Resources.Limits[corev1.ResourceMemory]; ok {
			memBytes = uint64(mem.Value())
		}
		if cpu, ok := c.Resources.Limits[corev1.ResourceCPU]; ok {
			cpuMillis = cpu.MilliValue()
		}
	}
	return
}

// isPrivileged reports whether the container requests privileged mode.
func isPrivileged(c *corev1.Container) bool {
	return c.SecurityContext != nil &&
		c.SecurityContext.Privileged != nil &&
		*c.SecurityContext.Privileged
}

// createPodSync runs the full pod creation sequence within a saga.
// Each completed step registers a compensation; on any failure (including
// context cancellation from a concurrent DeletePod) compensations run in
// reverse, ensuring no partial state is left on disk or in systemd.
func (g *Gambit) createPodSync(ctx context.Context, pod *corev1.Pod) error {
	uid := string(pod.UID)
	g.setKind(pod)

	// Pre-flight: verify machined can accept new registrations.
	// Failing fast here avoids a 60s waitForContainer timeout when
	// nspawn exits immediately with "Failed to pin client process: Too many open files".
	if err := g.Runtime.CheckMachined(ctx); err != nil {
		g.EventRecorder.Eventf(pod, corev1.EventTypeWarning, "FailedPreFlight", "machined check failed: %v", err)
		return fmt.Errorf("machined pre-flight: %w", err)
	}

	saga := NewSaga(pod, g.Logger, g.EventRecorder)

	// Step 1: network setup.
	// HostNetwork pods share the host network namespace — skip CNI and use
	// /proc/1/ns/net directly so the container joins the host netns.
	var netPath, podIP string
	if pod.Spec.HostNetwork {
		netPath = "/proc/1/ns/net"
		podIP = resolveNodeIP(g.Config)
	} else {
		var err error
		netPath, podIP, err = g.NetworkManager.Setup(ctx, uid, pod.Namespace, pod.Name, pod.Spec.NodeName)
		if err != nil {
			g.EventRecorder.Eventf(pod, corev1.EventTypeWarning, "FailedNetwork", "CNI setup failed: %v", err)
			return fmt.Errorf("network setup: %w", err)
		}
		saga.Add("network", func(ctx context.Context) {
			_ = g.NetworkManager.Teardown(ctx, uid, pod.Namespace, pod.Name)
		})
	}

	// Register pod workspace cleanup as early compensation (runs last in LIFO).
	saga.Add("workspace", func(_ context.Context) {
		volResolver := volume.NewResolver(g.Config.BaseDir, g.Config.Name, uid, g.hostNodeName, nil, nil, nil)
		_ = volResolver.Cleanup()
		podDir := filepath.Join(g.Config.BaseDir, "pawns", g.Config.Name, "pods", uid)
		_ = os.RemoveAll(podDir)
	})

	// Populate environment variables now that podIP is known.
	// This resolves ConfigMap/Secret envFrom, FieldRef (including status.podIP),
	// service env vars, and $(var) expansion — all with the correct podIP.
	pod.Status.PodIP = podIP
	pod.Status.PodIPs = []corev1.PodIP{{IP: podIP}}
	pod.Status.HostIP = resolveNodeIP(g.Config)
	rm, _ := manager.NewResourceManager(nil, g.secretLister, g.cmLister, g.svcLister)
	if err := podutils.PopulateEnvironmentVariables(ctx, pod, rm, g.EventRecorder); err != nil {
		g.EventRecorder.Eventf(pod, corev1.EventTypeWarning, "FailedPopulateEnv", "environment variable resolution failed: %v", err)
		saga.Compensate()
		return fmt.Errorf("env population: %w", err)
	}

	// Pod-level image pull cache for init and app containers.
	// PullWithOptions has its own layer-level caching, but this avoids
	// re-pulling the same image for multiple containers in the same pod.
	type pullCacheEntry struct {
		layers []string
		cached bool
	}
	pullCache := make(map[string]pullCacheEntry)

	// Run init containers sequentially; each must exit 0 before the next starts.
	for i := range pod.Spec.InitContainers {
		ic := &pod.Spec.InitContainers[i]
		g.Logger.Info("Starting init container", "pod", pod.Name, "container", ic.Name)

		// Check pod-level cache first before pulling from registry.
		var layers []string
		var cached bool
		if entry, hit := pullCache[ic.Image]; hit {
			// Cache hit: reuse layers from previous pull in this pod.
			layers = entry.layers
			cached = true
		} else {
			// Cache miss: pull normally and store result.
			g.EventRecorder.Eventf(pod, corev1.EventTypeNormal, "Pulling", "Pulling image %s for init container %s", ic.Image, ic.Name)
			var pullCached bool
			var err error
			layers, pullCached, err = g.ImageManager.PullWithOptions(ic.Image, string(ic.ImagePullPolicy),
				image.PullOptions{
					Progress: pullProgressFunc(g, pod, ic.Image, ic.Name),
					Event:    podEventFn(g, pod),
				})
			if err != nil {
				g.EventRecorder.Eventf(pod, corev1.EventTypeWarning, "FailedPull", "Pull %s: %v", ic.Name, err)
				saga.Compensate()
				return fmt.Errorf("pull init container %s: %w", ic.Name, err)
			}
			// Cache the successful pull result.
			pullCache[ic.Image] = pullCacheEntry{layers: layers, cached: pullCached}
		}
		if cached {
			g.EventRecorder.Eventf(pod, corev1.EventTypeNormal, "Cached", "Image %s already present (pod cache) for init container %s", ic.Image, ic.Name)
		} else {
			g.EventRecorder.Eventf(pod, corev1.EventTypeNormal, "Pulled", "Pulled image %s for init container %s", ic.Image, ic.Name)
		}

		rootfs, err := g.ImageManager.Mount(uid+"-"+ic.Name, layers)
		if err != nil {
			g.EventRecorder.Eventf(pod, corev1.EventTypeWarning, "FailedMount", "Mount %s: %v", ic.Name, err)
			saga.Compensate()
			return fmt.Errorf("mount init container %s: %w", ic.Name, err)
		}
		g.EventRecorder.Eventf(pod, corev1.EventTypeNormal, "Created", "Created init container %s", ic.Name)
		// Init containers are ephemeral: unmount immediately after they exit.
		// No compensation registered — we unmount inline below.

		resolvedEnv := g.Tidal.ResolveEnv(pod, ic, podIP)
		volResolver := volume.NewResolver(g.Config.BaseDir, g.Config.Name, uid, g.hostNodeName, g.cmLister, g.secretLister, g.kubeClient)
		bindMounts, err := volResolver.Resolve(ctx, pod, ic)
		if err != nil {
			g.EventRecorder.Eventf(pod, corev1.EventTypeWarning, "FailedMount",
				"Volume resolution failed for init container %s: %v", ic.Name, err)
			_ = g.ImageManager.Unmount(uid + "-" + ic.Name)
			saga.Compensate()
			return fmt.Errorf("volume resolution for init container %s: %w", ic.Name, err)
		}
		if g.clusterDNS != "" {
			if err := writeResolvConf(rootfs, g.clusterDNS); err != nil {
				g.Logger.Warn("Failed to write resolv.conf for init container", "container", ic.Name, "err", err)
			}
		}
		icMemLimit, icCPULimit := extractResourceLimits(ic)
		icEP, icCmd := g.ImageManager.ImageEntrypoint(ic.Image)
		cfg := perigeos.PodConfig{
			Name:             pod.Name,
			Namespace:        pod.Namespace,
			UID:              uid,
			ContainerName:    ic.Name,
			Container:        ic,
			PawnName:         g.Config.Name,
			RootFS:           rootfs,
			BindMounts:       bindMounts,
			NetNSPath:        netPath,
			HostNetwork:      pod.Spec.HostNetwork,
			HostPID:          pod.Spec.HostPID,
			Privileged:       isPrivileged(ic),
			Environment:      resolvedEnv,
			PodIP:            podIP,
			MemoryLimitBytes: icMemLimit,
			CPULimitMillis:   icCPULimit,
			ImageEntrypoint:  icEP,
			ImageCmd:         icCmd,
		}

		if err := g.Runtime.RunMachine(ctx, uid, cfg); err != nil {
			_ = g.ImageManager.Unmount(uid + "-" + ic.Name)
			saga.Compensate()
			return fmt.Errorf("start init container %s: %w", ic.Name, err)
		}

		state, err := g.Runtime.WaitForMachineExit(ctx, uid, ic.Name, initContainerTimeout)

		if err != nil {
			// Context cancel or timeout — stop the machine before cleanup.
			_ = g.Runtime.StopMachine(context.Background(), uid, ic.Name)
			_ = g.ImageManager.Unmount(uid + "-" + ic.Name)
			g.EventRecorder.Eventf(pod, corev1.EventTypeWarning, "FailedInit", "Init container %s: %v", ic.Name, err)
			saga.Compensate()
			return fmt.Errorf("init container %s: %w", ic.Name, err)
		}
		_ = g.ImageManager.Unmount(uid + "-" + ic.Name)
		if state == perigeos.StateFailed {
			g.EventRecorder.Eventf(pod, corev1.EventTypeWarning, "FailedInit", "Init container %s exited with error", ic.Name)
			saga.Compensate()
			return fmt.Errorf("init container %s failed", ic.Name)
		}

		g.Logger.Info("Init container completed", "pod", pod.Name, "container", ic.Name)
	}

	// Step 2+: pull and mount each app container, registering compensations as we go.
	type launchResult struct {
		name    string
		cleanup func(context.Context)
		err     error
	}
	results := make(chan launchResult, len(pod.Spec.Containers))

	for i := range pod.Spec.Containers {
		c := &pod.Spec.Containers[i]
		go func() {
			g.EventRecorder.Eventf(pod, corev1.EventTypeNormal, "Pulling", "Pulling image %s for container %s", c.Image, c.Name)
			cleanup, err := g.launchContainer(ctx, pod, c, uid, netPath, podIP)
			if err == nil {
				g.EventRecorder.Eventf(pod, corev1.EventTypeNormal, "Created", "Created container %s", c.Name)
			} else {
				g.EventRecorder.Eventf(pod, corev1.EventTypeWarning, "FailedStart", "Container %s: %v", c.Name, err)
			}
			results <- launchResult{c.Name, cleanup, err}
		}()
	}

	var launchErr error
	for range pod.Spec.Containers {
		r := <-results
		if r.err != nil {
			if launchErr == nil {
				launchErr = fmt.Errorf("container %s: %w", r.name, r.err)
			}
		}
		if r.cleanup != nil {
			saga.Add("container/"+r.name, r.cleanup)
		}
	}

	if launchErr != nil {
		saga.Compensate()
		return launchErr
	}

	// Promote pod from Pending to Running and record IP.
	g.store.PromoteRunning(uid, pod, podIP)

	// Initialize probe state before the first status push so containers
	// with readiness probes start as not-ready instead of defaulting to
	// ready (the IsContainerReady fallback returns true when no probe
	// state exists yet).
	g.store.InitRestartState(pod)

	// Index volume-mounted ConfigMaps/Secrets for live refresh.
	g.volumes.Track(uid, pod)

	g.Logger.Info("Pod started successfully", "pod", pod.Name, "ip", podIP,
		"containers", len(pod.Spec.Containers))
	g.EventRecorder.Eventf(pod, corev1.EventTypeNormal, "Started", "Started pod %s", pod.Name)

	// Push Running status to PodController immediately. We construct the
	// status directly rather than calling GetPodStatus, because the
	// BatchWatcher cache may contain stale/intermediate states from
	// systemd's unit startup sequence (brief inactive→active transition).
	{
		updated := pod.DeepCopy()
		updated.Status.Phase = corev1.PodRunning
		updated.Status.HostIP = resolveNodeIP(g.Config)
		updated.Status.PodIP = podIP
		now := metav1.NewTime(time.Now())
		updated.Status.StartTime = &now
		allReady := true
		for _, c := range pod.Spec.Containers {
			ready := g.isContainerReady(uid, c.Name)
			if !ready {
				allReady = false
			}
			updated.Status.ContainerStatuses = append(updated.Status.ContainerStatuses, corev1.ContainerStatus{
				Name:  c.Name,
				Image: c.Image,
				Ready: ready,
				State: corev1.ContainerState{
					Running: &corev1.ContainerStateRunning{StartedAt: now},
				},
			})
		}
		readyCondition := corev1.ConditionFalse
		if allReady {
			readyCondition = corev1.ConditionTrue
		}
		updated.Status.Conditions = []corev1.PodCondition{{
			Type:   corev1.PodReady,
			Status: readyCondition,
		}}
		g.notifyPodStatus(updated)
	}

	// Mark all containers as seen-running in the BatchWatcher so it knows
	// they actually started (even if the D-Bus "running" event arrives
	// after the unit already exited for fast-exit containers).
	if g.batchWatcher != nil {
		for _, c := range pod.Spec.Containers {
			g.batchWatcher.MarkRunning(uid, c.Name)
		}
	}

	// Don't Poke here — the BatchWatcher will detect the exit via D-Bus
	// events (SubState=failed for non-zero exits, immediate) or the next
	// ticker poll (2s, for clean exits). Poking immediately would race
	// with systemd's ExecMainStatus update, causing exit-1 containers
	// to be misclassified as Succeeded.

	return nil
}

// waitForContainer polls MachineStatus until the container is Running or timeout.
// launchContainer consolidates the duplication between createPodSync and restartContainer.
// It handles image pull, overlay mount, environment resolution, volume resolution,
// DNS setup, resource extraction, PodConfig building, machine startup, and waiting.
// On any error after mount, it unmounts before returning. On RunMachine success but
// waitForContainer failure, it stops the machine and unmounts.
func (g *Gambit) launchContainer(
	ctx context.Context,
	pod *corev1.Pod,
	c *corev1.Container,
	uid, netPath, podIP string,
) (cleanup func(context.Context), err error) {
	// Pull image
	layers, _, err := g.ImageManager.PullWithOptions(c.Image, string(c.ImagePullPolicy),
		image.PullOptions{
			Progress: pullProgressFunc(g, pod, c.Image, c.Name),
			Event:    podEventFn(g, pod),
		})
	if err != nil {
		return nil, fmt.Errorf("pull: %w", err)
	}

	// Mount overlay
	rootfs, err := g.ImageManager.Mount(uid+"-"+c.Name, layers)
	if err != nil {
		return nil, fmt.Errorf("mount: %w", err)
	}

	// Cleanup function to stop machine and unmount on exit or failure
	cleanup = func(ctx context.Context) {
		_ = g.Runtime.StopMachine(ctx, uid, c.Name)
		_ = g.ImageManager.Unmount(uid + "-" + c.Name)
	}

	// Resolve environment variables
	resolvedEnv := g.Tidal.ResolveEnv(pod, c, podIP)

	// Resolve volumes
	volResolver := volume.NewResolver(g.Config.BaseDir, g.Config.Name, uid, g.hostNodeName, g.cmLister, g.secretLister, g.kubeClient)
	bindMounts, err := volResolver.Resolve(ctx, pod, c)
	if err != nil {
		cleanup(context.Background())
		return nil, fmt.Errorf("volume resolution: %w", err)
	}

	// Write resolv.conf if cluster DNS is set
	if g.clusterDNS != "" {
		if err := writeResolvConf(rootfs, g.clusterDNS); err != nil {
			g.Logger.Warn("Failed to write resolv.conf", "container", c.Name, "err", err)
		}
	}

	// Extract resource limits and image entrypoint
	memLimit, cpuLimit := extractResourceLimits(c)
	ep, cmd := g.ImageManager.ImageEntrypoint(c.Image)

	// Build PodConfig with all fields
	cfg := perigeos.PodConfig{
		Name:                          pod.Name,
		Namespace:                     pod.Namespace,
		UID:                           uid,
		ContainerName:                 c.Name,
		Container:                     c,
		PawnName:                      g.Config.Name,
		RootFS:                        rootfs,
		BindMounts:                    bindMounts,
		NetNSPath:                     netPath,
		HostNetwork:                   pod.Spec.HostNetwork,
		HostPID:                       pod.Spec.HostPID,
		Privileged:                    isPrivileged(c),
		Environment:                   resolvedEnv,
		PodIP:                         podIP,
		MemoryLimitBytes:              memLimit,
		CPULimitMillis:                cpuLimit,
		ImageEntrypoint:               ep,
		ImageCmd:                      cmd,
		TerminationGracePeriodSeconds: podTerminationGracePeriod(pod),
	}

	// Start the machine
	if err := g.Runtime.RunMachine(ctx, uid, cfg); err != nil {
		cleanup(context.Background())
		return nil, fmt.Errorf("RunMachine: %w", err)
	}

	// Wait for container to become running (or fast-exit)
	if err := g.waitForContainer(ctx, uid, c.Name, machineStartTimeout); err != nil {
		cleanup(context.Background())
		return nil, fmt.Errorf("waitForContainer: %w", err)
	}

	// Enable bidirectional mount propagation now that the container is confirmed
	// running. MakeSharedMounts needs the machine registered with machined so it
	// can resolve the leader PID — calling it inside RunMachine (before
	// waitForContainer) races with nspawn registration and fails with "no MainPID".
	if err := g.Runtime.MakeSharedMounts(ctx, uid, c.Name, cfg.BindMounts); err != nil {
		cleanup(context.Background())
		return nil, fmt.Errorf("MakeSharedMounts: %w", err)
	}

	// Run PostStart lifecycle hook. Per the k8s spec, PostStart runs
	// immediately after a container is created. If it fails, the container
	// is killed (we run cleanup and return an error).
	if c.Lifecycle != nil && c.Lifecycle.PostStart != nil {
		if err := g.runLifecycleHook(ctx, pod, c, uid, c.Lifecycle.PostStart, "PostStart"); err != nil {
			g.EventRecorder.Eventf(pod, corev1.EventTypeWarning, "FailedPostStartHook",
				"PostStart hook failed for container %s: %v", c.Name, err)
			cleanup(context.Background())
			return nil, fmt.Errorf("PostStart hook: %w", err)
		}
	}

	return cleanup, nil
}

func (g *Gambit) waitForContainer(ctx context.Context, uid, containerName string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		state, err := g.Runtime.MachineStatus(ctx, uid, containerName)
		if err == nil {
			switch state {
			case perigeos.StateRunning:
				return nil
			case perigeos.StateExited, perigeos.StateFailed:
				// Container already ran and exited (fast-exit containers like
				// certgen finish before the first poll). This is success from
				// the perspective of "the machine started" — the BatchWatcher
				// will handle terminal phase transitions and exit codes.
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	return fmt.Errorf("container %s/%s did not become running within %s", uid, containerName, timeout)
}

// markPodFailed records a pod as Failed in the internal maps and pushes
// the terminal status to the PodController.
func (g *Gambit) markPodFailed(uid string, pod *corev1.Pod, err error) {
	g.EventRecorder.Eventf(pod, corev1.EventTypeWarning, "CreateFailed", "pod creation failed: %v", err)
	failedPod := g.store.MarkFailed(uid, pod, "CreateFailed", err.Error())
	g.notifyPodStatus(failedPod)
}

// pushCrashLoopStatus updates the pod's ContainerStatuses to show
// CrashLoopBackOff for all containers. Called after repeated create failures
// so that kubectl describe and k8s events reflect why the pod isn't starting.
// The pod phase stays Pending — we keep retrying.
func (g *Gambit) pushCrashLoopStatus(uid string, pod *corev1.Pod, lastErr error) {
	updated := pod.DeepCopy()
	updated.Status.Phase = corev1.PodPending
	updated.Status.ContainerStatuses = nil
	for _, c := range pod.Spec.Containers {
		updated.Status.ContainerStatuses = append(updated.Status.ContainerStatuses, corev1.ContainerStatus{
			Name:  c.Name,
			Image: c.Image,
			State: corev1.ContainerState{
				Waiting: &corev1.ContainerStateWaiting{
					Reason:  "CrashLoopBackOff",
					Message: lastErr.Error(),
				},
			},
		})
	}
	g.notifyPodStatus(updated)
}

// isContainerReady returns whether a container should be reported as Ready.
// If no readiness probe is defined, defaults to true (set in initRestartState).
func (g *Gambit) isContainerReady(uid, containerName string) bool {
	return g.store.IsContainerReady(uid, containerName)
}

// restartContainer implements a single container restart with CrashLoopBackOff.
func (g *Gambit) restartContainer(ctx context.Context, uid string, pod *corev1.Pod, containerName string) {
	// Don't start new machines during graceful shutdown.
	if g.node.IsShuttingDown() {
		return
	}

	count, backoff := g.store.BumpBackoff(uid, containerName)
	if count == 0 {
		// No restart state found
		return
	}

	g.Logger.Info("Restarting container (CrashLoopBackOff)",
		"pod", pod.Name, "container", containerName,
		"restartCount", count, "backoff", backoff)
	g.EventRecorder.Eventf(pod, corev1.EventTypeWarning, "BackOff",
		"Back-off restarting container %s (count: %d)", containerName, count)

	select {
	case <-time.After(backoff):
	case <-ctx.Done():
		return
	}

	// Stop the old machine (may already be stopped).
	g.EventRecorder.Eventf(pod, corev1.EventTypeNormal, "Killing", "Restarting container %s", containerName)
	_ = g.Runtime.StopMachine(ctx, uid, containerName)

	// Find the container spec to rebuild the config.
	var container *corev1.Container
	for i := range pod.Spec.Containers {
		if pod.Spec.Containers[i].Name == containerName {
			container = &pod.Spec.Containers[i]
			break
		}
	}
	if container == nil {
		return
	}

	// Unmount any leftover overlay from the previous run.
	_ = g.ImageManager.Unmount(uid + "-" + containerName)

	// Pre-flight: verify machined health before starting.
	if err := g.Runtime.CheckMachined(ctx); err != nil {
		g.Logger.Error("Restart: machined unhealthy, skipping", "container", containerName, "err", err)
		return
	}

	podIP := g.store.PodIP(uid)

	var netPath string
	if pod.Spec.HostNetwork {
		netPath = "/proc/1/ns/net"
	} else {
		netPath = filepath.Join("/var/run/netns", "peri-"+uid)
	}

	_, err := g.launchContainer(ctx, pod, container, uid, netPath, podIP)
	if err != nil {
		g.Logger.Error("Restart: launch failed", "container", containerName, "err", err)
		return
	}

	g.store.MarkRestarted(uid, containerName)

	g.Logger.Info("Container restarted successfully", "pod", pod.Name, "container", containerName)

	// Push updated status immediately so restartCount is visible in k8s
	// without waiting for the next batch watcher cycle.
	restartedPod := g.store.GetPodCopy(uid)
	if restartedPod != nil {
		status := g.buildPodStatus(restartedPod, func(u, cn string) perigeos.MachineState {
			state, err := g.Runtime.MachineStatus(ctx, u, cn)
			if err != nil {
				return perigeos.StateUnknown
			}
			return state
		})
		updated := restartedPod.DeepCopy()
		status.DeepCopyInto(&updated.Status)
		g.notifyPodStatus(updated)
	}
}

func (g *Gambit) UpdatePod(_ context.Context, pod *corev1.Pod) error {
	g.Logger.Info("UpdatePod", "pawn", g.Config.Name, "pod", pod.Name)
	return nil
}

func (g *Gambit) DeletePod(ctx context.Context, pod *corev1.Pod) error {
	var caller string
	if _, file, line, ok := runtime.Caller(1); ok {
		caller = fmt.Sprintf("%s:%d", filepath.Base(file), line)
	}
	g.Logger.Info("DeletePod", "pawn", g.Config.Name, "namespace", pod.Namespace, "name", pod.Name, "caller", caller)
	uid := string(pod.UID)
	g.setKind(pod)

	// Mark pod as deleting so the batch watcher won't restart its containers.
	g.store.MarkDeleting(uid)

	// If a CreatePod saga is running, cancel it and wait for its compensations
	// to finish before proceeding. This closes the race where DeletePod arrives
	// while the CNI is still allocating an IP or a machine is starting.
	g.cancelInFlight(uid)

	// Enforce terminationGracePeriodSeconds. PreStop hooks + container stop
	// share this budget. If PreStop consumes part of it, the remaining time
	// is available for SIGTERM before systemd sends SIGKILL.
	gracePeriod := podTerminationGracePeriod(pod)
	if gracePeriod > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(gracePeriod)*time.Second)
		defer cancel()
	}

	// Run PreStop lifecycle hooks before stopping containers.
	g.runPreStopHooks(ctx, pod, uid)

	// Stop all app containers. systemd sends SIGTERM → waits TimeoutStopSec
	// (set from terminationGracePeriodSeconds at unit creation) → SIGKILL.
	for _, c := range pod.Spec.Containers {
		if err := g.Runtime.StopMachine(ctx, uid, c.Name); err != nil {
			g.Logger.Error("Failed to stop container", "container", c.Name, "err", err)
		}
		if err := g.ImageManager.Unmount(uid + "-" + c.Name); err != nil {
			g.Logger.Error("Failed to unmount container overlay", "container", c.Name, "err", err)
		}
	}
	// Stop any init containers that may still be running (edge case).
	for _, c := range pod.Spec.InitContainers {
		if err := g.Runtime.StopMachine(ctx, uid, c.Name); err != nil {
			g.Logger.Error("Failed to stop init container", "container", c.Name, "err", err)
		}
		if err := g.ImageManager.Unmount(uid + "-" + c.Name); err != nil {
			g.Logger.Error("Failed to unmount init container overlay", "container", c.Name, "err", err)
		}
	}

	// HostNetwork pods share the host netns — no CNI netns was created.
	if !pod.Spec.HostNetwork {
		if err := g.NetworkManager.Teardown(ctx, uid, pod.Namespace, pod.Name); err != nil {
			g.Logger.Error("Failed to teardown network", "err", err)
		}
	}

	// Clean up any host-side volume state (emptyDir, configMap projections, CSI mounts).
	volResolver := volume.NewResolver(g.Config.BaseDir, g.Config.Name, uid, g.hostNodeName, nil, nil, g.kubeClient)
	// First, clean up CSI volumes if kubeClient is available
	if err := volResolver.CleanupCSI(ctx, pod); err != nil {
		g.Logger.Warn("CSI cleanup failed (non-fatal)", "uid", uid, "err", err)
	}
	// Then clean up the state directory
	if err := volResolver.Cleanup(); err != nil {
		g.Logger.Warn("Volume cleanup failed (non-fatal)", "uid", uid, "err", err)
	}

	// Clean up pod workspace directory.
	podDir := filepath.Join(g.Config.BaseDir, "pawns", g.Config.Name, "pods", uid)
	if err := os.RemoveAll(podDir); err != nil {
		g.Logger.Warn("Failed to remove pod directory", "uid", uid, "err", err)
	}

	g.volumes.Untrack(uid)
	g.store.Unregister(uid, pod.Namespace, pod.Name)

	if err := deletePodState(g.Config.BaseDir, g.Config.Name, uid); err != nil {
		g.Logger.Warn("Failed to delete pod state", "pod", pod.Name, "err", err)
	}

	return nil
}

// admitPod checks if the pod's resource requests fit within remaining node capacity.
// Returns an empty string if admitted, or a reason string if rejected.
func (g *Gambit) admitPod(pod *corev1.Pod) string {
	return g.store.AdmitPod(pod, g.Config.CPU, g.Config.Memory)
}

// pullProgressFunc returns a callback that emits Pulling events at 10% steps.
// podEventFn returns an image.PullEventFn that forwards layer pull events as
// pod events on the given pod.
func podEventFn(g *Gambit, pod *corev1.Pod) image.PullEventFn {
	return func(eventType, reason, message string) {
		g.EventRecorder.Eventf(pod, eventType, reason, "%s", message)
	}
}

func pullProgressFunc(g *Gambit, pod *corev1.Pod, imageName, containerName string) image.PullProgress {
	var lastPct int
	return func(done, total int) {
		if total == 0 {
			return
		}
		pct := done * 100 / total
		// Emit at every 10% boundary crossed, and always at 100%.
		step := pct / 10 * 10
		if step > lastPct || pct == 100 {
			g.EventRecorder.Eventf(pod, corev1.EventTypeNormal, "Pulling",
				"Pulling image %s: %d%% (%d/%d layers) for %s", imageName, pct, done, total, containerName)
			lastPct = step
		}
	}
}

// podTerminationGracePeriod returns the pod's termination grace period in seconds.
// Defaults to 30 (Kubernetes default) if not set.
func podTerminationGracePeriod(pod *corev1.Pod) int64 {
	if pod.Spec.TerminationGracePeriodSeconds != nil {
		return *pod.Spec.TerminationGracePeriodSeconds
	}
	return 30
}

// runLifecycleHook executes a single lifecycle hook (PostStart or PreStop).
// Returns an error if the hook fails. The caller decides whether to act on it
// (PostStart failures kill the container; PreStop failures are logged only).
func (g *Gambit) runLifecycleHook(ctx context.Context, pod *corev1.Pod, c *corev1.Container, uid string, handler *corev1.LifecycleHandler, hookName string) error {
	switch {
	case handler.Exec != nil:
		g.Logger.Info("Running lifecycle hook", "hook", hookName, "container", c.Name, "pod", pod.Name, "type", "exec")
		return g.Runtime.RunInContainer(ctx, uid, c.Name, handler.Exec.Command, &noopAttachIO{})

	case handler.HTTPGet != nil:
		g.Logger.Info("Running lifecycle hook", "hook", hookName, "container", c.Name, "pod", pod.Name, "type", "httpGet")
		podIP := g.store.PodIP(uid)

		host := handler.HTTPGet.Host
		if host == "" {
			host = podIP
		}
		if host == "" {
			return fmt.Errorf("no host/podIP available")
		}

		port := handler.HTTPGet.Port.String()
		scheme := string(handler.HTTPGet.Scheme)
		if scheme == "" {
			scheme = "http"
		}

		url := fmt.Sprintf("%s://%s:%s%s", strings.ToLower(scheme), host, port, handler.HTTPGet.Path)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return fmt.Errorf("bad request: %w", err)
		}
		for _, h := range handler.HTTPGet.HTTPHeaders {
			req.Header.Set(h.Name, h.Value)
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return fmt.Errorf("HTTP request failed: %w", err)
		}
		resp.Body.Close()

		if resp.StatusCode >= 300 {
			return fmt.Errorf("HTTP status %d", resp.StatusCode)
		}
		return nil

	case handler.Sleep != nil:
		g.Logger.Info("Running lifecycle hook", "hook", hookName, "container", c.Name, "pod", pod.Name, "type", "sleep")
		time.Sleep(time.Duration(handler.Sleep.Seconds) * time.Second)
		return nil

	default:
		return nil
	}
}

// runPreStopHooks executes PreStop lifecycle hooks for all running containers.
// Hooks run within the parent context's deadline (shared with the stop budget).
// Errors are logged but do not prevent container shutdown.
func (g *Gambit) runPreStopHooks(ctx context.Context, pod *corev1.Pod, uid string) {
	for i := range pod.Spec.Containers {
		c := &pod.Spec.Containers[i]
		if c.Lifecycle == nil || c.Lifecycle.PreStop == nil {
			continue
		}
		if err := g.runLifecycleHook(ctx, pod, c, uid, c.Lifecycle.PreStop, "PreStop"); err != nil {
			g.Logger.Warn("PreStop hook failed", "container", c.Name, "err", err)
			if g.EventRecorder != nil {
				g.EventRecorder.Eventf(pod, corev1.EventTypeWarning, "FailedPreStopHook",
					"PreStop hook failed for container %s: %v", c.Name, err)
			}
		}
	}
}

// ─── Pod Queries ─────────────────────────────────────────────────────────────
