package node

import (
        "context"
        "fmt"
        "net/http"
        "os"
        "path/filepath"
        "strings"
        "time"

        "github.com/malformed-c/periapsis/internal/image"
        "github.com/malformed-c/periapsis/internal/manager"
        "github.com/malformed-c/periapsis/internal/podutils"
        "github.com/malformed-c/periapsis/internal/volume"
        corev1 "k8s.io/api/core/v1"
        metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

        perigeos "github.com/malformed-c/periapsis/internal/runtime"
)

type pullCacheEntry struct {
        layers []string
        cached bool
}

type containerRuntimeProfile struct {
        MemoryLimitBytes uint64
        CPULimitMillis   int64
        CPURequestMillis int64
        RunAsUser        *int64
        RunAsGroup       *int64
}

func (g *Gambit) CreatePod(ctx context.Context, pod *corev1.Pod) error {
        g.Logger.Info("CreatePod", "pawn", g.Config.Name, "namespace", pod.Namespace, "pod", pod.Name)

        if len(pod.Spec.Containers) == 0 {
                return nil
        }

        // 1. Admission Check
        if reason := g.admitPod(pod); reason != "" {
                g.Logger.Warn("Pod admission rejected", "pod", pod.Name, "reason", reason)
                if g.EventRecorder != nil {
                        g.EventRecorder.Eventf(pod, corev1.EventTypeWarning, "AdmissionFailed", reason)
                }
                return fmt.Errorf("pod admission: %s", reason)
        }

        uid := string(pod.UID)
        g.EventRecorder.Eventf(pod, corev1.EventTypeNormal, "Starting", "Starting pod creation process")

        // 2. Concurrency & Hydration Guard
        if g.store.AlreadyInFlight(uid) {
                g.Logger.Info("CreatePod: already in-flight, skipping", "pod", pod.Name)
                return nil
        }

        if exists, wasStub := g.store.AlreadyRunning(uid, pod); exists {
                if wasStub {
                        g.store.InitRestartState(pod)
                }
                g.Logger.Info("CreatePod: already running (hydrated), skipping", "pod", pod.Name)
                return nil
        }

        // 3. Register as Pending (In-Flight)
        sagaCtx, cancel := context.WithCancel(context.Background())
        handle := &creationHandle{cancel: cancel, done: make(chan struct{})}
        g.store.RegisterPending(uid, pod, handle)

        // 4. The Reconciler Worker Loop
        go func() {
                defer close(handle.done)
                defer cancel()

                createSem := g.store.CreateSem()
                select {
                case createSem <- struct{}{}:
                case <-sagaCtx.Done():
                        return
                }
                defer func() { <-createSem }()

                // Hoisted Pull Cache: Survives retries!
                pullCache := make(map[string]pullCacheEntry)
                backoff := createBackoffInit
                neverRestart := pod.Spec.RestartPolicy == corev1.RestartPolicyNever

                // Idempotent Retry Loop for the Sandbox (Network + Init Containers)
                for attempt := 1; ; attempt++ {
                        err := g.syncPodSandboxAndContainers(sagaCtx, pod, pullCache)
                        if err == nil {
                                return // Success! Network is up, Init containers finished, App containers injected.
                        }

                        g.Logger.Warn("Pod sandbox/init sync failed, backing off", "pod", pod.Name, "attempt", attempt, "err", err)
                        g.EventRecorder.Eventf(pod, corev1.EventTypeWarning, "CreateFailed", "sandbox/init attempt %d failed: %v", attempt, err)

                        if neverRestart {
                                g.Logger.Error("CreatePod failed (restartPolicy=Never)", "pod", pod.Name, "err", err)
                                g.markPodFailed(uid, pod, err)
                                return
                        }

                        g.EventRecorder.Eventf(pod, corev1.EventTypeWarning, "BackOff",
                                "Sandbox sync failed: %v. Retrying in %v", err, backoff)

                        // Sleep and retry. syncPodSandboxAndContainers will pick up right where it left off.
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

// syncPodSandboxAndContainers evaluates the current reality of the pod and pushes it forward.
// If any step fails, it returns an error immediately, allowing the outer loop to
// clean up and retry from scratch cleanly.
// syncPodSandboxAndContainers evaluates the current reality of the pod and pushes it forward.
// It builds the network sandbox and injects the containers into systemd.
func (g *Gambit) syncPodSandboxAndContainers(ctx context.Context, pod *corev1.Pod, pullCache map[string]pullCacheEntry) error {
        uid := string(pod.UID)

        // TODO rm
        // g.setKind(pod)

        // Step 1: Network Setup (Idempotent)
        podIP := g.store.PodIP(uid)
        var netPath string

        if pod.Spec.HostNetwork {
                netPath = "/proc/1/ns/net"
                podIP = resolveNodeIP(g.Config)
        } else if podIP == "" {
                var err error
                netPath, podIP, err = g.NetworkManager.Setup(ctx, uid, pod.Namespace, pod.Name, pod.Spec.NodeName)
                if err != nil {
                        g.EventRecorder.Eventf(pod, corev1.EventTypeWarning, "NetworkFailed", "CNI setup failed: %v", err)
                        return fmt.Errorf("network setup: %w", err)
                }
                g.EventRecorder.Eventf(pod, corev1.EventTypeNormal, "NetworkReady", "CNI network configured, podIP=%s", podIP)
                g.store.PromoteRunning(uid, pod, podIP)
        } else {
                netPath = filepath.Join("/var/run/netns", "peri-"+uid)
                g.EventRecorder.Eventf(pod, corev1.EventTypeNormal, "Reconciling", "Resuming creation: network sandbox already exists")
        }

        // Step 2: Environment Resolution
        pod.Status.PodIP = podIP
        pod.Status.PodIPs = []corev1.PodIP{{IP: podIP}}
        pod.Status.HostIP = resolveNodeIP(g.Config)
        rm, _ := manager.NewResourceManager(nil, g.secretLister, g.cmLister, g.svcLister)
        if err := podutils.PopulateEnvironmentVariables(ctx, pod, rm, g.EventRecorder); err != nil {
                g.EventRecorder.Eventf(pod, corev1.EventTypeWarning, "PopulateEnvFailed", "environment variable resolution failed: %v", err)
                return fmt.Errorf("env population: %w", err)
        }

        // Step 3: Init Containers (Strictly Sequential)
        runtimeProfiles := buildContainerRuntimeProfiles(pod)

        for i := range pod.Spec.InitContainers {
                ic := &pod.Spec.InitContainers[i]

                state := g.batchWatcher.ContainerState(uid, ic.Name)

                // K8s 1.29+ Native Sidecar Support (Init containers that stay running)
                isSidecar := ic.RestartPolicy != nil && *ic.RestartPolicy == corev1.ContainerRestartPolicyAlways

                if isSidecar && state == perigeos.StateRunning {
                        continue // Sidecar is up, move to next container
                } else if !isSidecar && state == perigeos.StateExited {
                        continue // Standard init container finished successfully, move to next
                }

                err := g.launchContainer(ctx, pod, ic, uid, netPath, podIP, pullCache, runtimeProfiles, !isSidecar)
                if err != nil {
                        // Init containers MUST succeed before proceeding. Abort and let BatchWatcher retry.
                        g.EventRecorder.Eventf(pod, corev1.EventTypeWarning, "InitFailed", "Init container %s: %v", ic.Name, err)
                        return fmt.Errorf("init container %s failed: %w", ic.Name, err)
                }
        }

        g.EventRecorder.Eventf(pod, corev1.EventTypeNormal, "SandboxReady", "Network and init containers prepared")

        // Step 4: App Containers (Concurrent eventual consistency)
        for i := range pod.Spec.Containers {
                c := &pod.Spec.Containers[i]

                state := g.batchWatcher.ContainerState(uid, c.Name)

                // If systemd already knows about it, skip. Let the BatchWatcher handle its lifecycle.
                if state == perigeos.StateRunning || state == perigeos.StateExited || state == perigeos.StateFailed {
                        continue
                }

                err := g.launchContainer(ctx, pod, c, uid, netPath, podIP, pullCache, runtimeProfiles, false)
                if err != nil {
                        // CRITICAL DIFFERENCE: DO NOT RETURN HERE!
                        // If driver-registrar crashes because the socket isn't ready, we just log it.
                        // We MUST continue the loop so the CSI Plugin gets launched and creates the socket!
                        // The BatchWatcher will CrashLoopBackOff this failed container automatically later.
                        g.EventRecorder.Eventf(pod, corev1.EventTypeWarning, "StartFailed", "Container %s: %v", c.Name, err)
                        g.Logger.Warn("App container failed to start (will crash-loop)", "container", c.Name, "err", err)
                } else {
                        g.EventRecorder.Eventf(pod, corev1.EventTypeNormal, "Created", "Created container %s", c.Name)
                }
        }

        // Step 5: Finalize state
        g.store.PromoteRunning(uid, pod, podIP)
        g.store.InitRestartState(pod)
        g.volumes.Track(uid, pod)

        g.Logger.Info("Pod sandbox and initial containers injected", "pod", pod.Name, "ip", podIP)
        g.EventRecorder.Eventf(pod, corev1.EventTypeNormal, "Started", "Started pod %s", pod.Name)

        // Push ContainerCreating - batchwatcher will promote to Running once
        // it observes the systemd unit in "running" substate via D-Bus.
        // Previously we pushed Running immediately here, which created a
        // window where K8s showed Running 1/1 even if the container died
        // before batchwatcher's first poll.
        //
        // Only push ContainerCreating if ALL of these hold:
        //  1. The BatchWatcher hasn't already observed any container running
        //  2. The store phase hasn't already been promoted past Pending
        //     (PromoteRunning was called above, so if anything set it to
        //      Running, a stale ContainerCreating push would race with the
        //      BatchWatcher's Running push and overwrite it in k8s).
        alreadyRunning := g.store.PodPhase(uid) == corev1.PodRunning
        if !alreadyRunning {
                for _, c := range pod.Spec.Containers {
                        if g.batchWatcher.ContainerState(uid, c.Name) == perigeos.StateRunning {
                                alreadyRunning = true
                                break
                        }
                }
        }

        if !alreadyRunning {
                g.pushContainerCreatingStatus(pod, podIP)
        } else {
                g.Logger.Debug("Skipping ContainerCreating status push; container already running", "pod", pod.Name)
        }

        return nil
}

// teardownPodIdempotent replaces the Saga compensations. It looks at the actual state
// of the node and violently scrubs any trace of the pod. It is safe to call repeatedly.
func (g *Gambit) teardownPodIdempotent(ctx context.Context, uid string, pod *corev1.Pod) {
        g.Logger.Info("Executing idempotent teardown for pod", "pod", pod.Name)
        g.EventRecorder.Eventf(pod, corev1.EventTypeNormal, "Killing", "Stopping pod %s", pod.Name)

        allContainers := append(pod.Spec.Containers, pod.Spec.InitContainers...)

        // 1. Systemd & Filesystem Scrubber
        for _, c := range allContainers {
                g.EventRecorder.Eventf(pod, corev1.EventTypeNormal, "Killing", "Stopping container %s", c.Name)

                // Stop unit (blocks until DBus confirms)
                _ = g.Runtime.StopMachine(ctx, uid, c.Name)

                // Clear failed fragment files from systemd
                _ = g.Runtime.ResetUnit(ctx, uid, c.Name)

                // Unmount the container filesystem
                _ = g.ImageManager.Unmount(uid + "-" + c.Name)

                g.EventRecorder.Eventf(pod, corev1.EventTypeNormal, "Killed", "Stopped container %s", c.Name)
        }

        // 2. Network Scrubber
        if !pod.Spec.HostNetwork {
                _ = g.NetworkManager.Teardown(ctx, uid, pod.Namespace, pod.Name)
                g.EventRecorder.Eventf(pod, corev1.EventTypeNormal, "NetworkTeardown", "CNI network released for pod %s", pod.Name)
        }

        // 3. Workspace Scrubber
        volResolver := volume.NewResolver(g.Config.BaseDir, g.Config.Name, uid, g.hostNodeName, nil, nil, g.kubeClient)
        _ = volResolver.CleanupCSI(ctx, pod)
        _ = volResolver.Cleanup()

        podDir := filepath.Join(g.Config.BaseDir, "pawns", g.Config.Name, "pods", uid)
        _ = os.RemoveAll(podDir)
}

// launchContainer handles pulling, mounting, starting, and waiting for a container.
// It uses the shared pullCache so retries do not spam the network.
func (g *Gambit) launchContainer(
        ctx context.Context,
        pod *corev1.Pod,
        c *corev1.Container,
        uid, netPath, podIP string,
        pullCache map[string]pullCacheEntry,
        runtimeProfiles map[string]containerRuntimeProfile,
        isInit bool,
) error {
        // 1. Check Pull Cache
        var layers []string
        if entry, hit := pullCache[c.Image]; hit {
                layers = entry.layers
        } else {
                g.EventRecorder.Eventf(pod, corev1.EventTypeNormal, "Pulling", "Pulling image %s for container %s", c.Image, c.Name)
                var err error
                var cached bool
                layers, cached, err = g.ImageManager.PullWithOptions(c.Image, string(c.ImagePullPolicy), image.PullOptions{
                        Progress: pullProgressFunc(g, pod, c.Image, c.Name),
                        Event:    podEventFn(g, pod),
                })
                if err != nil {
                        return fmt.Errorf("pull: %w", err)
                }
                pullCache[c.Image] = pullCacheEntry{layers: layers, cached: cached}
                if cached {
                        g.EventRecorder.Eventf(pod, corev1.EventTypeNormal, "Cached", "Image %s already present", c.Image)
                } else {
                        g.EventRecorder.Eventf(pod, corev1.EventTypeNormal, "Pulled", "Pulled image %s", c.Image)
                }
        }

        // 2. Mount Overlay
        rootfs, err := g.ImageManager.Mount(uid+"-"+c.Name, layers)
        if err != nil {
                return fmt.Errorf("mount: %w", err)
        }
        g.EventRecorder.Eventf(pod, corev1.EventTypeNormal, "Mounted", "Mounted overlay for container %s", c.Name)

        // 3. Resolve Environment and Volumes
        resolvedEnv := g.Tidal.ResolveEnv(pod, c, podIP)
        volResolver := volume.NewResolver(g.Config.BaseDir, g.Config.Name, uid, g.hostNodeName, g.cmLister, g.secretLister, g.kubeClient)
        bindMounts, err := volResolver.Resolve(ctx, pod, c)
        if err != nil {
                _ = g.ImageManager.Unmount(uid + "-" + c.Name)
                return fmt.Errorf("volume resolution: %w", err)
        }

        if g.clusterDNS != "" {
                _ = writeResolvConf(rootfs, g.clusterDNS, pod.Namespace)
        }

        profile := runtimeProfiles[c.Name]
        ep, cmd := g.ImageManager.ImageEntrypoint(c.Image)

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
                RunAsUser:                     profile.RunAsUser,
                RunAsGroup:                    profile.RunAsGroup,
                Environment:                   resolvedEnv,
                PodIP:                         podIP,
                MemoryLimitBytes:              profile.MemoryLimitBytes,
                CPULimitMillis:                profile.CPULimitMillis,
                CPURequestMillis:              profile.CPURequestMillis,
                ImageEntrypoint:               ep,
                ImageCmd:                      cmd,
                TerminationGracePeriodSeconds: podTerminationGracePeriod(pod),
        }

        // 4. Start the Machine
        if profile.RunAsUser != nil {
                if isPrivileged(c) {
                        g.EventRecorder.Eventf(pod, corev1.EventTypeNormal, "UserIdentity",
                                "Container %s: running as uid %d (privileged, no userns)", c.Name, *profile.RunAsUser)
                } else {
                        g.EventRecorder.Eventf(pod, corev1.EventTypeNormal, "UserIdentity",
                                "Container %s: running as uid %d via userns shim", c.Name, *profile.RunAsUser)
                }
        }
        if err := g.Runtime.RunMachine(ctx, uid, cfg); err != nil {
                // Immediately clean up the mount if systemd rejects the Run request
                _ = g.ImageManager.Unmount(uid + "-" + c.Name)
                _ = g.Runtime.ResetUnit(ctx, uid, c.Name)
                return fmt.Errorf("RunMachine: %w", err)
        }

        // 5. Wait for target state
        if isInit {
                state, err := g.Runtime.WaitForMachineExit(ctx, uid, c.Name, initContainerTimeout)
                _ = g.ImageManager.Unmount(uid + "-" + c.Name) // Init containers ephemeral

                if err != nil {
                        _ = g.Runtime.StopMachine(context.Background(), uid, c.Name)
                        _ = g.Runtime.ResetUnit(context.Background(), uid, c.Name)
                        return fmt.Errorf("init timeout/err: %w", err)
                }
                if state == perigeos.StateFailed {
                        return fmt.Errorf("init container exited with error")
                }
        } else {
                if err := g.waitForContainer(ctx, uid, c.Name, machineStartTimeout); err != nil {
                        return fmt.Errorf("waitForContainer: %w", err)
                }

                // Record that this container was observed running.
                // Without this, if the container exits before the batchwatcher's
                // first poll observes it via ListManagedMachines, seenRunning is
                // never set and the terminal deferral in checkPod loops forever
                // (the pod gets stuck in a non-Pending non-Running limbo).
                g.batchWatcher.MarkRunning(uid, c.Name)

                // Propagate Bidirectional mounts (required for CSI drivers).
                // If this fails, stop the container before returning - leaving it
                // running without shared propagation is worse than a clean retry.
                if err := g.Runtime.MakeSharedMounts(ctx, uid, c.Name, bindMounts); err != nil {
                        _ = g.Runtime.StopMachine(context.Background(), uid, c.Name)
                        _ = g.ImageManager.Unmount(uid + "-" + c.Name)
                        return fmt.Errorf("MakeSharedMounts: %w", err)
                }

                // Run PostStart lifecycle hook
                if c.Lifecycle != nil && c.Lifecycle.PostStart != nil {
                        if err := g.runLifecycleHook(ctx, pod, c, uid, c.Lifecycle.PostStart, "PostStart"); err != nil {
                                g.EventRecorder.Eventf(pod, corev1.EventTypeWarning, "PostStartHookFailed",
                                        "PostStart hook failed for container %s: %v", c.Name, err)
                                return fmt.Errorf("PostStart hook: %w", err)
                        }
                }
        }

        return nil
}

func (g *Gambit) DeletePod(ctx context.Context, pod *corev1.Pod) error {
        uid := string(pod.UID)
        g.Logger.Info("DeletePod", "pawn", g.Config.Name, "namespace", pod.Namespace, "name", pod.Name)

        // TODO rm
        // g.setKind(pod)

        g.store.MarkDeleting(uid)
        g.cancelInFlight(uid) // Stops any currently running CreatePod reconcile loop

        gracePeriod := podTerminationGracePeriod(pod)
        if gracePeriod > 0 {
                var cancel context.CancelFunc
                ctx, cancel = context.WithTimeout(ctx, time.Duration(gracePeriod)*time.Second)
                defer cancel()
        }

        g.runPreStopHooks(ctx, pod, uid)

        // Guarantee teardown always fires even if grace period expires
        teardownCtx, cancelTeardown := context.WithTimeout(context.Background(), 30*time.Second)
        defer cancelTeardown()
        g.teardownPodIdempotent(teardownCtx, uid, pod)

        g.volumes.Untrack(uid)
        g.store.Unregister(uid, pod.Namespace, pod.Name)
        _ = deletePodState(g.Config.BaseDir, g.Config.Name, uid)

        return nil
}

func (g *Gambit) UpdatePod(_ context.Context, pod *corev1.Pod) error {
        return nil
}

func (g *Gambit) restartContainer(ctx context.Context, uid string, pod *corev1.Pod, containerName string, count int32, backoff time.Duration) {
        if g.node.IsShuttingDown() {
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

        // 1. Scrub previous crash state
        _ = g.Runtime.StopMachine(ctx, uid, containerName)
        _ = g.Runtime.ResetUnit(ctx, uid, containerName)
        _ = g.ImageManager.Unmount(uid + "-" + containerName)

        if err := g.Runtime.CheckMachined(ctx); err != nil {
                g.Logger.Error("Restart: machined unhealthy, skipping", "container", containerName, "err", err)
                return
        }

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

        podIP := g.store.PodIP(uid)
        netPath := "/proc/1/ns/net"
        if !pod.Spec.HostNetwork {
                netPath = filepath.Join("/var/run/netns", "peri-"+uid)
        }

        // Dummy cache for individual container restart
        dummyCache := make(map[string]pullCacheEntry)

        profiles := buildContainerRuntimeProfiles(pod)
        err := g.launchContainer(ctx, pod, container, uid, netPath, podIP, dummyCache, profiles, false)
        if err != nil {
                g.Logger.Error("Restart: launch failed", "container", containerName, "err", err)
                return
        }

        g.store.MarkRestarted(uid, containerName)
        g.Logger.Info("Container restarted successfully", "pod", pod.Name, "container", containerName)

        // Publish updated status
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

// Helpers ---------------------------------------------------------------------

func extractResourceLimits(pod *corev1.Pod, c *corev1.Container) (memBytes uint64, cpuLimitMillis int64, cpuRequestMillis int64) {
        if c.Resources.Limits != nil {
                if mem, ok := c.Resources.Limits[corev1.ResourceMemory]; ok {
                        memBytes = uint64(mem.Value())
                }
                if cpu, ok := c.Resources.Limits[corev1.ResourceCPU]; ok {
                        cpuLimitMillis = cpu.MilliValue()
                }
        }
        if c.Resources.Requests != nil {
                if cpu, ok := c.Resources.Requests[corev1.ResourceCPU]; ok {
                        cpuRequestMillis = cpu.MilliValue()
                }
        }

        // Fallback to pod-level resources when container-level values are not set.
        // Pod-level resources are a shared budget, while container-level values are
        // the primary source when present.
        if pod != nil && pod.Spec.Resources != nil {
                if memBytes == 0 {
                        if mem, ok := pod.Spec.Resources.Limits[corev1.ResourceMemory]; ok {
                                memBytes = uint64(mem.Value())
                        }
                }
                if cpuLimitMillis == 0 {
                        if cpu, ok := pod.Spec.Resources.Limits[corev1.ResourceCPU]; ok {
                                cpuLimitMillis = cpu.MilliValue()
                        }
                }
                if cpuRequestMillis == 0 {
                        if cpu, ok := pod.Spec.Resources.Requests[corev1.ResourceCPU]; ok {
                                cpuRequestMillis = cpu.MilliValue()
                        }
                }
        }
        return
}

func buildContainerRuntimeProfiles(pod *corev1.Pod) map[string]containerRuntimeProfile {
        profiles := make(map[string]containerRuntimeProfile, len(pod.Spec.InitContainers)+len(pod.Spec.Containers))
        for i := range pod.Spec.InitContainers {
                c := &pod.Spec.InitContainers[i]
                memLimit, cpuLimit, cpuRequest := extractResourceLimits(pod, c)
                runAsUser, runAsGroup := effectiveRunAs(pod, c)
                profiles[c.Name] = containerRuntimeProfile{
                        MemoryLimitBytes: memLimit,
                        CPULimitMillis:   cpuLimit,
                        CPURequestMillis: cpuRequest,
                        RunAsUser:        runAsUser,
                        RunAsGroup:       runAsGroup,
                }
        }
        for i := range pod.Spec.Containers {
                c := &pod.Spec.Containers[i]
                memLimit, cpuLimit, cpuRequest := extractResourceLimits(pod, c)
                runAsUser, runAsGroup := effectiveRunAs(pod, c)
                profiles[c.Name] = containerRuntimeProfile{
                        MemoryLimitBytes: memLimit,
                        CPULimitMillis:   cpuLimit,
                        CPURequestMillis: cpuRequest,
                        RunAsUser:        runAsUser,
                        RunAsGroup:       runAsGroup,
                }
        }
        return profiles
}

func effectiveRunAs(pod *corev1.Pod, c *corev1.Container) (runAsUser, runAsGroup *int64) {
        if pod != nil && pod.Spec.SecurityContext != nil {
                runAsUser = pod.Spec.SecurityContext.RunAsUser
                runAsGroup = pod.Spec.SecurityContext.RunAsGroup
        }
        if c != nil && c.SecurityContext != nil {
                if c.SecurityContext.RunAsUser != nil {
                        runAsUser = c.SecurityContext.RunAsUser
                }
                if c.SecurityContext.RunAsGroup != nil {
                        runAsGroup = c.SecurityContext.RunAsGroup
                }
        }
        return runAsUser, runAsGroup
}

func isPrivileged(c *corev1.Container) bool {
        return c.SecurityContext != nil &&
                c.SecurityContext.Privileged != nil &&
                *c.SecurityContext.Privileged
}

func (g *Gambit) waitForContainer(ctx context.Context, uid, containerName string, timeout time.Duration) error {
        deadline := time.Now().Add(timeout)
        for time.Now().Before(deadline) {
                state, err := g.Runtime.MachineStatus(ctx, uid, containerName)
                if err == nil {
                        switch state {
                        case perigeos.StateRunning, perigeos.StateExited:
                                return nil
                        case perigeos.StateFailed:
                                // Startup failure (e.g. nspawn refused stale unix-export mount).
                                // Don't call MakeSharedMounts on a dead container.
                                return fmt.Errorf("container %s/%s failed on startup", uid, containerName)
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

func (g *Gambit) markPodFailed(uid string, pod *corev1.Pod, err error) {
        g.EventRecorder.Eventf(pod, corev1.EventTypeWarning, "CreateFailed", "pod creation failed: %v", err)
        failedPod := g.store.MarkFailed(uid, pod, "CreateFailed", err.Error())
        g.notifyPodStatus(failedPod)
}

func (g *Gambit) admitPod(pod *corev1.Pod) string {
        return g.store.AdmitPod(pod, g.Config.CPU, g.Config.Memory)
}

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
                step := pct / 10 * 10
                if step > lastPct || pct == 100 {
                        g.EventRecorder.Eventf(pod, corev1.EventTypeNormal, "Pulling",
                                "Pulling image %s: %d%% (%d/%d layers) for %s", imageName, pct, done, total, containerName)
                        lastPct = step
                }
        }
}

func podTerminationGracePeriod(pod *corev1.Pod) int64 {
        if pod.Spec.TerminationGracePeriodSeconds != nil {
                return *pod.Spec.TerminationGracePeriodSeconds
        }
        return 30
}

func (g *Gambit) runLifecycleHook(ctx context.Context, pod *corev1.Pod, c *corev1.Container, uid string, handler *corev1.LifecycleHandler, hookName string) error {
        switch {
        case handler.Exec != nil:
                return g.Runtime.RunInContainer(ctx, uid, c.Name, handler.Exec.Command, &noopAttachIO{})
        case handler.HTTPGet != nil:
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
                time.Sleep(time.Duration(handler.Sleep.Seconds) * time.Second)
                return nil
        default:
                return nil
        }
}

func (g *Gambit) runPreStopHooks(ctx context.Context, pod *corev1.Pod, uid string) {
        for i := range pod.Spec.Containers {
                c := &pod.Spec.Containers[i]
                if c.Lifecycle == nil || c.Lifecycle.PreStop == nil {
                        continue
                }
                if err := g.runLifecycleHook(ctx, pod, c, uid, c.Lifecycle.PreStop, "PreStop"); err != nil {
                        g.Logger.Warn("PreStop hook failed", "container", c.Name, "err", err)
                }
        }
}

func (g *Gambit) pushContainerCreatingStatus(pod *corev1.Pod, podIP string) {
        updated := pod.DeepCopy()
        updated.Status.Phase = corev1.PodPending
        updated.Status.HostIP = resolveNodeIP(g.Config)
        updated.Status.PodIP = podIP
        now := metav1.NewTime(time.Now())
        updated.Status.StartTime = &now
        for _, c := range pod.Spec.Containers {
                updated.Status.ContainerStatuses = append(updated.Status.ContainerStatuses, corev1.ContainerStatus{
                        Name:  c.Name,
                        Image: c.Image,
                        Ready: false,
                        State: corev1.ContainerState{
                                Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"},
                        },
                })
        }
        updated.Status.Conditions = []corev1.PodCondition{{
                Type:   corev1.PodReady,
                Status: corev1.ConditionFalse,
        }}
        g.notifyPodStatus(updated)
}
