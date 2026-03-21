package provider

import (
	"context"
	"log/slog"
	"sync"
	"time"

	pruntime "github.com/malformed-c/periapsis/internal/runtime"
	corev1 "k8s.io/api/core/v1"
)

// BatchWatcher replaces per-pod watcher goroutines with a single goroutine
// per pawn that calls ListManagedMachines once per cycle (1 D-Bus call),
// diffs against expected state, and handles restart policy + probes.
type BatchWatcher struct {
	gambit *Gambit
	logger *slog.Logger
	cancel context.CancelFunc
	done   chan struct{}

	// restarting tracks containers currently in a restart goroutine
	// to prevent double-restarts between poll cycles.
	restartingMu sync.Mutex
	restarting   map[string]bool // key: uid/containerName
}

// StartBatchWatcher creates and starts the batch watcher for a Gambit pawn.
func StartBatchWatcher(g *Gambit) *BatchWatcher {
	ctx, cancel := context.WithCancel(context.Background())
	bw := &BatchWatcher{
		gambit:     g,
		logger:     g.Logger.With("component", "batchwatcher"),
		cancel:     cancel,
		done:       make(chan struct{}),
		restarting: make(map[string]bool),
	}
	go bw.run(ctx)
	return bw
}

// Stop cancels the batch watcher and waits for it to exit.
func (bw *BatchWatcher) Stop() {
	bw.cancel()
	<-bw.done
}

func (bw *BatchWatcher) run(ctx context.Context) {
	defer close(bw.done)
	ticker := time.NewTicker(containerWatchPoll)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			bw.poll(ctx)
		}
	}
}

func (bw *BatchWatcher) poll(ctx context.Context) {
	machines, err := bw.gambit.Runtime.ListManagedMachines(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		bw.logger.Error("Batch poll: ListManagedMachines failed", "err", err)
		return
	}

	// Index by uid/containerName for O(1) lookup.
	stateMap := make(map[string]pruntime.MachineState, len(machines))
	for _, m := range machines {
		stateMap[m.UID+"/"+m.ContainerName] = m.State
	}

	// Snapshot pods under read lock.
	bw.gambit.mu.RLock()
	type podEntry struct {
		uid    string
		pod    *corev1.Pod
		phase  corev1.PodPhase
		podIP  string
	}
	entries := make([]podEntry, 0, len(bw.gambit.pods))
	for uid, pod := range bw.gambit.pods {
		entries = append(entries, podEntry{uid: uid, pod: pod, phase: bw.gambit.podPhases[uid], podIP: bw.gambit.podIPs[uid]})
	}
	bw.gambit.mu.RUnlock()

	for _, e := range entries {
		// Skip pods still being created (Pending) — no machine yet.
		if e.phase == corev1.PodPending {
			continue
		}
		// Skip pods in terminal phase.
		if e.phase == corev1.PodSucceeded || e.phase == corev1.PodFailed {
			continue
		}
		if len(e.pod.Spec.Containers) == 0 {
			continue
		}

		bw.checkPod(ctx, e.uid, e.pod, e.podIP, stateMap)
	}
}

func (bw *BatchWatcher) checkPod(ctx context.Context, uid string, pod *corev1.Pod, podIP string, stateMap map[string]pruntime.MachineState) {
	policy := pod.Spec.RestartPolicy
	if policy == "" {
		policy = corev1.RestartPolicyAlways
	}

	allExited := true
	allSucceeded := true

	for _, c := range pod.Spec.Containers {
		key := uid + "/" + c.Name
		state, exists := stateMap[key]
		if !exists {
			state = pruntime.StateExited
		}

		switch state {
		case pruntime.StateRunning, pruntime.StateCreating:
			allExited = false
			allSucceeded = false

			// Reset backoff if container has been running long enough.
			if state == pruntime.StateRunning {
				bw.gambit.mu.Lock()
				if rs, ok := bw.gambit.restarts[uid][c.Name]; ok {
					if time.Since(rs.lastStarted) > restartBackoffReset {
						rs.backoff = restartBackoffInit
					}
				}
				bw.gambit.mu.Unlock()

				// Run probes for running containers.
				bw.runProbes(ctx, uid, pod, &c, podIP)
			}

		case pruntime.StateFailed:
			allSucceeded = false
			if policy == corev1.RestartPolicyAlways || policy == corev1.RestartPolicyOnFailure {
				bw.maybeRestart(ctx, uid, pod, c.Name)
				allExited = false
			}

		case pruntime.StateExited:
			if policy == corev1.RestartPolicyAlways {
				bw.maybeRestart(ctx, uid, pod, c.Name)
				allExited = false
			}
			// OnFailure + exit 0 → don't restart.
			// Never → don't restart.
		}
	}

	if !allExited {
		return
	}

	// All containers exited and none will be restarted — set terminal phase.
	bw.gambit.mu.Lock()
	if allSucceeded {
		bw.gambit.podPhases[uid] = corev1.PodSucceeded
	} else {
		bw.gambit.podPhases[uid] = corev1.PodFailed
	}
	bw.gambit.mu.Unlock()
}

// runProbes executes startup, liveness, and readiness probes for a running
// container, respecting each probe's PeriodSeconds cadence.
func (bw *BatchWatcher) runProbes(ctx context.Context, uid string, pod *corev1.Pod, c *corev1.Container, podIP string) {
	bw.gambit.mu.RLock()
	ps := bw.gambit.probeStates[uid][c.Name]
	bw.gambit.mu.RUnlock()
	if ps == nil {
		return
	}

	// 1. Startup probe gates liveness/readiness.
	if c.StartupProbe != nil && !ps.StartupPassed {
		if isDue(ps, "startup", c.StartupProbe.PeriodSeconds) {
			result := bw.gambit.probeRunner.RunProbe(ctx, pod, c.Name, c.StartupProbe, podIP)
			markProbed(ps, "startup")
			if restart := EvalStartup(ps, c.StartupProbe, result); restart {
				bw.logger.Warn("Startup probe failed past threshold, restarting",
					"pod", pod.Name, "container", c.Name)
				bw.maybeRestart(ctx, uid, pod, c.Name)
			}
		}
		return // don't run liveness/readiness until startup passes
	}

	// 2. Liveness probe — failure past threshold triggers restart.
	if c.LivenessProbe != nil && isDue(ps, "liveness", c.LivenessProbe.PeriodSeconds) {
		result := bw.gambit.probeRunner.RunProbe(ctx, pod, c.Name, c.LivenessProbe, podIP)
		markProbed(ps, "liveness")
		if restart := EvalLiveness(ps, c.LivenessProbe, result); restart {
			bw.logger.Warn("Liveness probe failed past threshold, restarting",
				"pod", pod.Name, "container", c.Name)
			bw.gambit.EventRecorder.Eventf(pod, corev1.EventTypeWarning, "Unhealthy",
				"Liveness probe failed for container %s", c.Name)
			// Reset probe state so the restarted container starts fresh.
			bw.gambit.mu.Lock()
			bw.gambit.probeStates[uid][c.Name] = &ContainerProbeState{
				Ready:         c.ReadinessProbe == nil,
				LastProbeTime: make(map[string]time.Time),
			}
			bw.gambit.mu.Unlock()
			bw.maybeRestart(ctx, uid, pod, c.Name)
			return
		}
	}

	// 3. Readiness probe — controls Ready condition on the container.
	if c.ReadinessProbe != nil && isDue(ps, "readiness", c.ReadinessProbe.PeriodSeconds) {
		result := bw.gambit.probeRunner.RunProbe(ctx, pod, c.Name, c.ReadinessProbe, podIP)
		markProbed(ps, "readiness")
		EvalReadiness(ps, c.ReadinessProbe, result)
	}
}

// maybeRestart launches a restart goroutine for a container if one isn't
// already running. Prevents double-restarts between poll cycles.
func (bw *BatchWatcher) maybeRestart(ctx context.Context, uid string, pod *corev1.Pod, containerName string) {
	key := uid + "/" + containerName

	bw.restartingMu.Lock()
	if bw.restarting[key] {
		bw.restartingMu.Unlock()
		return
	}
	bw.restarting[key] = true
	bw.restartingMu.Unlock()

	go func() {
		defer func() {
			bw.restartingMu.Lock()
			delete(bw.restarting, key)
			bw.restartingMu.Unlock()
		}()
		bw.gambit.restartContainer(ctx, uid, pod, containerName)
	}()
}
