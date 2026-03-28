package node

import (
	"context"
	"log/slog"
	"sync"
	"time"

	pruntime "github.com/malformed-c/periapsis/internal/runtime"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// BatchWatcher replaces per-pod watcher goroutines with a single goroutine
// per pawn that calls ListManagedMachines once per cycle (1 D-Bus call),
// diffs against expected state, and handles restart policy + probes.
//
// It also acts as the status coalescer: each cycle it compares the current
// container state map against the previous one and pushes status updates
// (via notifyPodStatus) only for pods whose observable state actually changed.
// This eliminates the need for the 5s poll-all-pods loop — status updates
// are O(changed_pods) instead of O(all_pods).
//
// The watcher is hybrid event+poll: it subscribes to D-Bus unit state signals
// via Runtime.SubscribeEvents so container exits are detected immediately,
// with the ticker as a consistency fallback.
type BatchWatcher struct {
	gambit *Gambit
	logger *slog.Logger
	cancel context.CancelFunc
	done   chan struct{}

	// pokeCh receives non-blocking sends when a pod lifecycle event
	// (creation, deletion) occurs. This triggers an immediate poll so
	// fast-exit containers are detected without waiting for the ticker.
	pokeCh chan struct{}

	// prevStateMap holds the stateMap from the previous poll cycle.
	// Used by the coalescer to detect container state transitions.
	prevStateMap map[string]pruntime.MachineState

	// prevReady tracks the last-known Ready state per container.
	// Readiness changes (probe pass/fail) don't change the machine state
	// but do affect the pod's Ready condition, so they need separate tracking.
	prevReady map[string]bool // key: uid/containerName

	// stateCache holds the latest stateMap for external consumers
	// (e.g. GetPodStatus) to read without per-container D-Bus calls.
	stateCacheMu sync.RWMutex
	stateCache   map[string]pruntime.MachineState

	// seenRunning tracks containers that have been observed in Running
	// state at least once. Used to prevent premature terminal phase
	// decisions: systemd units briefly pass through inactive/dead during
	// startup, and ExecMainStatus isn't updated until after the unit
	// settles. We only make terminal decisions for containers we've
	// confirmed were actually running.
	seenRunning map[string]bool // key: uid/containerName

	// restarting tracks containers currently in a restart goroutine
	// to prevent double-restarts between poll cycles.
	restartingMu sync.Mutex
	restarting   map[string]bool // key: uid/containerName
}

// StartBatchWatcher creates and starts the batch watcher for a Gambit pawn.
func StartBatchWatcher(g *Gambit) *BatchWatcher {
	ctx, cancel := context.WithCancel(context.Background())
	bw := &BatchWatcher{
		gambit:       g,
		logger:       g.Logger.With("component", "batchwatcher"),
		cancel:       cancel,
		done:         make(chan struct{}),
		pokeCh:       make(chan struct{}, 1),
		prevStateMap: make(map[string]pruntime.MachineState),
		prevReady:    make(map[string]bool),
		stateCache:   make(map[string]pruntime.MachineState),
		seenRunning:  make(map[string]bool),
		restarting:   make(map[string]bool),
	}
	go bw.run(ctx)
	return bw
}

// Poke triggers an immediate poll cycle. Non-blocking — if a poke is already
// pending, the additional signal is coalesced.
func (bw *BatchWatcher) Poke() {
	select {
	case bw.pokeCh <- struct{}{}:
	default:
	}
}

// MarkRunning records that a container has been observed in Running state.
// Called by CreatePod after the machine is started so that the BatchWatcher
// knows the container was running even if the D-Bus "running" event arrives
// after the unit exits (fast-exit containers).
func (bw *BatchWatcher) MarkRunning(uid, containerName string) {
	bw.seenRunning[uid+"/"+containerName] = true
}

// ContainerState returns the cached state for a container from the most recent
// poll cycle. Returns StateUnknown if no cache entry exists yet.
func (bw *BatchWatcher) ContainerState(uid, containerName string) pruntime.MachineState {
	key := uid + "/" + containerName
	bw.stateCacheMu.RLock()
	state, ok := bw.stateCache[key]
	bw.stateCacheMu.RUnlock()
	if !ok {
		return pruntime.StateUnknown
	}
	return state
}

// Stop cancels the batch watcher and waits for it to exit.
func (bw *BatchWatcher) Stop() {
	bw.cancel()
	<-bw.done
}

func (bw *BatchWatcher) run(ctx context.Context) {
	defer close(bw.done)

	// On startup, clean up stale units left by a previous crash/restart.
	bw.cleanupStaleUnits(ctx)

	ticker := time.NewTicker(containerWatchPoll)
	defer ticker.Stop()

	// Subscribe to D-Bus unit state events for reactive detection.
	eventCh := bw.gambit.Runtime.SubscribeEvents(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			bw.poll(ctx)
		case ev := <-eventCh:
			bw.handleUnitEvent(ctx, ev)
		case <-bw.pokeCh:
			bw.poll(ctx)
		}
	}
}

// handleUnitEvent reacts to a D-Bus unit state change by querying the
// individual container's MachineStatus and updating the stateCache.
// This is more targeted than a full poll — it only touches the affected
// container, giving sub-second detection for fast-exit containers.
func (bw *BatchWatcher) handleUnitEvent(ctx context.Context, ev pruntime.UnitEvent) {
	// Parse uid and containerName from the unit name.
	// Format: perigeos-<pawn>-pod-<uid>-<containerName>.service
	uid, containerName := bw.gambit.parseUnitName(ev.UnitName)
	if uid == "" {
		return
	}

	// Map substate to MachineState.
	//
	// We intentionally ignore "dead" — it's an intermediate substate that
	// fires BEFORE systemd updates ExecMainStatus. If we react to it, we
	// see exit code 0 and incorrectly mark the pod as Succeeded. Systemd
	// always follows "dead" with either:
	//   - "failed" (non-zero exit) → we react to this immediately
	//   - unit collection (exit 0, CollectMode=inactive) → the ticker
	//     poll detects the unit is gone within 2s
	var state pruntime.MachineState
	switch ev.SubState {
	case "running":
		state = pruntime.StateRunning
	case "failed":
		state = pruntime.StateFailed
	case "start-pre", "start", "start-post":
		state = pruntime.StateCreating
	default:
		return // ignore "dead" and other transient states
	}

	key := uid + "/" + containerName

	// Track that we've seen this container running (used by checkPod
	// to avoid premature terminal decisions during unit startup).
	if state == pruntime.StateRunning {
		bw.seenRunning[key] = true
	}

	// Update stateCache atomically.
	bw.stateCacheMu.Lock()
	prev := bw.stateCache[key]
	bw.stateCache[key] = state
	bw.stateCacheMu.Unlock()

	if prev == state {
		return // no change
	}

	// For failed containers, trigger a full poll to process restart policy
	// and push terminal phase.
	if state == pruntime.StateFailed {
		bw.poll(ctx)
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

	// Publish to cache so GetPodStatus can read without per-container D-Bus calls.
	bw.stateCacheMu.Lock()
	bw.stateCache = stateMap
	bw.stateCacheMu.Unlock()

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

	// Track which pods have state changes for the coalescer.
	changedPods := make(map[string]bool)

	for _, e := range entries {
		// Skip pods still being created (Pending) — no machine yet.
		if e.phase == corev1.PodPending {
			continue
		}
		// Skip pods in terminal phase — unless a container's state changed
		// to a *different known state*, meaning systemd settled to a different
		// result (e.g. the pod was marked Succeeded from an intermediate
		// "dead" substate, but the unit actually failed).
		// A unit disappearing from stateMap (after ResetUnit cleanup) is NOT
		// a state change — it's expected cleanup.
		if e.phase == corev1.PodSucceeded || e.phase == corev1.PodFailed {
			needsReeval := false
			for _, c := range e.pod.Spec.Containers {
				key := e.uid + "/" + c.Name
				cur, curExists := stateMap[key]
				prev, prevExists := bw.prevStateMap[key]
				if prevExists && curExists && prev != cur {
					needsReeval = true
					break
				}
			}
			if !needsReeval {
				continue
			}
		}
		if len(e.pod.Spec.Containers) == 0 {
			continue
		}

		// Detect container state changes for the coalescer.
		for _, c := range e.pod.Spec.Containers {
			key := e.uid + "/" + c.Name
			cur := stateMap[key]
			if prev, ok := bw.prevStateMap[key]; !ok || prev != cur {
				changedPods[e.uid] = true
			}
			// Check readiness changes (probe transitions don't change machine state).
			if cur == pruntime.StateRunning {
				ready := bw.gambit.isContainerReady(e.uid, c.Name)
				if prev, ok := bw.prevReady[key]; !ok || prev != ready {
					changedPods[e.uid] = true
					bw.prevReady[key] = ready
				}
			}
		}

		bw.checkPod(ctx, e.uid, e.pod, e.podIP, stateMap)
	}

	// Coalescer: push status updates only for pods with actual changes.
	// The downstream enqueuePodStatusUpdate has cmp.Equal dedup as a safety net.
	stateLookup := func(uid, containerName string) pruntime.MachineState {
		if s, ok := stateMap[uid+"/"+containerName]; ok {
			return s
		}
		return pruntime.StateUnknown
	}
	for _, e := range entries {
		if !changedPods[e.uid] {
			continue
		}
		// Re-read podPhases under lock — checkPod may have set a terminal
		// phase during *this* poll cycle, after the entries snapshot was taken.
		bw.gambit.mu.RLock()
		currentPhase := bw.gambit.podPhases[e.uid]
		bw.gambit.mu.RUnlock()
		if currentPhase == corev1.PodPending || currentPhase == corev1.PodSucceeded || currentPhase == corev1.PodFailed {
			continue
		}
		status := bw.gambit.buildPodStatus(e.pod, stateLookup)
		updated := e.pod.DeepCopy()
		status.DeepCopyInto(&updated.Status)
		bw.gambit.notifyPodStatus(updated)
	}

	// Rotate state maps for next cycle.
	bw.prevStateMap = stateMap

	// Clean up prevReady for pods no longer tracked.
	activeKeys := make(map[string]bool, len(entries)*2)
	for _, e := range entries {
		for _, c := range e.pod.Spec.Containers {
			activeKeys[e.uid+"/"+c.Name] = true
		}
	}
	for k := range bw.prevReady {
		if !activeKeys[k] {
			delete(bw.prevReady, k)
		}
	}
	for k := range bw.seenRunning {
		if !activeKeys[k] {
			delete(bw.seenRunning, k)
		}
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
		bw.logger.Debug("checkPod container state", "pod", pod.Name, "container", c.Name, "state", state, "exists", exists, "policy", policy)

		// If the container appears exited but was never observed running,
		// it may be in a brief inactive/dead state during unit startup
		// (systemd transitions through inactive→active for transient units,
		// and ExecMainStatus isn't updated until after the unit settles).
		// Don't make terminal decisions — wait for the unit to settle.
		if !bw.seenRunning[key] && (state == pruntime.StateExited || state == pruntime.StateUnknown) {
			bw.logger.Debug("Deferring terminal decision — container never seen running", "pod", pod.Name, "container", c.Name, "state", state)
			allExited = false
			allSucceeded = false
			continue
		}

		switch state {
		case pruntime.StateRunning, pruntime.StateCreating:
			allExited = false
			allSucceeded = false

			if state == pruntime.StateRunning {
				bw.seenRunning[key] = true
			}

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
	var terminalPhase corev1.PodPhase
	if allSucceeded {
		terminalPhase = corev1.PodSucceeded
	} else {
		terminalPhase = corev1.PodFailed
	}
	bw.logger.Info("Setting terminal phase", "pod", pod.Name, "phase", terminalPhase, "allSucceeded", allSucceeded)

	// Build terminal status.
	updated := pod.DeepCopy()
	updated.Status.Phase = terminalPhase
	now := metav1.NewTime(time.Now())
	for i, cs := range updated.Status.ContainerStatuses {
		if cs.State.Running != nil {
			updated.Status.ContainerStatuses[i].State.Terminated = &corev1.ContainerStateTerminated{
				Reason:     "Completed",
				FinishedAt: now,
				StartedAt:  cs.State.Running.StartedAt,
			}
			updated.Status.ContainerStatuses[i].State.Running = nil
		}
	}

	// Update both the phase map AND the pod's Status in-place so that
	// GetPodStatus returns the terminal status even after the systemd
	// unit is cleaned up.
	bw.gambit.mu.Lock()
	bw.gambit.podPhases[uid] = terminalPhase
	if p, ok := bw.gambit.pods[uid]; ok {
		updated.Status.DeepCopyInto(&p.Status)
	}
	bw.gambit.mu.Unlock()

	bw.gambit.notifyPodStatus(updated)

	// Clean up dead/failed systemd units now that we've read their state.
	// Without this, transient units accumulate in systemd's listing.
	for _, c := range pod.Spec.Containers {
		if err := bw.gambit.Runtime.ResetUnit(ctx, uid, c.Name); err != nil {
			bw.logger.Debug("ResetUnit failed (unit may already be collected)", "pod", pod.Name, "container", c.Name, "err", err)
		}
	}
}

// cleanupStaleUnits removes dead/failed systemd units from a previous
// perigeos lifetime that never got cleaned up (e.g. after a crash).
func (bw *BatchWatcher) cleanupStaleUnits(ctx context.Context) {
	bw.gambit.mu.RLock()
	activeUIDs := make(map[string]bool, len(bw.gambit.pods))
	for uid := range bw.gambit.pods {
		activeUIDs[uid] = true
	}
	bw.gambit.mu.RUnlock()

	cleaned, err := bw.gambit.Runtime.CleanupStaleUnits(ctx, activeUIDs)
	if err != nil {
		bw.logger.Error("Startup stale unit cleanup failed", "err", err)
		return
	}
	if cleaned > 0 {
		bw.logger.Info("Cleaned up stale units from previous run", "count", cleaned)
	}
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
// Skips the restart if the pod has been removed from gambit (DeletePod in progress).
func (bw *BatchWatcher) maybeRestart(ctx context.Context, uid string, pod *corev1.Pod, containerName string) {
	key := uid + "/" + containerName

	// Don't restart containers for pods that are being deleted.
	bw.gambit.mu.RLock()
	isDeleting := bw.gambit.deleting[uid]
	bw.gambit.mu.RUnlock()
	if isDeleting {
		return
	}

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
