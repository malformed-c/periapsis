// Copyright (C) 2025-2026 Malformed C. All rights reserved.
// SPDX-License-Identifier: BUSL-1.1

package node

// BatchWatcher — fully event-based D-Bus signal adapter.
//
// Phase 4 of ADR-0011: the BatchWatcher is now a thin adapter whose only
// responsibility is to subscribe to systemd D-Bus PropertiesChanged signals,
// parse them into types.UnitFact values, and forward them to Syzygy.
//
// All state management (coalescing, restart policy, probes, terminal phase
// decisions) has moved to the pure foci.Reduce function, driven by Syzygy's
// single-threaded event loop. Poll-based reconciliation is handled by
// Syzygy's anti-entropy loop, which calls ListManagedMachines and emits
// ContainerStateFacts. Probes are owned by ProbeScheduler.
//
// What remains:
//   - Startup: cleanup stale units from a previous crash
//   - Runtime: D-Bus event subscription → UnitFact → SendFact
//   - Cleanup: stale unit removal on startup

import (
	"context"
	"log/slog"
	"strings"

	perigeos "github.com/malformed-c/periapsis/internal/runtime"
	"github.com/malformed-c/periapsis/internal/types"
)

// BatchWatcherDeps holds all external dependencies for the BatchWatcher.
type BatchWatcherDeps struct {
	Runtime  perigeos.Runtime
	Logger   *slog.Logger
	PawnName string
	Store    *PodStore // only used for startup stale-unit cleanup

	// ParseUnitName extracts (uid, containerName) from a systemd unit name.
	ParseUnitName func(unitName string) (uid, containerName string)

	// SendFact forwards a Fact to the Syzygy event loop.
	// Required — the event-based BW cannot function without Syzygy.
	SendFact func(fact types.Fact) bool
}

// BatchWatcher subscribes to D-Bus unit state signals and forwards them
// as UnitFacts to the Syzygy event loop. It holds no pod state, runs no
// probes, performs no restart logic, and never pushes status directly.
type BatchWatcher struct {
	deps   BatchWatcherDeps
	logger *slog.Logger
	cancel context.CancelFunc
	done   chan struct{}
}

// StartBatchWatcher creates and starts the event-based batch watcher.
func StartBatchWatcher(deps BatchWatcherDeps) *BatchWatcher {
	if deps.SendFact == nil {
		deps.Logger.Warn("BatchWatcher started without SendFact — events will be dropped")
		deps.SendFact = func(types.Fact) bool { return false }
	}

	ctx, cancel := context.WithCancel(context.Background())
	bw := &BatchWatcher{
		deps:   deps,
		logger: deps.Logger.With("component", "batchwatcher"),
		cancel: cancel,
		done:   make(chan struct{}),
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

	// On startup, clean up stale units left by a previous crash/restart.
	bw.cleanupStaleUnits(ctx)

	// Subscribe to D-Bus unit state events.
	eventCh := bw.deps.Runtime.SubscribeEvents(ctx)

	for {
		select {
		case <-ctx.Done():
			return

		case ev, ok := <-eventCh:
			if !ok {
				return
			}

			bw.handleUnitEvent(ev)
		}
	}
}

// handleUnitEvent reacts to a D-Bus unit state change by emitting a
// UnitFact to Syzygy. Syzygy's Reduce function handles all downstream
// logic: phase transitions, restart decisions, event recording, and
// status updates via the Effect pipeline.
func (bw *BatchWatcher) handleUnitEvent(ev perigeos.UnitEvent) {
	// Quick filter: does this unit belong to this pawn?
	if !strings.HasPrefix(ev.UnitName, "perigeos-"+bw.deps.PawnName+"-") {
		return
	}

	uid, containerName := bw.deps.ParseUnitName(ev.UnitName)
	if uid == "" {
		return
	}

<<<<<<< HEAD
	// Fetch exit code for "failed" substate so the reducer can decide
	// whether to restart or terminate.
	var exitCode int32
	if ev.SubState == "failed" {
		exitCode = bw.deps.Runtime.MachineExitCode(context.Background(), uid, containerName)
	}

	bw.logger.Debug("handleUnitEvent: emitting UnitFact",
		"uid", uid, "container", containerName,
		"unit", ev.UnitName, "subState", ev.SubState, "exitCode", exitCode)

	bw.deps.SendFact(types.NewUnitFact(uid, ev.UnitName, ev.SubState, exitCode))
=======
	// Fetch pod once - still needed for the isInit check (init container Running
	// must not trigger a poll, since app containers haven't been launched yet).
	pod := bw.deps.Store.GetPodCopy(uid)

	// Determine if this is an init container. Init containers run sequentially
	// before app containers; when an init container transitions to Running,
	// app containers haven't been started yet. Triggering a poll at that
	// point causes checkPod to see app containers as missing from
	// ListManagedMachines (exists=false), hitting the "never seen running"
	// deferral and pushing ContainerCreating before the lifecycle code
	// has finished launching them.
	isInit := pod != nil && isInitContainer(pod, containerName)

	// Clear container state for any non-running/non-starting substate.
	if ev.SubState != "running" && ev.SubState != "start" &&
		ev.SubState != "start-pre" && ev.SubState != "start-post" {

		// TODO: Fill Reason and Message
		bw.deps.Store.SetContainerState(uid, containerName,
			corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{}}, false)
	}

	// Map substate to MachineState.
	//
	// We intentionally ignore "dead" - it's an intermediate substate that
	// fires BEFORE systemd updates ExecMainStatus. If we react to it, we
	// see exit code 0 and incorrectly mark the pod as Succeeded. Systemd
	// always follows "dead" with either:
	//   - "failed" (non-zero exit) -> we react to this immediately
	//   - unit collection (exit 0, CollectMode=inactive) -> the ticker
	//     poll detects the unit is gone within 2s
	// Emit informational events for container stop substates.
	// These fire while the container is shutting down and let operators
	// see exactly where time is spent during pod deletion.
	switch ev.SubState {
	case "stop-sigterm", "stop-watchdog":
		// systemd sent SIGTERM; container has terminationGracePeriodSeconds to exit.
		if pod != nil {
			bw.deps.EventRecorder.Eventf(pod, corev1.EventTypeNormal, "Killing",
				"Container %s received SIGTERM, waiting for graceful exit", containerName)
		}

	case "stop-sigkill", "stop-kill":
		// Grace period expired; systemd is sending SIGKILL.
		if pod != nil {
			bw.deps.EventRecorder.Eventf(pod, corev1.EventTypeWarning, "Killing",
				"Container %s grace period expired, sending SIGKILL", containerName)
		}
	}

	// Emit UnitFact to Syzygy for all substates that affect pod state or
	// produce k8s events (Started, Killing). Reduce handles the logic;
	// RecordEvent effects flow back through Horizon.
	//
	// Substates that Reduce handles:
	//   running, failed, start-pre, start, start-post  -> phase transitions + Started event
	//   stop-sigterm, stop-watchdog                    -> Killing/SIGTERM event
	//   stop-sigkill, stop-kill                        -> Killing/SIGKILL event
	//   dead, others                                   -> ignored by Reduce (default case)
	if bw.deps.SendFact != nil {
		bw.deps.SendFact(types.NewUnitFact(uid, ev.UnitName, ev.SubState, 0))
	}

	// Map substate -> MachineState for the local stateCache.
	// "dead" and stop-* substates are intentionally not cached -
	// the ticker poll reconciles them within containerWatchPoll.
	var state perigeos.MachineState
	switch ev.SubState {
	case "running":
		state = perigeos.StateRunning

	case "failed":
		state = perigeos.StateFailed

	case "start-pre", "start", "start-post":
		state = perigeos.StateCreating

	default:
		bw.logger.Debug("handleUnitEvent: ignoring substate",
			"uid", uid, "container", containerName, "subState", ev.SubState)

		return // ignore "dead" and other transient states
	}

	key := uid + "/" + containerName

	bw.logger.Debug("handleUnitEvent: substate mapped",
		"uid", uid, "container", containerName, "subState", ev.SubState, "state", state, "isInit", isInit)

	// Track seenRunning for BW's internal checkPod / makeStateLookup logic.
	// (Syzygy tracks this independently via ContainerState.SeenRunning.)
	if state == perigeos.StateRunning {
		bw.pollMu.Lock()
		bw.seenRunning[key] = true
		bw.pollMu.Unlock()
	}

	// Update stateCache atomically.
	bw.stateCacheMu.Lock()
	prev := bw.stateCache[key]
	bw.stateCache[key] = state
	bw.stateCacheMu.Unlock()

	if prev == state {
		bw.logger.Debug("handleUnitEvent: no state change, skipping poll",
			"uid", uid, "container", containerName, "state", state)

		return // no change, skip poll trigger
	}

	// TODO: Remove
	// Emit container lifecycle events on state transitions.
	// The D-Bus path is the fast reactive path - events fire within
	// milliseconds of the actual systemd state change. The poll path
	// (below in checkPod) covers failures and exits that the D-Bus
	// path may miss (e.g. "dead" substate is intentionally ignored).
	if state == perigeos.StateRunning {
		if pod != nil {
			reason := "Started"
			if isInit {
				reason = "InitStarted"
			}

			bw.deps.EventRecorder.Eventf(pod, corev1.EventTypeNormal, reason,
				"Container %s started", containerName)
		}
	}

	// Trigger a full poll on state transitions that affect pod status.
	// For app container Running: pushes the Running phase immediately and
	// starts the readiness probe initialDelay timer, instead of waiting
	// up to containerWatchPoll (2s) for the ticker.
	// For Failed: processes restart policy and pushes terminal phase.
	// For init container Running: skip - app containers haven't been
	// launched yet, so the poll would see them as missing and either
	// defer terminal decisions or push premature ContainerCreating.
	// The lifecycle code will trigger status updates when it finishes.
	// Init container Running: skip - app containers haven't been launched yet.
	if state == perigeos.StateRunning && !isInit {
		bw.logger.Info("handleUnitEvent: triggering poll (app container Running)",
			"uid", uid, "pod", podName(pod), "container", containerName)

		bw.poll(ctx)

	} else if state == perigeos.StateFailed {
		bw.logger.Info("handleUnitEvent: triggering poll (container Failed)",
			"uid", uid, "pod", podName(pod), "container", containerName)

		bw.poll(ctx)

	} else if state == perigeos.StateRunning && isInit {
		bw.logger.Debug("handleUnitEvent: skipping poll (init container Running)",
			"uid", uid, "pod", podName(pod), "container", containerName)
	}
}

// podName returns the pod's name for logging, or "<unknown>" if pod is nil.
func podName(pod *corev1.Pod) string {
	if pod == nil {
		return "<unknown>"
	}

	return pod.Name
}

// isInitContainer reports whether containerName is an init container of pod.
func isInitContainer(pod *corev1.Pod, containerName string) bool {
	for i := range pod.Spec.InitContainers {
		if pod.Spec.InitContainers[i].Name == containerName {
			return true
		}
	}

	return false
}

func (bw *BatchWatcher) poll(ctx context.Context) {
	bw.pollMu.Lock()
	defer bw.pollMu.Unlock()

	machines, err := bw.deps.Runtime.ListManagedMachines(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return
		}

		bw.logger.Error("Batch poll: ListManagedMachines failed", "err", err)
		return
	}

	// Index by uid/containerName for O(1) lookup.
	stateMap := make(map[string]perigeos.MachineState, len(machines))
	exitCodeMap := make(map[string]int32, len(machines))
	for _, m := range machines {
		key := m.UID + "/" + m.ContainerName
		stateMap[key] = m.State
		exitCodeMap[key] = m.ExitCode
	}

	// Publish to cache so GetPodStatus can read without per-container D-Bus calls.
	bw.stateCacheMu.Lock()
	bw.stateCache = stateMap
	bw.stateCacheMu.Unlock()

	// Snapshot pods efficiently using store's atomic snapshot.
	type podEntry struct {
		uid   string
		pod   *corev1.Pod
		phase corev1.PodPhase
		podIP string
	}
	storeEntries := bw.deps.Store.Snapshot()
	entries := make([]podEntry, len(storeEntries))
	for i, e := range storeEntries {
		entries[i] = podEntry{uid: e.UID, pod: e.Pod, phase: e.Phase, podIP: e.PodIP}
	}

	// Run probes concurrently for all running containers before the sequential
	// checkPod loop. At hundreds of pods each probe can take up to its timeout
	// (1s by default). Running them serially would stall the entire poll for
	// minutes, causing pods probed late to miss their 3s window and flip
	// not-ready - producing the rollout oscillation observed at scale.
	//
	// After this fan-out completes, isDue() returns false for every container
	// that was probed here, so the runProbes calls inside checkPod are no-ops.
	{
		sem := make(chan struct{}, maxConcurrentProbes)
		var probeWg sync.WaitGroup
		for i := range entries {
			e := entries[i]
			if e.phase == corev1.PodPending || e.phase == corev1.PodSucceeded || e.phase == corev1.PodFailed {
				continue
			}

			for j := range e.pod.Spec.Containers {
				c := &e.pod.Spec.Containers[j]
				key := e.uid + "/" + c.Name
				if stateMap[key] != perigeos.StateRunning {
					continue
				}

				ps := bw.deps.Store.ProbeState(e.uid, c.Name)
				if ps == nil {
					bw.logger.Debug("Probe fan-out: skipping (no probe state)",
						"pod", e.pod.Name, "container", c.Name)

					continue
				}

				// Check isDue before spawning to avoid 4000 goroutines
				// blocking on sem at 2000 pods. Most containers aren't due
				// on any given 2s tick with a 10s period.
				hasAnyDue := false
				if c.StartupProbe != nil && isDue(ps, "startup", c.StartupProbe.PeriodSeconds, c.StartupProbe.InitialDelaySeconds) {
					hasAnyDue = true

				} else if c.LivenessProbe != nil && isDue(ps, "liveness", c.LivenessProbe.PeriodSeconds, c.LivenessProbe.InitialDelaySeconds) {
					hasAnyDue = true

				} else if c.ReadinessProbe != nil && isDue(ps, "readiness", c.ReadinessProbe.PeriodSeconds, c.ReadinessProbe.InitialDelaySeconds) {
					hasAnyDue = true
				}

				if !hasAnyDue {
					continue
				}

				probeWg.Go(func() {
					sem <- struct{}{}
					defer func() { <-sem }()
					bw.runProbes(ctx, e.uid, e.pod, c, e.podIP)
				})
			}
		}

		probeWg.Wait()
	}

	// Track which pods have state changes for the coalescer.
	changedPods := make(map[string]bool)

	for _, e := range entries {
		// Skip pods still being created (Pending) - no machine yet.
		if e.phase == corev1.PodPending {
			bw.logger.Debug("Coalescer: skipping Pending pod", "pod", e.pod.Name)

			continue
		}

		// Skip pods being deleted - teardown is in progress, pushing
		// Running/CrashLoopBackOff status would race with VK's terminal push.
		if bw.deps.Store.IsDeleting(e.uid) {
			bw.logger.Debug("Coalescer: skipping deleting pod", "uid", e.uid, "pod", e.pod.Name)

			continue
		}

		// Skip pods in terminal phase - unless a container's state changed
		// to a *different known state*, meaning systemd settled to a different
		// result (e.g. the pod was marked Succeeded from an intermediate
		// "dead" substate, but the unit actually failed).
		// A unit disappearing from stateMap (after ResetUnit cleanup) is NOT
		// a state change - it's expected cleanup.
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
				bw.logger.Debug("Coalescer: state change",
					"pod", e.pod.Name, "container", c.Name,
					"prev", prev, "cur", cur)

				changedPods[e.uid] = true
			}

			// Check readiness changes (probe transitions don't change machine state).
			// Only track when ProbeState exists - before InitRestartState,
			// IsContainerReady defaults to true, which would seed prevReady
			// incorrectly and suppress the Ready=false->true transition.
			if cur == perigeos.StateRunning {
				ps := bw.deps.Store.ProbeState(e.uid, c.Name)
				if ps != nil {
					ready := bw.deps.Store.IsContainerReady(e.uid, c.Name)
					if prev, ok := bw.prevReady[key]; !ok || prev != ready {
						bw.logger.Debug("Coalescer: readiness change",
							"pod", e.pod.Name, "container", c.Name,
							"prev", prev, "cur", ready)

						changedPods[e.uid] = true
						bw.prevReady[key] = ready
					}
				}
			}
		}

		bw.checkPod(ctx, e.uid, e.pod, e.podIP, stateMap, exitCodeMap)
	}

	// Coalescer: push status updates only for pods with actual changes.
	// The downstream enqueuePodStatusUpdate has cmp.Equal dedup as a safety net.
	stateLookup := bw.makeStateLookup(stateMap)
	for _, e := range entries {
		if !changedPods[e.uid] {
			continue
		}

		// Re-read podPhases under lock - checkPod may have set a terminal
		// phase during *this* poll cycle, after the entries snapshot was taken.
		currentPhase := bw.deps.Store.PodPhase(e.uid)
		if currentPhase == corev1.PodPending || currentPhase == corev1.PodSucceeded || currentPhase == corev1.PodFailed {
			bw.logger.Debug("Coalescer: skipping status push (phase filter)",
				"uid", e.uid, "pod", e.pod.Name, "storePhase", currentPhase)

			continue
		}
		// Fetch the current pod under lock rather than using e.pod from the
		// snapshot. The snapshot pointer was valid at poll-start but may be
		// stale by the time the coalescer runs (PromoteRunning could have
		// replaced ps.pod). GetPodCopy gives a consistent DeepCopy of the
		// current store state at push time.
		currentPod := bw.deps.Store.GetPodCopy(e.uid)
		if currentPod == nil {
			bw.logger.Debug("Coalescer: pod deleted mid-cycle, skipping", "uid", e.uid)

			continue
		}

		status := bw.deps.BuildPodStatus(currentPod, stateLookup)
		bw.logger.Info("Coalescer: pushing status",
			"uid", e.uid, "pod", currentPod.Name, "computedPhase", status.Phase,
			"ready", status.Conditions[0].Status,
			"containers", len(status.ContainerStatuses))

		status.DeepCopyInto(&currentPod.Status)
		bw.deps.NotifyStatus(currentPod)
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

func (bw *BatchWatcher) checkPod(ctx context.Context, uid string, pod *corev1.Pod, podIP string, stateMap map[string]perigeos.MachineState, exitCodeMap map[string]int32) {
	policy := pod.Spec.RestartPolicy
	if policy == "" {
		policy = corev1.RestartPolicyAlways
	}

	allExited := true
	allSucceeded := true
	anyRestarting := false // true only when maybeRestart is called this cycle

	for _, c := range pod.Spec.Containers {
		key := uid + "/" + c.Name
		state, exists := stateMap[key]
		if !exists {
			state = perigeos.StateExited
		}

		bw.logger.Debug("checkPod container state", "pod", pod.Name, "container", c.Name, "state", state, "exists", exists, "policy", policy)

		// If a restart goroutine is in-flight for this container
		// (backoff sleep, unit cleanup, or re-launch), the old unit
		// may already be gone from ListManagedMachines while the new
		// one hasn't appeared yet.  Treat it as not-exited to prevent
		// a premature terminal phase (the classic Succeeded-instead-
		// of-CrashLoopBackOff race).
		bw.restartingMu.Lock()
		isRestarting := bw.restarting[key]
		bw.restartingMu.Unlock()
		if isRestarting {
			bw.logger.Debug("Container restart in progress - skipping terminal eval",
				"pod", pod.Name, "container", c.Name)

			allExited = false
			allSucceeded = false

			continue
		}

		// If the container appears exited but was never observed running,
		// it may be in a brief inactive/dead state during unit startup
		// (systemd transitions through inactive->active for transient units,
		// and ExecMainStatus isn't updated until after the unit settles).
		//
		// However, if the machine existed in ListManagedMachines with exit
		// code 0, the unit genuinely ran and completed (e.g. a Job certgen
		// that exits in <1s). Accept that as a successful run.
		if !bw.seenRunning[key] && (state == perigeos.StateExited || state == perigeos.StateUnknown) {
			if exists && state == perigeos.StateExited && exitCodeMap[key] == 0 {
				bw.logger.Debug("Fast-exit container completed successfully (never seen running but exit 0)", "pod", pod.Name, "container", c.Name)

				bw.seenRunning[key] = true
				// Fall through to the normal switch below.

			} else {
				bw.logger.Debug("Deferring terminal decision - container never seen running", "pod", pod.Name, "container", c.Name, "state", state)

				allExited = false
				allSucceeded = false

				continue
			}
		}

		switch state {
		case perigeos.StateRunning, perigeos.StateCreating:
			allExited = false
			allSucceeded = false

			if state == perigeos.StateRunning {
				bw.seenRunning[key] = true
			}

			// Reset backoff if container has been running long enough.
			if state == perigeos.StateRunning {
				rs := bw.deps.Store.RestartState(uid, c.Name)
				if rs != nil && time.Since(rs.lastStarted) > restartBackoffReset {
					bw.deps.Store.ResetBackoff(uid, c.Name)
				}

				// Run probes for running containers.
				bw.runProbes(ctx, uid, pod, &c, podIP)
			}

		case perigeos.StateFailed:
			bw.deps.Store.SetContainerState(uid, c.Name, corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{}}, false)

			// Emit failure event only on state transition to avoid
			// spamming every poll cycle while the container stays failed.
			// The restarting guard above prevents re-entry during
			// backoff, so this fires once per crash cycle.
			if prev, ok := bw.prevStateMap[key]; !ok || prev != perigeos.StateFailed {
				exitCode := exitCodeMap[key]
				bw.deps.EventRecorder.Eventf(pod, corev1.EventTypeWarning, "Failed",
					"Container %s exited with error (code %d)", c.Name, exitCode)
			}

			allSucceeded = false
			if policy == corev1.RestartPolicyAlways || policy == corev1.RestartPolicyOnFailure {
				bw.maybeRestart(ctx, uid, pod, c.Name)
				allExited = false
				anyRestarting = true
			}

		case perigeos.StateExited:
			bw.deps.Store.SetContainerState(uid, c.Name, corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{}}, false)

			// Emit completion/failure event only on state transition.
			// D-Bus "dead" substate is intentionally ignored (see
			// handleUnitEvent comment), so this poll path is the
			// canonical detection point for container exits.
			if prev, ok := bw.prevStateMap[key]; !ok || prev != perigeos.StateExited {
				exitCode := exitCodeMap[key]
				if exitCode == 0 {
					bw.deps.EventRecorder.Eventf(pod, corev1.EventTypeNormal, "Completed",
						"Container %s exited with code %d", c.Name, exitCode)

				} else {
					bw.deps.EventRecorder.Eventf(pod, corev1.EventTypeWarning, "Failed",
						"Container %s exited with error (code %d)", c.Name, exitCode)
				}
			}

			if policy == corev1.RestartPolicyAlways {
				bw.maybeRestart(ctx, uid, pod, c.Name)
				allExited = false
				anyRestarting = true
			}
			// OnFailure + exit 0 -> don't restart.
			// Never -> don't restart.
		}
	}

	if anyRestarting && !bw.deps.Store.IsDeleting(uid) {
		// At least one container is being restarted. Eagerly push a
		// CrashLoopBackOff status so kubectl sees it immediately rather
		// than waiting for the coalescer (which may miss the window if
		// the state transition and the next poll overlap).
		// Only fires when maybeRestart was actually called this cycle -
		// not for plain Running containers, which would cause a spurious
		// push on every poll cycle and bypass the coalescer entirely.
		// Skip for pods being deleted - teardown is in progress and a
		// CrashLoopBackOff push would race with VK's terminal status.
		stateLookup := bw.makeStateLookup(stateMap)
		currentPod := bw.deps.Store.GetPodCopy(uid)
		if currentPod != nil {
			status := bw.deps.BuildPodStatus(currentPod, stateLookup)
			if status.Phase == corev1.PodRunning {
				bw.logger.Info("checkPod: eager restart status push",
					"uid", uid, "pod", currentPod.Name, "ready", status.Conditions[0].Status)

				status.DeepCopyInto(&currentPod.Status)
				bw.deps.NotifyStatus(currentPod)
			}
		}
	}

	if !allExited {
		return
	}

	// All containers exited and none will be restarted - set terminal phase.
	var terminalPhase corev1.PodPhase
	if allSucceeded {
		terminalPhase = corev1.PodSucceeded

	} else {
		terminalPhase = corev1.PodFailed
	}

	bw.logger.Info("checkPod: setting terminal phase",
		"uid", uid, "pod", pod.Name, "phase", terminalPhase, "allSucceeded", allSucceeded)

	// Build terminal status with a full buildPodStatus so restart counts,
	// container states, and conditions are all consistent.
	stateLookup := func(u, cn string) perigeos.MachineState {
		if s, ok := stateMap[u+"/"+cn]; ok {
			return s
		}

		return perigeos.StateExited
	}

	// Update phase first so GetPodStatus returns terminal status even after
	// the systemd unit is cleaned up, then push with current pod.
	bw.deps.Store.SetPhase(uid, terminalPhase)

	currentPod := bw.deps.Store.GetPodCopy(uid)
	if currentPod != nil {
		status := bw.deps.BuildPodStatus(currentPod, stateLookup)
		status.Phase = terminalPhase
		status.DeepCopyInto(&currentPod.Status)
		bw.logger.Info("checkPod: pushing terminal status",
			"uid", uid, "pod", currentPod.Name, "phase", terminalPhase)

		bw.deps.NotifyStatus(currentPod)
	}

	// Clean up dead/failed systemd units now that we've read their state.
	// Without this, transient units accumulate in systemd's listing.
	for _, c := range pod.Spec.Containers {
		if err := bw.deps.Runtime.ResetUnit(ctx, uid, c.Name); err != nil {
			bw.logger.Debug("ResetUnit failed (unit may already be collected)",
				"uid", uid, "pod", pod.Name, "container", c.Name, "err", err)
		}
	}
>>>>>>> master
}

// cleanupStaleUnits removes dead/failed systemd units from a previous
// perigeos lifetime that never got cleaned up (e.g. after a crash).
func (bw *BatchWatcher) cleanupStaleUnits(ctx context.Context) {
	if bw.deps.Store == nil {
		return
	}

	activeUIDs := bw.deps.Store.ActiveUIDs()
	cleaned, err := bw.deps.Runtime.CleanupStaleUnits(ctx, activeUIDs)
	if err != nil {
		bw.logger.Error("Startup stale unit cleanup failed", "err", err)

		return
	}

	if cleaned > 0 {
		bw.logger.Info("Cleaned up stale units from previous run", "count", cleaned)
	}
}
<<<<<<< HEAD
=======

// runProbes executes startup, liveness, and readiness probes for a running
// container, respecting each probe's PeriodSeconds cadence.
func (bw *BatchWatcher) runProbes(ctx context.Context, uid string, pod *corev1.Pod, c *corev1.Container, podIP string) {
	ps := bw.deps.Store.ProbeState(uid, c.Name)
	if ps == nil {
		return
	}

	// 1. Startup probe gates liveness/readiness.
	if c.StartupProbe != nil && !ps.StartupPassed {
		if isDue(ps, "startup", c.StartupProbe.PeriodSeconds, c.StartupProbe.InitialDelaySeconds) {
			// Network I/O outside any lock.
			result := bw.deps.Store.ProbeRunner().RunProbe(ctx, pod, c.Name, c.StartupProbe, podIP)

			bw.logger.Debug("Startup probe result",
				"pod", pod.Name, "container", c.Name, "result", probeResultString(result),
				"failCount", ps.StartupFailCount, "podIP", podIP)

			// Write results under lock - concurrent with isContainerReady readers.
			var restart bool
			bw.deps.Store.UpdateProbeState(uid, c.Name, func(ps *ContainerProbeState) {
				markProbed(ps, "startup")
				restart = EvalStartup(ps, c.StartupProbe, result)
			})

			ps = bw.deps.Store.ProbeState(uid, c.Name)
			if result == ProbeSuccess && ps != nil && ps.StartupPassed {
				bw.logger.Info("Startup probe passed", "pod", pod.Name, "container", c.Name)

				bw.deps.EventRecorder.Eventf(pod, corev1.EventTypeNormal, "Started",
					"Container %s passed startup probe", c.Name)
			}

			if result == ProbeFailure && ps != nil {
				bw.deps.EventRecorder.Eventf(pod, corev1.EventTypeWarning, "Unhealthy",
					"Startup probe failed for container %s (%d/%d)",
					c.Name, ps.StartupFailCount, c.StartupProbe.FailureThreshold)
			}

			if restart {
				bw.logger.Warn("Startup probe failed past threshold, restarting",
					"pod", pod.Name, "container", c.Name)

				bw.maybeRestart(ctx, uid, pod, c.Name)
			}
		}

		return // don't run liveness/readiness until startup passes
	}

	// 2. Liveness probe - failure past threshold triggers restart.
	if c.LivenessProbe != nil && isDue(ps, "liveness", c.LivenessProbe.PeriodSeconds, c.LivenessProbe.InitialDelaySeconds) {
		result := bw.deps.Store.ProbeRunner().RunProbe(ctx, pod, c.Name, c.LivenessProbe, podIP)
		bw.logger.Debug("Liveness probe result",
			"pod", pod.Name, "container", c.Name, "result", probeResultString(result), "podIP", podIP)

		var restart bool
		bw.deps.Store.UpdateProbeState(uid, c.Name, func(ps *ContainerProbeState) {
			markProbed(ps, "liveness")
			restart = EvalLiveness(ps, c.LivenessProbe, result)
		})

		if restart {
			// Reset probe state so the restarted container starts fresh.
			bw.deps.Store.ResetProbeState(uid, c.Name)
		}

		if result == ProbeFailure {
			ps = bw.deps.Store.ProbeState(uid, c.Name)
			if ps != nil {
				bw.deps.EventRecorder.Eventf(pod, corev1.EventTypeWarning, "Unhealthy",
					"Liveness probe failed for container %s (%d/%d)",
					c.Name, ps.LiveFailCount, c.LivenessProbe.FailureThreshold)
			}
		}

		if restart {
			bw.logger.Warn("Liveness probe failed past threshold, restarting",
				"pod", pod.Name, "container", c.Name)

			bw.maybeRestart(ctx, uid, pod, c.Name)

			return
		}
	}

	// 3. Readiness probe - controls Ready condition on the container.
	if c.ReadinessProbe != nil && isDue(ps, "readiness", c.ReadinessProbe.PeriodSeconds, c.ReadinessProbe.InitialDelaySeconds) {
		result := bw.deps.Store.ProbeRunner().RunProbe(ctx, pod, c.Name, c.ReadinessProbe, podIP)

		bw.logger.Debug("Readiness probe result",
			"pod", pod.Name, "container", c.Name, "result", probeResultString(result), "podIP", podIP)

		wasReady := ps.Ready
		bw.deps.Store.UpdateProbeState(uid, c.Name, func(ps *ContainerProbeState) {
			markProbed(ps, "readiness")
			EvalReadiness(ps, c.ReadinessProbe, result)
		})

		ps = bw.deps.Store.ProbeState(uid, c.Name)
		nowReady := ps != nil && ps.Ready
		if result == ProbeFailure {
			bw.deps.EventRecorder.Eventf(pod, corev1.EventTypeWarning, "Unhealthy",
				"Readiness probe failed for container %s", c.Name)
		}

		if !wasReady && nowReady {
			bw.deps.EventRecorder.Eventf(pod, corev1.EventTypeNormal, "ProbeReady",
				"Container %s passed readiness probe", c.Name)
		}
	}
}

// makeStateLookup returns a stateLookup function for buildPodStatus that
// correctly handles containers missing from ListManagedMachines:
//   - restarting -> StateFailed (produces CrashLoopBackOff)
//   - previously seen running -> StateExited (produces Completed)
//   - otherwise -> StateUnknown (produces ContainerCreating)
func (bw *BatchWatcher) makeStateLookup(stateMap map[string]perigeos.MachineState) func(uid, containerName string) perigeos.MachineState {
	return func(uid, containerName string) perigeos.MachineState {
		key := uid + "/" + containerName
		if s, ok := stateMap[key]; ok {
			return s
		}

		bw.restartingMu.Lock()
		restarting := bw.restarting[key]
		bw.restartingMu.Unlock()

		if restarting {
			return perigeos.StateFailed
		}

		// Container was seen running but its unit is gone and it's not
		// being restarted - it completed.
		// pollMu is already held by poll() (caller)
		// Just read the map directly.
		seen := bw.seenRunning[key]

		if seen {
			return perigeos.StateExited
		}

		return perigeos.StateUnknown
	}
}

// maybeRestart launches a restart goroutine for a container if one isn't
// already running. Prevents double-restarts between poll cycles.
// Skips the restart if the pod has been removed from gambit (DeletePod in progress).
func (bw *BatchWatcher) maybeRestart(ctx context.Context, uid string, pod *corev1.Pod, containerName string) {
	key := uid + "/" + containerName

	// Don't restart containers for pods that are being deleted.
	if bw.deps.Store.IsDeleting(uid) {
		return
	}

	bw.restartingMu.Lock()

	if bw.restarting[key] {
		bw.restartingMu.Unlock()
		return

	}
	bw.restarting[key] = true
	bw.restartingMu.Unlock()

	// Bump backoff BEFORE launching the goroutine so that the eager
	// CrashLoopBackOff status push (which reads RestartCounts from the
	// store) already sees the incremented count.
	count, backoff := bw.deps.Store.BumpBackoff(uid, containerName)
	if count == 0 {
		bw.restartingMu.Lock()
		delete(bw.restarting, key)
		bw.restartingMu.Unlock()

		return
	}

	go func() {
		defer func() {
			bw.restartingMu.Lock()
			delete(bw.restarting, key)
			bw.restartingMu.Unlock()
		}()

		bw.deps.RestartContainer(ctx, uid, pod, containerName, count, backoff)
	}()
}
>>>>>>> master
