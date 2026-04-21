package node

// TODO God object

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	perigeos "github.com/malformed-c/periapsis/internal/runtime"
	"github.com/malformed-c/periapsis/internal/types"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
)

// BatchWatcherDeps holds all external dependencies for the BatchWatcher,
// replacing the direct *Gambit reference.
type BatchWatcherDeps struct {
	Store         *PodStore
	Runtime       perigeos.Runtime
	EventRecorder record.EventRecorder
	Logger        *slog.Logger
	PawnName      string

	// Callbacks into Gambit (avoids circular import).
	NotifyStatus     func(*corev1.Pod)
	RestartContainer func(ctx context.Context, uid string, pod *corev1.Pod, containerName string, count int32, backoff time.Duration)
	BuildPodStatus   func(pod *corev1.Pod, stateLookup func(string, string) perigeos.MachineState) *corev1.PodStatus
	ParseUnitName    func(unitName string) (uid, containerName string)

	// SendFact forwards a Fact to the Syzygy event loop.
	// If nil, fact emission is skipped (e.g. in tests that don't wire Syzygy).
	// Used by handleUnitEvent to hand off event-recording logic to Reduce.
	SendFact func(fact types.Fact) bool
}

// maxConcurrentProbes is the maximum number of probe HTTP/TCP/exec calls
// that may be in-flight simultaneously. Keeps file-descriptor and goroutine
// pressure bounded when hundreds of pods are running on one pawn.
const maxConcurrentProbes = 50

// BatchWatcher replaces per-pod watcher goroutines with a single goroutine
// per pawn that calls ListManagedMachines once per cycle (1 D-Bus call),
// diffs against expected state, and handles restart policy + probes.
//
// It also acts as the status coalescer: each cycle it compares the current
// container state map against the previous one and pushes status updates
// (via notifyPodStatus) only for pods whose observable state actually changed.
// This eliminates the need for the 5s poll-all-pods loop - status updates
// are O(changed_pods) instead of O(all_pods).
//
// The watcher is hybrid event+poll: it subscribes to D-Bus unit state signals
// via Runtime.SubscribeEvents so container exits are detected immediately,
// with the ticker as a consistency fallback.
type BatchWatcher struct {
	deps   BatchWatcherDeps
	logger *slog.Logger
	cancel context.CancelFunc
	done   chan struct{}

	// pokeCh receives non-blocking sends when a pod lifecycle event
	// (creation, deletion) occurs. This triggers an immediate poll so
	// fast-exit containers are detected without waiting for the ticker.
	pokeCh chan struct{}

	// prevStateMap holds the stateMap from the previous poll cycle.
	// Used by the coalescer to detect container state transitions.
	prevStateMap map[string]perigeos.MachineState

	// prevReady tracks the last-known Ready state per container.
	// Readiness changes (probe pass/fail) don't change the machine state
	// but do affect the pod's Ready condition, so they need separate tracking.
	prevReady map[string]bool // key: uid/containerName

	// stateCache holds the latest stateMap for external consumers
	// (e.g. GetPodStatus) to read without per-container D-Bus calls.
	stateCacheMu sync.RWMutex
	stateCache   map[string]perigeos.MachineState

	// seenRunning tracks containers that have been observed in Running
	// state at least once. Used to prevent premature terminal phase
	// decisions: systemd units briefly pass through inactive/dead during
	// startup, and ExecMainStatus isn't updated until after the unit
	// settles. We only make terminal decisions for containers we've
	// confirmed were actually running.
	// Protected exclusively by pollMu.
	seenRunning map[string]bool // key: uid/containerName

	// pollMu serializes poll() and handleUnitEvent() to prevent concurrent
	// access to prevStateMap, prevReady, and seenRunning.
	pollMu sync.Mutex

	// restarting tracks containers currently in a restart goroutine
	// to prevent double-restarts between poll cycles.
	restartingMu sync.Mutex
	restarting   map[string]bool // key: uid/containerName
}

// Lock helpers
func withLock(mu sync.Locker, fn func()) {
	mu.Lock()
	defer mu.Unlock()

	fn()
}

type readLocker interface {
	RLock()
	RUnlock()
}

func withRLock(mu readLocker, fn func()) {
	mu.RLock()
	defer mu.RUnlock()
	fn()
}

// StartBatchWatcher creates and starts the batch watcher for a Gambit pawn.
func StartBatchWatcher(deps BatchWatcherDeps) *BatchWatcher {
	if deps.EventRecorder == nil {
		deps.EventRecorder = record.NewFakeRecorder(1024)
	}
	ctx, cancel := context.WithCancel(context.Background())
	bw := &BatchWatcher{
		deps:         deps,
		logger:       deps.Logger.With("component", "batchwatcher"),
		cancel:       cancel,
		done:         make(chan struct{}),
		pokeCh:       make(chan struct{}, 1),
		prevStateMap: make(map[string]perigeos.MachineState),
		prevReady:    make(map[string]bool),
		stateCache:   make(map[string]perigeos.MachineState),
		seenRunning:  make(map[string]bool),
		restarting:   make(map[string]bool),
	}
	// Seed seenRunning from hydrated pods - these containers were running
	// before perigeos restarted, so the BatchWatcher must know they started
	// even though it never observed the Running transition. Without this,
	// containers killed by KillMode=control-group appear as Exited-never-ran
	// and the restart/terminal logic defers forever.
	pods := deps.Store.GetPods()
	for _, pod := range pods {
		uid := string(pod.UID)
		if deps.Store.PodPhase(uid) == corev1.PodRunning {
			for _, c := range pod.Spec.Containers {
				bw.seenRunning[uid+"/"+c.Name] = true
			}
		}
	}

	go bw.run(ctx)
	return bw
}

// Poke triggers an immediate poll cycle. Non-blocking - if a poke is already
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
	key := uid + "/" + containerName
	bw.pollMu.Lock()
	bw.seenRunning[key] = true
	bw.pollMu.Unlock()
}

// ContainerState returns the cached state for a container from the most recent
// poll cycle. Returns StateUnknown if no cache entry exists yet.
func (bw *BatchWatcher) ContainerState(uid, containerName string) perigeos.MachineState {
	key := uid + "/" + containerName
	bw.stateCacheMu.RLock()
	state, ok := bw.stateCache[key]
	bw.stateCacheMu.RUnlock()
	if !ok {
		return perigeos.StateUnknown
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
	eventCh := bw.deps.Runtime.SubscribeEvents(ctx)

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
// This is more targeted than a full poll - it only touches the affected
// container, giving sub-second detection for fast-exit containers.
// It:
//   - Updates the local stateCache (used by the query path / BuildPodStatus)
//   - Emits a UnitFact to Syzygy for state-machine transitions and k8s event recording
//   - Triggers a full poll on Running/Failed transitions
//
// Event recording (Started, Killing/SIGTERM, Killing/SIGKILL) has moved to
// foci.Reduce via RecordEvent effects - BW no longer calls EventRecorder.Eventf
// directly for container lifecycle events.
func (bw *BatchWatcher) handleUnitEvent(ctx context.Context, ev perigeos.UnitEvent) {
	// QUICK FILTER: Does this unit even belong to this pawn?
	// The unit name starts with "perigeos-<pawn>-..."
	if !strings.HasPrefix(ev.UnitName, "perigeos-"+bw.deps.PawnName+"-") {
		return
	}

	// Parse uid and containerName from the unit name
	// Format: perigeos-<pawn>-pod-<uid>-<containerName>.service
	uid, containerName := bw.deps.ParseUnitName(ev.UnitName)
	if uid == "" {
		return
	}

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

	// Emit UnitFact to Syzygy for all substates that affect pod state or
	// produce k8s events (Started, Killing). Reduce handles the logic;
	// RecordEvent effects flow back through Horizon.
	//
	// Substates that Reduce handles:
	//   running, failed, start-pre, start, start-post  → phase transitions + Started event
	//   stop-sigterm, stop-watchdog                    → Killing/SIGTERM event
	//   stop-sigkill, stop-kill                        → Killing/SIGKILL event
	//   dead, others                                   → ignored by Reduce (default case)
	if bw.deps.SendFact != nil {
		bw.deps.SendFact(types.NewUnitFact(uid, ev.UnitName, ev.SubState, 0))
	}

	// Map substate → MachineState for the local stateCache.
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
		return
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
		return
	}

	// Trigger a full poll on transitions that affect pod phase.
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
		// (systemd transitions through inactive→active for transient units,
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
			// OnFailure + exit 0 → don't restart.
			// Never → don't restart.
		}
	}

	if anyRestarting {
		// At least one container is being restarted. Eagerly push a
		// CrashLoopBackOff status so kubectl sees it immediately rather
		// than waiting for the coalescer (which may miss the window if
		// the state transition and the next poll overlap).
		// Only fires when maybeRestart was actually called this cycle —
		// not for plain Running containers, which would cause a spurious
		// push on every poll cycle and bypass the coalescer entirely.
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
}

// cleanupStaleUnits removes dead/failed systemd units from a previous
// perigeos lifetime that never got cleaned up (e.g. after a crash).
func (bw *BatchWatcher) cleanupStaleUnits(ctx context.Context) {
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
//   - restarting → StateFailed (produces CrashLoopBackOff)
//   - previously seen running → StateExited (produces Completed)
//   - otherwise → StateUnknown (produces ContainerCreating)
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
		bw.pollMu.Lock()
		seen := bw.seenRunning[key]
		bw.pollMu.Unlock()
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
