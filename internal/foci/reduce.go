package foci

// Reduce is the pure state machine transition function.
//
//   Reduce(state, fact) → (newState, effects)
//
// Invariants:
//   - No side effects - no I/O, no API calls, no shared mutation
//   - Deterministic - same (state, fact) always produces same (newState, effects)
//   - State is passed by value - the caller's copy is never mutated
//   - Effects are the only way to interact with the outside world
//
// The caller (Syzygy) is responsible for:
//   - Storing the returned newState in its map
//   - Executing the returned effects (in order)
//   - Deduplicating status updates (coalescing)

import (
	"fmt"
	"strings"
	"time"

	perigeos "github.com/malformed-c/periapsis/internal/runtime"
	"github.com/malformed-c/periapsis/internal/types"
	corev1 "k8s.io/api/core/v1"
)

// Reduce applies a Fact to a PodState and returns the new state plus
// any side effects. If the PodState is zero-valued (UID is empty), the
// fact is for an untracked pod and is ignored.
func Reduce(state PodState, fact types.Fact) (PodState, []types.Effect) {
	// Ignore facts for untracked pods.
	if state.UID == "" {
		return state, nil
	}

	switch f := fact.(type) {
	case *types.UnitFact:
		return reduceUnitFact(state, f)
	case *types.ContainerStateFact:
		return reduceContainerStateFact(state, f)
	case *types.ExitFact:
		return reduceExitFact(state, f)
	case *types.ProbeFact:
		return reduceProbeFact(state, f)
	case *types.SpecFact:
		return reduceSpecFact(state, f)
	case *types.PodAdmitFact:
		return reducePodAdmitFact(state, f)
	case *types.PodEvictFact:
		return reducePodEvictFact(state)
	case *types.MarkRunningFact:
		return reduceMarkRunningFact(state, f)
	case *types.BackoffResetFact:
		return reduceBackoffResetFact(state, f)
	default:
		return state, nil
	}
}

// --- UnitFact ---

func reduceUnitFact(state PodState, fact *types.UnitFact) (PodState, []types.Effect) {
	containerName := ParseContainerFromUnit(fact.UnitName)
	if containerName == "" {
		return state, nil
	}

	idx := state.FindContainer(containerName)
	if idx == -1 {
		return state, nil
	}

	var effects []types.Effect
	cv := &state.Containers[idx]
	prevPhase := cv.Phase

	switch fact.SubState {
	case "running":
		cv.Phase = PhaseRunning
		cv.SeenRunning = true
		cv.Restarting = false
		cv.LastStarted = time.Now()
		if prevPhase != PhaseRunning {
			effects = append(effects, types.RecordEvent{
				UID:       state.UID,
				EventType: corev1.EventTypeNormal,
				Reason:    "Started",
				Message:   fmt.Sprintf("Container %s started", containerName),
			})
		}

	case "failed":
		cv.Phase = PhaseCrashLoopBackOff
		cv.ExitCode = fact.ExitCode
		if prevPhase != PhaseCrashLoopBackOff {
			effects = append(effects, types.RecordEvent{
				UID:       state.UID,
				EventType: corev1.EventTypeWarning,
				Reason:    "Failed",
				Message:   fmt.Sprintf("Container %s exited with error (code %d)", containerName, fact.ExitCode),
			})
		}
		// Restart decision
		if shouldRestart(state.Spec.RestartPolicy, cv.ExitCode) {
			cv.Restarting = true
			cv.RestartCount++
			cv.Backoff = min(cv.Backoff*2, MaxBackoff)
			effects = append(effects, types.RestartContainer{
				UID:           state.UID,
				Namespace:     state.Namespace,
				PodName:       state.Name,
				ContainerName: containerName,
				RestartCount:  cv.RestartCount,
				Backoff:       cv.Backoff,
			})
		}

	case "start-pre", "start", "start-post":
		cv.Phase = PhaseCreating

	case "stop-sigterm", "stop-watchdog":
		// systemd sent SIGTERM; container has terminationGracePeriodSeconds to exit.
		// Emit event but don't change phase - container is still alive.
		return state, []types.Effect{types.RecordEvent{
			UID:       state.UID,
			EventType: corev1.EventTypeNormal,
			Reason:    "Killing",
			Message:   fmt.Sprintf("Container %s received SIGTERM, waiting for graceful exit", containerName),
		}}

	case "stop-sigkill", "stop-kill":
		// Grace period expired; systemd is sending SIGKILL.
		return state, []types.Effect{types.RecordEvent{
			UID:       state.UID,
			EventType: corev1.EventTypeWarning,
			Reason:    "Killing",
			Message:   fmt.Sprintf("Container %s grace period expired, sending SIGKILL", containerName),
		}}

	default:
		// Ignore "dead" and other transient substates.
		return state, nil
	}

	if prevPhase != cv.Phase || cv.Phase == PhaseRunning {
		newState, statusEff := recomputePhase(state)
		effects = append(effects, statusEff...)
		state = newState
	}

	return state, effects
}

// --- ContainerStateFact (from poll) ---

func reduceContainerStateFact(state PodState, fact *types.ContainerStateFact) (PodState, []types.Effect) {
	idx := state.FindContainer(fact.Container)
	if idx == -1 {
		return state, nil
	}

	var effects []types.Effect
	cv := &state.Containers[idx]
	prevPhase := cv.Phase

	switch fact.State {
	case perigeos.StateRunning:
		cv.Phase = PhaseRunning
		cv.SeenRunning = true
		cv.Restarting = false
		cv.LastStarted = time.Now()

		// Reset backoff if running stably.
		if !cv.LastStarted.IsZero() && time.Since(cv.LastStarted) > RestartBackoffReset {
			cv.Backoff = RestartBackoffInit
		}

	case perigeos.StateCreating:
		cv.Phase = PhaseCreating

	case perigeos.StateFailed:
		cv.ExitCode = fact.ExitCode
		if shouldRestart(state.Spec.RestartPolicy, fact.ExitCode) {
			cv.Phase = PhaseCrashLoopBackOff
			if !cv.Restarting {
				cv.Restarting = true
				cv.RestartCount++
				cv.Backoff = min(cv.Backoff*2, MaxBackoff)
				effects = append(effects, types.RestartContainer{
					UID:           state.UID,
					Namespace:     state.Namespace,
					PodName:       state.Name,
					ContainerName: fact.Container,
					RestartCount:  cv.RestartCount,
					Backoff:       cv.Backoff,
				})
			}
		} else {
			cv.Phase = PhaseTerminated
			cv.ExitReason = "Error"
		}
		if prevPhase != PhaseCrashLoopBackOff {
			effects = append(effects, types.RecordEvent{
				UID:       state.UID,
				EventType: corev1.EventTypeWarning,
				Reason:    "Failed",
				Message:   fmt.Sprintf("Container %s exited with error (code %d)", fact.Container, fact.ExitCode),
			})
		}

	case perigeos.StateExited:
		cv.ExitCode = fact.ExitCode
		if state.Spec.RestartPolicy == corev1.RestartPolicyAlways {
			cv.Phase = PhaseCrashLoopBackOff
			if !cv.Restarting {
				cv.Restarting = true
				cv.RestartCount++
				cv.Backoff = min(cv.Backoff*2, MaxBackoff)
				effects = append(effects, types.RestartContainer{
					UID:           state.UID,
					Namespace:     state.Namespace,
					PodName:       state.Name,
					ContainerName: fact.Container,
					RestartCount:  cv.RestartCount,
					Backoff:       cv.Backoff,
				})
			}
		} else {
			cv.Phase = PhaseTerminated
			if fact.ExitCode == 0 {
				cv.ExitReason = "Completed"
			} else {
				cv.ExitReason = "Error"
			}
		}
		if prevPhase != PhaseTerminated && prevPhase != PhaseCrashLoopBackOff {
			if fact.ExitCode == 0 {
				effects = append(effects, types.RecordEvent{
					UID:       state.UID,
					EventType: corev1.EventTypeNormal,
					Reason:    "Completed",
					Message:   fmt.Sprintf("Container %s exited with code %d", fact.Container, fact.ExitCode),
				})
			} else {
				effects = append(effects, types.RecordEvent{
					UID:       state.UID,
					EventType: corev1.EventTypeWarning,
					Reason:    "Failed",
					Message:   fmt.Sprintf("Container %s exited with error (code %d)", fact.Container, fact.ExitCode),
				})
			}
		}

	default:
		// StateUnknown - container not in ListManagedMachines.
		// If restarting, keep CrashLoopBackOff.
		// If seen running, set Terminated (unit was collected).
		// Otherwise, keep Creating (never started).
		if cv.Restarting {
			// No state change - restart in progress.
			return state, nil
		}
		if cv.SeenRunning {
			cv.Phase = PhaseTerminated
			cv.ExitCode = 0
			cv.ExitReason = "Completed"
		} else {
			// Never seen running - defer, keep Creating.
			return state, nil
		}
	}

	if prevPhase != cv.Phase {
		newState, statusEff := recomputePhase(state)
		effects = append(effects, statusEff...)
		state = newState
	}

	return state, effects
}

// --- ExitFact ---

func reduceExitFact(state PodState, fact *types.ExitFact) (PodState, []types.Effect) {
	idx := state.FindContainer(fact.Container)
	if idx == -1 {
		return state, nil
	}

	var effects []types.Effect
	cv := &state.Containers[idx]

	// If we never saw it running, defer the terminal decision.
	if !cv.SeenRunning {
		cv.Phase = PhaseCreating
		return state, nil
	}

	prevPhase := cv.Phase

	if fact.WillRestart {
		cv.Phase = PhaseCrashLoopBackOff
		cv.Restarting = true
		cv.RestartCount++
		cv.Backoff = min(cv.Backoff*2, MaxBackoff)
		effects = append(effects, types.RestartContainer{
			UID:           state.UID,
			Namespace:     state.Namespace,
			PodName:       state.Name,
			ContainerName: fact.Container,
			RestartCount:  cv.RestartCount,
			Backoff:       cv.Backoff,
		})
	} else {
		cv.Phase = PhaseTerminated
		cv.ExitCode = fact.ExitCode
		cv.ExitReason = fact.Reason
		if cv.ExitReason == "" {
			if fact.ExitCode == 0 {
				cv.ExitReason = "Completed"
			} else {
				cv.ExitReason = "Error"
			}
		}
	}

	if prevPhase != cv.Phase {
		newState, statusEff := recomputePhase(state)
		effects = append(effects, statusEff...)
		state = newState
	}

	return state, effects
}

// --- ProbeFact ---

func reduceProbeFact(state PodState, fact *types.ProbeFact) (PodState, []types.Effect) {
	idx := state.FindContainer(fact.Container)
	if idx == -1 {
		return state, nil
	}

	cv := &state.Containers[idx]
	wasReady := cv.Ready

	switch fact.ProbeType {
	case "readiness":
		cv.Ready = fact.Ready
	case "startup":
		cv.StartupPassed = fact.StartupPassed
		// If startup passed, the container may now become ready.
		// Don't update readiness here - the next readiness ProbeFact will do that.
	case "liveness":
		// Liveness failure triggers restart - handled by ProbeScheduler.
		// We just track the state.
	}

	var effects []types.Effect
	if wasReady != cv.Ready {
		if cv.Ready {
			effects = append(effects, types.RecordEvent{
				UID:       state.UID,
				EventType: corev1.EventTypeNormal,
				Reason:    "ProbeReady",
				Message:   fmt.Sprintf("Container %s passed readiness probe", fact.Container),
			})
		}
		newState, statusEff := recomputePhase(state)
		effects = append(effects, statusEff...)
		state = newState
	}

	return state, effects
}

// --- SpecFact ---

func reduceSpecFact(state PodState, fact *types.SpecFact) (PodState, []types.Effect) {
	if fact.Pod == nil {
		return state, nil
	}

	// Rebuild PodSpec from the new pod.
	newSpec := NewPodSpec(fact.Pod)
	state.Spec = newSpec

	// Sync container states - add new containers, keep existing state.
	// Removed containers are dropped (their state is lost, which is fine
	// for image updates).
	newContainers := make([]ContainerState, 0, len(newSpec.Containers))
	for _, cs := range newSpec.Containers {
		idx := state.FindContainer(cs.Name)
		if idx != -1 {
			// Existing container - keep state, update image.
			existing := state.Containers[idx]
			existing.Image = cs.Image
			newContainers = append(newContainers, existing)
		} else {
			// New container - start in Creating.
			newContainers = append(newContainers, ContainerState{
				Name:        cs.Name,
				Image:       cs.Image,
				Phase:       PhaseCreating,
				Ready:       !cs.HasReadinessProbe,
				Backoff:     RestartBackoffInit,
				LastStarted: time.Now(),
			})
		}
	}
	state.Containers = newContainers

	// Recompute status in case the spec change affects phase/readiness.
	newState, effects := recomputePhase(state)
	return newState, effects
}

// --- PodAdmitFact ---

func reducePodAdmitFact(state PodState, fact *types.PodAdmitFact) (PodState, []types.Effect) {
	if state.UID != "" {
		// Already tracking this pod - update spec.
		return reduceSpecFact(state, types.NewSpecFact(fact.UID(), fact.Namespace, fact.Name, fact.Pod))
	}

	// New pod - create initial state.
	newState := NewPodState(fact.UID(), fact.Namespace, fact.Name, fact.PodIP, fact.Pod)

	containers := make([]types.ContainerInitPayload, 0, len(newState.Spec.Containers))
	for _, cs := range newState.Spec.Containers {
		containers = append(containers, types.ContainerInitPayload{
			Name:              cs.Name,
			HasReadinessProbe: cs.HasReadinessProbe,
		})
	}

	effects := []types.Effect{
		types.InitRestartState{
			UID:        fact.UID(),
			Namespace:  fact.Namespace,
			Name:       fact.Name,
			Containers: containers,
		},
		types.PersistPodState{UID: fact.UID()},
	}
	return newState, effects
}

// --- PodEvictFact ---

func reducePodEvictFact(state PodState) (PodState, []types.Effect) {
	// Emit ResetUnit for every tracked container so Horizon cleans up
	// the systemd units before we drop the state.
	effects := make([]types.Effect, 0, len(state.Containers))
	for _, cv := range state.Containers {
		effects = append(effects, types.ResetUnit{
			UID:           state.UID,
			ContainerName: cv.Name,
		})
	}
	// Return zero-value PodState to signal removal from the state map.
	return PodState{}, effects
}

// --- MarkRunningFact ---

func reduceMarkRunningFact(state PodState, fact *types.MarkRunningFact) (PodState, []types.Effect) {
	idx := state.FindContainer(fact.ContainerName)
	if idx == -1 {
		return state, nil
	}

	state.Containers[idx].SeenRunning = true
	return state, nil
}

// --- BackoffResetFact ---

func reduceBackoffResetFact(state PodState, fact *types.BackoffResetFact) (PodState, []types.Effect) {
	idx := state.FindContainer(fact.ContainerName)
	if idx == -1 {
		return state, nil
	}

	state.Containers[idx].Backoff = RestartBackoffInit
	return state, nil
}

// --- Phase Computation ---

// recomputePhase computes the pod-level phase and readiness from the
// container states, then emits an UpdateStatus effect if the status
// has materially changed.
func recomputePhase(state PodState) (PodState, []types.Effect) {
	prevPhase := state.Phase
	anyRunning := false
	anyCreating := false
	anyRestarting := false
	allTerminated := true
	allSucceeded := true

	for i := range state.Containers {
		cv := &state.Containers[i]
		switch cv.Phase {
		case PhaseRunning:
			anyRunning = true
			allTerminated = false
		case PhaseCreating:
			anyCreating = true
			allTerminated = false
		case PhaseCrashLoopBackOff:
			anyRestarting = true
			allTerminated = false
			allSucceeded = false
		case PhaseTerminated:
			// Terminal - check exit code.
			if cv.ExitCode != 0 {
				allSucceeded = false
			}
		}
	}

	// Compute pod phase.
	switch {
	case anyRunning:
		state.Phase = corev1.PodRunning
	case anyCreating:
		state.Phase = corev1.PodPending
	case anyRestarting:
		state.Phase = corev1.PodRunning
	case allTerminated:
		if allSucceeded {
			state.Phase = corev1.PodSucceeded
		} else {
			state.Phase = corev1.PodFailed
		}
	default:
		// No containers or all unknown - keep current phase.
	}

	// Build status payload.
	status := BuildPodStatusPayload(state)

	var effects []types.Effect
	effects = append(effects, types.UpdateStatus{
		UID:       state.UID,
		Namespace: state.Namespace,
		Name:      state.Name,
		Status:    status,
	})

	// Terminal phase - set in PodStore and clean up systemd units.
	if state.Phase == corev1.PodSucceeded || state.Phase == corev1.PodFailed {
		if prevPhase != state.Phase {
			effects = append(effects, types.SetPodPhase{
				UID:   state.UID,
				Phase: state.Phase,
			})
			for _, cv := range state.Containers {
				effects = append(effects, types.ResetUnit{
					UID:           state.UID,
					ContainerName: cv.Name,
				})
			}
		}
	}

	effects = append(effects, types.PersistPodState{UID: state.UID})

	return state, effects
}

// --- Helpers ---

// shouldRestart returns whether a container with the given exit code should
// be restarted based on the pod's restart policy.
func shouldRestart(policy corev1.RestartPolicy, exitCode int32) bool {
	switch policy {
	case corev1.RestartPolicyAlways:
		return true
	case corev1.RestartPolicyOnFailure:
		return exitCode != 0
	default: // Never
		return false
	}
}

// ParseContainerFromUnit extracts the container name from a systemd unit name.
// Format: perigeos-<pawn>-pod-<uid>-<containerName>.service
func ParseContainerFromUnit(unitName string) string {
	suffix := ".service"
	if !strings.HasSuffix(unitName, suffix) {
		return ""
	}

	prefix := "perigeos-"
	idx := strings.Index(unitName, prefix)
	if idx == -1 {
		return ""
	}
	rest := unitName[idx+len(prefix):] // "<pawn>-pod-<uid>-<containerName>.service"

	podIdx := strings.Index(rest, "-pod-")
	if podIdx == -1 {
		return ""
	}
	afterPod := rest[podIdx+5:]                     // "<uid>-<containerName>.service"
	afterPod = afterPod[:len(afterPod)-len(suffix)] // "<uid>-<containerName>"

	// UID is 36 chars (8-4-4-4-12). Container name starts after UID + hyphen.
	if len(afterPod) < 38 {
		return ""
	}

	return afterPod[37:]
}

// min returns the smaller of a and b.
func min(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
