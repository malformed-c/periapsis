package foci

// Conversion between PodState (flat value types) and corev1.PodStatus.
// This is the output boundary of the state machine — PodState is the
// internal representation, PodStatus is the external K8s API representation.

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"github.com/malformed-c/periapsis/internal/types"
)

// BuildPodStatusPayload converts a PodState into a PodStatusPayload
// suitable for sending to Horizon.
func BuildPodStatusPayload(state PodState) types.PodStatusPayload {
	containerStatuses := make([]types.ContainerStatusPayload, 0, len(state.Containers))

	for _, cv := range state.Containers {
		cs := types.ContainerStatusPayload{
			Name:         cv.Name,
			Image:        cv.Image,
			Ready:        cv.Ready,
			RestartCount: cv.RestartCount,
			State:        containerStateToPayload(cv),
		}
		containerStatuses = append(containerStatuses, cs)
	}

	readyCondition := corev1.ConditionFalse
	allReady := true
	for _, cs := range containerStatuses {
		if !cs.Ready {
			allReady = false
			break
		}
	}
	if allReady {
		readyCondition = corev1.ConditionTrue
	}

	return types.PodStatusPayload{
		Phase: state.Phase,
		Conditions: []types.PodCondition{{
			Type:   corev1.PodReady,
			Status: readyCondition,
		}},
		ContainerStatuses: containerStatuses,
		HostIP:            state.PodIP, // will be overwritten by Horizon with nodeIP
		PodIP:             state.PodIP,
	}
}

// containerStateToPayload converts a ContainerState's Phase to the
// appropriate corev1-compatible payload.
func containerStateToPayload(cv ContainerState) types.ContainerStatePayload {
	switch cv.Phase {
	case PhaseCreating:
		return types.ContainerStatePayload{
			Waiting: &types.WaitingPayload{Reason: "ContainerCreating"},
		}
	case PhaseRunning:
		return types.ContainerStatePayload{
			Running: &types.RunningPayload{
				StartedAt: metav1.NewTime(cv.LastStarted),
			},
		}
	case PhaseTerminated:
		return types.ContainerStatePayload{
			Terminated: &types.TerminatedPayload{
				ExitCode:   cv.ExitCode,
				Reason:     cv.ExitReason,
				StartedAt:  metav1.NewTime(cv.LastStarted),
				FinishedAt: metav1.Now(),
			},
		}
	case PhaseCrashLoopBackOff:
		return types.ContainerStatePayload{
			Waiting: &types.WaitingPayload{Reason: "CrashLoopBackOff"},
		}
	default:
		return types.ContainerStatePayload{
			Waiting: &types.WaitingPayload{Reason: "ContainerCreating"},
		}
	}
}

// PodStatusPayloadToCorev1 converts our flat PodStatusPayload to a
// corev1.PodStatus for use by Horizon's k8s API calls.
func PodStatusPayloadToCorev1(payload types.PodStatusPayload) corev1.PodStatus {
	conditions := make([]corev1.PodCondition, 0, len(payload.Conditions))
	for _, c := range payload.Conditions {
		conditions = append(conditions, corev1.PodCondition{
			Type:   c.Type,
			Status: c.Status,
		})
	}

	containerStatuses := make([]corev1.ContainerStatus, 0, len(payload.ContainerStatuses))
	for _, cs := range payload.ContainerStatuses {
		containerStatuses = append(containerStatuses, corev1.ContainerStatus{
			Name:         cs.Name,
			Image:        cs.Image,
			Ready:        cs.Ready,
			RestartCount: cs.RestartCount,
			State:        payloadToContainerState(cs.State),
		})
	}

	return corev1.PodStatus{
		Phase:              payload.Phase,
		Conditions:         conditions,
		ContainerStatuses:  containerStatuses,
		HostIP:             payload.HostIP,
		PodIP:              payload.PodIP,
	}
}

// payloadToContainerState converts our flat ContainerStatePayload to a
// corev1.ContainerState.
func payloadToContainerState(p types.ContainerStatePayload) corev1.ContainerState {
	if p.Waiting != nil {
		return corev1.ContainerState{
			Waiting: &corev1.ContainerStateWaiting{Reason: p.Waiting.Reason},
		}
	}
	if p.Running != nil {
		return corev1.ContainerState{
			Running: &corev1.ContainerStateRunning{StartedAt: p.Running.StartedAt},
		}
	}
	if p.Terminated != nil {
		return corev1.ContainerState{
			Terminated: &corev1.ContainerStateTerminated{
				ExitCode:   p.Terminated.ExitCode,
				Reason:     p.Terminated.Reason,
				StartedAt:  p.Terminated.StartedAt,
				FinishedAt: p.Terminated.FinishedAt,
			},
		}
	}
	return corev1.ContainerState{
		Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"},
	}
}
