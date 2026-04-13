package node

import (
	"context"

	"github.com/malformed-c/periapsis/node/api"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	perigeos "github.com/malformed-c/periapsis/internal/runtime"
)

func (g *Gambit) GetPod(_ context.Context, namespace, name string) (*corev1.Pod, error) {
	return g.store.GetPod(namespace, name)
}

func (g *Gambit) GetPodStatus(ctx context.Context, namespace, name string) (*corev1.PodStatus, error) {
	targetPod, err := g.store.GetPod(namespace, name)
	if err != nil {
		return nil, err
	}

	uid := string(targetPod.UID)

	// Pod is queued, waiting for a createSem slot — no machine exists yet.
	// Return Pending so VK doesn't interpret NotFound as a missing pod.
	phase := g.store.PodPhase(uid)
	if phase == corev1.PodPending {
		return &corev1.PodStatus{Phase: corev1.PodPending}, nil
	}

	// If the pod is in a terminal phase (set by BatchWatcher), return
	// the stored status directly. The systemd unit may already be cleaned
	// up (ResetUnit), so querying the stateCache would give stale results.
	if phase == corev1.PodSucceeded || phase == corev1.PodFailed {
		return &targetPod.Status, nil
	}

	// If the pod was marked as failed during creation (e.g. CNI error,
	// image pull failure), return the stored failure status directly.
	// No machine was started, so querying systemd would give misleading
	// "Completed" / "Unknown" states.
	if targetPod.Status.Phase == corev1.PodFailed {
		return &targetPod.Status, nil
	}

	// Use the BatchWatcher's cached stateMap if available, otherwise fall
	// back to per-container D-Bus queries.
	var stateLookup func(uid, containerName string) perigeos.MachineState
	if g.batchWatcher != nil {
		stateLookup = g.batchWatcher.ContainerState
	} else {
		stateLookup = func(uid, containerName string) perigeos.MachineState {
			state, err := g.Runtime.MachineStatus(ctx, uid, containerName)
			if err != nil {
				return perigeos.StateUnknown
			}
			return state
		}
	}

	return g.buildPodStatus(targetPod, stateLookup), nil
}

// buildPodStatus constructs a PodStatus from the pod spec and a state lookup
// function. Used by both GetPodStatus (on-demand) and the BatchWatcher
// coalescer (push on change). The stateLookup func returns the current
// container state given (uid, containerName).
func (g *Gambit) buildPodStatus(pod *corev1.Pod, stateLookup func(uid, containerName string) perigeos.MachineState) *corev1.PodStatus {
	uid := string(pod.UID)
	previousStatusesByContainer := make(map[string]corev1.ContainerStatus, len(pod.Status.ContainerStatuses))
	for _, status := range pod.Status.ContainerStatuses {
		previousStatusesByContainer[status.Name] = status
	}

	containerStatuses := make([]corev1.ContainerStatus, 0, len(pod.Spec.Containers))
	podPhase := corev1.PodRunning
	allReady := true

	podRestarts := g.store.RestartCounts(uid)

	policy := pod.Spec.RestartPolicy
	if policy == "" {
		policy = corev1.RestartPolicyAlways
	}

	for _, c := range pod.Spec.Containers {
		state := stateLookup(uid, c.Name)
		g.Logger.Debug("buildPodStatus state lookup", "container", c.Name, "state", state)

		restartCount := podRestarts[c.Name]

		cs := corev1.ContainerStatus{
			Name:         c.Name,
			Image:        c.Image,
			Ready:        false,
			RestartCount: restartCount,
		}

		// TODO Refactor
		switch state {
		case perigeos.StateRunning:
			cs.Ready = g.store.IsContainerReady(uid, c.Name)
			startedAt := g.store.ContainerStartedAt(uid, c.Name)
			cs.State = corev1.ContainerState{
				Running: &corev1.ContainerStateRunning{StartedAt: startedAt},
			}
		case perigeos.StateCreating:
			podPhase = corev1.PodPending
			cs.State = corev1.ContainerState{
				Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"},
			}
		case perigeos.StateUnknown:
			// Only keep the last known Running status if the pod is still in the
			// process of being created (Pending). If the pod is already Running, a
			// transition to Unknown should be treated as Not Ready.
			if podPhase == corev1.PodPending {
				if previous, ok := previousStatusesByContainer[c.Name]; ok && previous.State.Running != nil {
					cs.Ready = previous.Ready
					cs.State = previous.State
					break
				}
			}

			// For all other cases, if we don't know the state, it's not ready.
			cs.Ready = false
			cs.State = corev1.ContainerState{
				Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"},
			}
			// Don't force podPhase = Pending here if the pod is already Running
		case perigeos.StateFailed:
			if policy == corev1.RestartPolicyAlways || policy == corev1.RestartPolicyOnFailure {
				podPhase = corev1.PodRunning
				cs.State = corev1.ContainerState{
					Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"},
				}
			} else {
				// Don't set podPhase to Failed here — terminal phase is
				// handled by checkPod/SetPhase.
				startedAt := g.store.ContainerStartedAt(uid, c.Name)
				cs.State = corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{
						ExitCode:   1,
						Reason:     "Error",
						StartedAt:  startedAt,
						FinishedAt: metav1.Now(),
					},
				}
			}
		case perigeos.StateExited:
			if policy == corev1.RestartPolicyAlways {
				podPhase = corev1.PodRunning
				cs.State = corev1.ContainerState{
					Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"},
				}
			} else {
				// Don't set podPhase to Succeeded here — the terminal
				// phase transition is handled by checkPod/SetPhase.
				// Setting it per-container would override CrashLoopBackOff
				// from other containers in multi-container pods.
				startedAt := g.store.ContainerStartedAt(uid, c.Name)
				cs.State = corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{
						ExitCode:   0,
						Reason:     "Completed",
						StartedAt:  startedAt,
						FinishedAt: metav1.Now(),
					},
				}
			}
		}

		if !cs.Ready {
			allReady = false
		}
		containerStatuses = append(containerStatuses, cs)
	}

	readyCondition := corev1.ConditionFalse
	if allReady {
		readyCondition = corev1.ConditionTrue
	}

	ip := g.store.PodIP(uid)

	return &corev1.PodStatus{
		Phase:     podPhase,
		HostIP:    resolveNodeIP(g.Config),
		PodIP:     ip,
		StartTime: pod.Status.StartTime,
		Conditions: []corev1.PodCondition{{
			Type:   corev1.PodReady,
			Status: readyCondition,
		}},
		ContainerStatuses: containerStatuses,
	}
}

func (g *Gambit) GetPods(_ context.Context) ([]*corev1.Pod, error) {
	g.Logger.Debug("GetPods", "pawn", g.Config.Name)
	return g.store.GetPods(), nil
}

// PodSnapshot holds pod info captured under a single lock acquisition.
type PodSnapshot struct {
	Name       string
	Namespace  string
	UID        string
	IP         string
	Phase      corev1.PodPhase
	Containers int
}

// SnapshotPods returns a lightweight snapshot of all pods in a single lock.
// Designed for the control API to avoid per-pod lock acquisitions.
// Returns nil if the lock cannot be acquired immediately (heavy write churn).
func (g *Gambit) SnapshotPods() []PodSnapshot {
	return g.store.SnapshotPods()
}

// ─── Logs & Exec ─────────────────────────────────────────────────────────────

var _ api.ContainerLogsHandlerFunc = (*Gambit)(nil).GetContainerLogs
