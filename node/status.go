// Copyright (C) 2025-2026 Malformed C. All rights reserved.
// SPDX-License-Identifier: BUSL-1.1

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

	// If the pod is in a terminal phase (set by BatchWatcher), return
	// the stored status directly. The systemd unit may already be cleaned
	// up (ResetUnit) so machineStates would be stale or absent.
	phase := g.store.PodPhase(uid)
	if phase == corev1.PodSucceeded || phase == corev1.PodFailed {
		statusCopy := targetPod.Status.DeepCopy()

		return statusCopy, nil
	}

	// If the pod was marked as failed during creation (e.g. CNI error,
	// image pull failure), return the stored failure status directly.
	// No machine was started, so querying systemd would give misleading
	// "Completed" / "Unknown" states.
	if targetPod.Status.Phase == corev1.PodFailed {
		statusCopy := targetPod.Status.DeepCopy()

		return statusCopy, nil
	}

	// Use the BatchWatcher's cached machine states in the store. If the
	// watcher hasn't observed any containers yet (e.g. very first poll hasn't
	// fired), the store returns StateUnknown, which renders as ContainerCreating.
	if g.batchWatcher == nil {
		// No BatchWatcher (test or early startup) - do a live D-Bus query and
		// write results to the store so buildPodStatus can read them.
		allContainers := append(targetPod.Spec.InitContainers, targetPod.Spec.Containers...)
		for _, c := range allContainers {
			state, err := g.Runtime.MachineStatus(ctx, uid, c.Name)
			if err != nil {
				state = perigeos.StateUnknown
			}

			g.store.SetContainerMachineState(uid, c.Name, state, 0)
		}
	}

	return g.buildPodStatus(targetPod), nil
}

// buildPodStatus constructs a PodStatus from the pod spec and the container
// machine states stored in PodStore. BatchWatcher writes the state for each
// container on every poll/event cycle; this function is the single consumer.
func (g *Gambit) buildPodStatus(pod *corev1.Pod) *corev1.PodStatus {
	uid := string(pod.UID)
	storePhase := g.store.PodPhase(uid)

	previousStatusesByContainer := make(map[string]corev1.ContainerStatus)
	for _, status := range pod.Status.InitContainerStatuses {
		previousStatusesByContainer[status.Name] = status
	}
	for _, status := range pod.Status.ContainerStatuses {
		previousStatusesByContainer[status.Name] = status
	}

	podRestarts := g.store.RestartCounts(uid)

	policy := pod.Spec.RestartPolicy
	if policy == "" {
		policy = corev1.RestartPolicyAlways
	}

	// 1. Process Init Containers
	initStatuses := make([]corev1.ContainerStatus, 0, len(pod.Spec.InitContainers))
	initAllSucceeded := true

	for _, ic := range pod.Spec.InitContainers {
		cs, finished := g.buildContainerStatus(uid, &ic, podRestarts[ic.Name], policy, true, previousStatusesByContainer[ic.Name], storePhase)
		initStatuses = append(initStatuses, cs)

		if !finished {
			initAllSucceeded = false

			break
		}

		if cs.State.Terminated != nil && cs.State.Terminated.ExitCode != 0 {
			initAllSucceeded = false

			break
		}
	}

	// 2. Process App Containers
	containerStatuses := make([]corev1.ContainerStatus, 0, len(pod.Spec.Containers))
	allReady := initAllSucceeded

	if initAllSucceeded {
		for _, c := range pod.Spec.Containers {
			cs, _ := g.buildContainerStatus(uid, &c, podRestarts[c.Name], policy, false, previousStatusesByContainer[c.Name], storePhase)
			containerStatuses = append(containerStatuses, cs)
			if !cs.Ready {
				allReady = false
			}
		}

	} else {
		// Init containers still running or failed.
		// Fill in "Waiting" status for remaining init containers and all app containers.
		for i := len(initStatuses); i < len(pod.Spec.InitContainers); i++ {
			c := pod.Spec.InitContainers[i]
			initStatuses = append(initStatuses, corev1.ContainerStatus{
				Name:  c.Name,
				Image: c.Image,
				State: corev1.ContainerState{
					Waiting: &corev1.ContainerStateWaiting{Reason: "PodInitializing"},
				},
			})
		}
		for _, c := range pod.Spec.Containers {
			containerStatuses = append(containerStatuses, corev1.ContainerStatus{
				Name:  c.Name,
				Image: c.Image,
				State: corev1.ContainerState{
					Waiting: &corev1.ContainerStateWaiting{Reason: "PodInitializing"},
				},
			})
		}

		allReady = false
	}

	readyCondition := corev1.ConditionFalse
	if allReady {
		readyCondition = corev1.ConditionTrue
	}

	ip := g.store.PodIP(uid)
	podIPs := []corev1.PodIP{}
	if ip != "" {
		podIPs = []corev1.PodIP{{IP: ip}}
	}

	return &corev1.PodStatus{
		Phase:     storePhase,
		HostIP:    resolveNodeIP(g.Config),
		PodIP:     ip,
		PodIPs:    podIPs,
		StartTime: pod.Status.StartTime,
		Conditions: []corev1.PodCondition{{
			Type:   corev1.PodReady,
			Status: readyCondition,
		}},
		InitContainerStatuses: initStatuses,
		ContainerStatuses:     containerStatuses,
	}

}

func (g *Gambit) buildContainerStatus(uid string, c *corev1.Container, restartCount int32, policy corev1.RestartPolicy, isInit bool, previous corev1.ContainerStatus, storePhase corev1.PodPhase) (corev1.ContainerStatus, bool) {
	state, exitCode := g.store.ContainerMachineState(uid, c.Name)
	cs := corev1.ContainerStatus{
		Name:         c.Name,
		Image:        c.Image,
		Ready:        false,
		RestartCount: restartCount,
	}

	finished := false

	switch state {
	case perigeos.StateRunning:
		cs.Ready = g.store.IsContainerReady(uid, c.Name)
		startedAt := g.store.ContainerStartedAt(uid, c.Name)
		cs.State = corev1.ContainerState{
			Running: &corev1.ContainerStateRunning{StartedAt: startedAt},
		}

	case perigeos.StateCreating:
		cs.State = corev1.ContainerState{
			Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"},
		}

	case perigeos.StateUnknown:
		// Only keep the last known Running status if the pod is still in the
		// process of being created (Pending). If the pod is already Running, a
		// transition to Unknown should be treated as Not Ready.
		if storePhase == corev1.PodPending && previous.State.Running != nil {
			cs.Ready = previous.Ready
			cs.State = previous.State

		} else if storePhase == corev1.PodPending && previous.State.Waiting != nil && previous.State.Waiting.Reason == "CrashLoopBackOff" {
			// Preserve CrashLoopBackOff during retries to avoid status flip-flops
			cs.Ready = false
			cs.State = previous.State

		} else {
			cs.Ready = false
			cs.State = corev1.ContainerState{
				Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"},
			}
		}

	case perigeos.StateFailed:
		if isInit || policy == corev1.RestartPolicyAlways || policy == corev1.RestartPolicyOnFailure {
			cs.State = corev1.ContainerState{
				Waiting: &corev1.ContainerStateWaiting{
					Reason:  "CrashLoopBackOff",
					Message: "Back-off restarting failed container",
				},
			}
		} else {
			startedAt := g.store.ContainerStartedAt(uid, c.Name)
			cs.State = corev1.ContainerState{
				Terminated: &corev1.ContainerStateTerminated{
					ExitCode:   exitCode,
					Reason:     "Error",
					StartedAt:  startedAt,
					FinishedAt: metav1.Now(),
				},
			}
			finished = true
		}

	case perigeos.StateExited:
		if exitCode == 0 {
			startedAt := g.store.ContainerStartedAt(uid, c.Name)
			cs.State = corev1.ContainerState{
				Terminated: &corev1.ContainerStateTerminated{
					ExitCode:   exitCode,
					Reason:     "Completed",
					StartedAt:  startedAt,
					FinishedAt: metav1.Now(),
				},
			}
			finished = true
		} else {
			if isInit || policy == corev1.RestartPolicyAlways || policy == corev1.RestartPolicyOnFailure {
				cs.State = corev1.ContainerState{
					Waiting: &corev1.ContainerStateWaiting{
						Reason:  "CrashLoopBackOff",
						Message: "Back-off restarting failed container",
					},
				}
			} else {
				startedAt := g.store.ContainerStartedAt(uid, c.Name)
				cs.State = corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{
						ExitCode:   exitCode,
						Reason:     "Error",
						StartedAt:  startedAt,
						FinishedAt: metav1.Now(),
					},
				}
				finished = true
			}
		}
	}

	return cs, finished
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

// --- Logs & Exec ---

var _ api.ContainerLogsHandlerFunc = (*Gambit)(nil).GetContainerLogs
