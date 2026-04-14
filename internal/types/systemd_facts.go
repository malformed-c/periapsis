package types

import corev1 "k8s.io/api/core/v1"

// UnitFact is emitted when a systemd unit's substate changes
// (e.g., "running" → "failed", "stop-sigterm").
type UnitFact struct {
	UID      string
	UnitName string
	SubState string // systemd substate: "running", "failed", "dead", "stop-sigterm", etc.
}

func (UnitFact) isFact()

// ExitFact is emitted when a container process exits.
type ExitFact struct {
	UID        string
	Container  string
	ExitCode   int32
	Reason     string
	StartedAt  string // RFC3339
	FinishedAt string // RFC3339
}

func (ExitFact) isFact()

// ContainerFact is emitted when a container's k8s-visible state transitions.
// This is the fact that Syzygy uses to update container statuses and
// decide whether to push a pod status update to k8s via Horizon.
type ContainerFact struct {
	UID       string
	Container string

	// The new container state as seen by k8s.
	State corev1.ContainerState

	// Whether the container is ready (probe-passing).
	Ready bool

	// The pod phase implied by this container transition.
	// Syzygy aggregates container phases to compute the pod phase.
	ImpliedPodPhase corev1.PodPhase
}

func (ContainerFact) isFact()

// PodStatusFact is emitted when a full pod status should be written to k8s.
// Horizon consumes this to perform the actual API server write.
type PodStatusFact struct {
	UID    string
	Status corev1.PodStatus
}

func (PodStatusFact) isFact()
