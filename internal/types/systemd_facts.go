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
// Uses flat ContainerState so Focus can consume this without k8s imports.
// Horizon maps ContainerState -> corev1.ContainerState.
type ContainerFact struct {
	UID       string
	Container string

	// The new container state (flat, k8s-free).
	State ContainerState

	// Whether the container is ready (probe-passing).
	Ready bool

	// The pod phase implied by this container transition.
	// Focus aggregates container phases to compute the pod phase.
	ImpliedPodPhase PodPhase
}

func (ContainerFact) isFact()

// PodStatusFact is emitted when a full pod status should be written to k8s.
// This is the legacy bypass path — used for lifecycle-initiated status
// pushes that go directly to Horizon without passing through a Focus.
// Horizon handles the actual k8s API write.
//
// This fact carries a corev1.PodStatus because it bypasses Focus
// (the k8s-free state machine) and goes straight to Horizon (the k8s layer).
type PodStatusFact struct {
	UID    string
	Status corev1.PodStatus
}

func (PodStatusFact) isFact()
