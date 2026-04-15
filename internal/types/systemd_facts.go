package types

import (
	"time"

	corev1 "k8s.io/api/core/v1"
)

// UnitFact is emitted when a systemd unit's substate changes.
// It covers the full lifecycle of a container service: start, run, exit.
// The caller parses the container name once and sets ExitCode/FinishedAt
// when the unit exits - Focus handles it all in one pass
type UnitFact struct {
	UID       string
	UnitName  string
	Container string // parsed from UnitName by the caller
	SubState  string // systemd substate: "running", "failed", "dead", "stop-sigterm", etc.

	StartedAt time.Time

	// Exit fields - set when the unit process exited.
	ExitCode   int       // nil if the unit hasn't exited
	FinishedAt time.Time // set on exit
}

func (UnitFact) HKT1(FactKind)
func (UnitFact) HKT2(UnitFact)

// ContainerFact is emitted when a container's k8s state transitions.
// Horizon maps ContainerState -> corev1.ContainerState.
type ContainerFact struct {
	UID       string
	Container string

	// The new container state (flat)
	State ContainerState

	// Whether the container is ready (probe-passing)
	Ready bool

	// The pod phase implied by this container transition.
	// Focus aggregates container phases to compute the pod phase
	ImpliedPodPhase PodPhase
}

func (ContainerFact) HKT1(FactKind)
func (ContainerFact) HKT2(ContainerFact)

// PodStatusFact is emitted when a full pod status should be written to k8s.
// This is the legacy bypass path - used for lifecycle-initiated status
// pushes that go directly to Horizon without passing through a Focus.
// Horizon handles the actual k8s API write.
//
// This fact carries a corev1.PodStatus because it bypasses Focus
// (the k8s-free state machine) and goes straight to Horizon (the k8s layer).
type PodStatusFact struct {
	UID    string
	Status corev1.PodStatus
}

func (PodStatusFact) HTK1(FactKind)
func (PodStatusFact) HTK2(PodStatusFact)
