package types

import "time"

// ContainerStateKind is the kind of container state
// This is the flat, k8s-free representation used by Foci
type ContainerStateKind int

const (
	StateWaiting    ContainerStateKind = iota // container is waiting to start
	StateRunning                              // container is running
	StateTerminated                           // container has exited
)

// ContainerState is a flat container state representation
// No k8s types - Horizon maps this to corev1.ContainerState
type ContainerState struct {
	Kind ContainerStateKind

	// Waiting state fields
	Reason string // e.g. "ContainerCreating", "CrashLoopBackOff"

	// Running state fields
	StartedAt time.Time

	// Terminated state fields
	ExitCode   int32
	FinishedAt time.Time
}

// WaitingState returns a ContainerState in waiting state
func WaitingState(reason string) ContainerState {
	return ContainerState{Kind: StateWaiting, Reason: reason}
}

// RunningState returns a ContainerState in running state
func RunningState(startedAt time.Time) ContainerState {
	return ContainerState{Kind: StateRunning, StartedAt: startedAt}
}

// TerminatedState returns a ContainerState in terminated state
func TerminatedState(exitCode int32, finishedAt time.Time) ContainerState {
	return ContainerState{Kind: StateTerminated, ExitCode: exitCode, FinishedAt: finishedAt}
}

// PodPhase is the flat pod phase enum
// No k8s types - Horizon maps this to corev1.PodPhase
type PodPhase int

const (
	PhasePending PodPhase = iota
	PhaseRunning
	PhaseSucceeded
	PhaseFailed
	PhaseUnknown
)
