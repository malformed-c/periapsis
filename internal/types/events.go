package types

import corev1 "k8s.io/api/core/v1"

type EventType uint8

const (
	TypeUnknown EventType = iota

	TypePod
	TypePodStatus

	TypeContainer
	TypeContainerState

	TypeSaga
	TypeSagaStep
)

type Event struct {
	Type EventType
	UID  string

	// Active based on Type PodStatus
	PodStatus corev1.PodStatus
	Reason    string

	// Active based on Type ContainerState
	Container string
	State     string
	Ready     bool

	// Active based on Type SagaStep
	SagaStep string
	Success  bool
}
