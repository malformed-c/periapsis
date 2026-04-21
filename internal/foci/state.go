package foci

// Flat value types for the per-pod state machine.
//
// Design principles:
//   - No pointers to shared mutable state - every field is a value type
//   - No DeepCopy anywhere in the hot path - PodState copies are cheap
//   - PodSpec is converted once at admission from *corev1.Pod, then the
//     entire state machine operates on value types
//   - The conversion cost at pod admission is worth it: you pay once,
//     then Reduce is a pure function over small value types that copies
//     cheaply on every call
//
// Memory impact at 3000 pods:
//   - PodState: ~300-500 bytes per pod (depending on container count)
//   - Total: ~1.5MB for 3000 pods
//   - No *corev1.Pod stored per Focus (saves ~30MB vs old architecture)

import (
	"time"

	corev1 "k8s.io/api/core/v1"
)

// --- Container Phase ---

// ContainerPhase represents the lifecycle phase of a container.
// This is a flat enum - no pointer fields, no heap allocation.
type ContainerPhase uint8

const (
	PhaseCreating         ContainerPhase = iota // Waiting{Reason: "ContainerCreating"}
	PhaseRunning                                // Running
	PhaseTerminated                             // Terminated (exit 0 = Completed, non-zero = Error)
	PhaseCrashLoopBackOff                       // Waiting{Reason: "CrashLoopBackOff"}
)

// String returns a human-readable name for the phase.
func (p ContainerPhase) String() string {
	switch p {
	case PhaseCreating:
		return "ContainerCreating"
	case PhaseRunning:
		return "Running"
	case PhaseTerminated:
		return "Terminated"
	case PhaseCrashLoopBackOff:
		return "CrashLoopBackOff"
	default:
		return "Unknown"
	}
}

// --- Container Spec (flat extraction from corev1.Container) ---

// ProbeSpec is the flat probe configuration extracted from corev1.Probe.
// Only the fields needed by the state machine are kept.
type ProbeSpec struct {
	InitialDelaySeconds int32
	PeriodSeconds       int32
	TimeoutSeconds      int32
	SuccessThreshold    int32
	FailureThreshold    int32
}

// ContainerSpec is the per-container spec extracted from corev1.Container.
// Only the fields needed by the state machine are kept.
type ContainerSpec struct {
	Name              string
	Image             string
	HasReadinessProbe bool
	HasLivenessProbe  bool
	HasStartupProbe   bool

	// Probe thresholds - needed by Reduce for startup gating logic.
	ReadinessProbe ProbeSpec
	LivenessProbe  ProbeSpec
	StartupProbe   ProbeSpec
}

// PodSpec is the flat pod spec needed by the state machine.
// Converted once from *corev1.Pod at pod admission.
type PodSpec struct {
	RestartPolicy corev1.RestartPolicy
	Containers    []ContainerSpec
}

// --- Container State ---

// ContainerState is the per-container state in the pod state machine.
// All value types - no pointers, no DeepCopy needed.
type ContainerState struct {
	Name         string
	Image        string
	Phase        ContainerPhase
	Ready        bool
	RestartCount int32

	// Lifecycle tracking
	SeenRunning bool
	Restarting  bool
	Backoff     time.Duration
	LastStarted time.Time

	// Exit/termination details (only meaningful when Phase == PhaseTerminated)
	ExitCode   int32
	ExitReason string

	// Probe tracking (evaluated results only - timing lives in PodStore)
	StartupPassed bool
}

// --- Pod State ---

// PodState is the per-pod state in the Syzygy's state map.
// This is the single source of truth for one pod's state machine.
//
// PodState is a value type - copying it is cheap (~300-500 bytes).
// The Reduce function takes PodState by value and returns a new PodState.
type PodState struct {
	UID       string
	Namespace string
	Name      string
	PodIP     string
	Spec      PodSpec
	Phase     corev1.PodPhase

	// Containers is indexed the same as Spec.Containers.
	// This is a slice, not a map, for cache-friendly iteration and
	// deterministic ordering. Lookups by name use a linear scan
	// (typically 1-3 containers per pod).
	Containers []ContainerState
}

// FindContainer returns the index of the container with the given name,
// or -1 if not found.
func (s PodState) FindContainer(name string) int {
	for i := range s.Containers {
		if s.Containers[i].Name == name {
			return i
		}
	}
	return -1
}

// FindSpec returns the index of the container spec with the given name,
// or -1 if not found.
func (s PodState) FindSpec(name string) int {
	for i := range s.Spec.Containers {
		if s.Spec.Containers[i].Name == name {
			return i
		}
	}
	return -1
}

// --- Constants ---

const (
	RestartBackoffInit  = 10 * time.Second
	MaxBackoff          = 5 * time.Minute
	RestartBackoffReset = 10 * time.Minute
)

// --- Conversion ---

// NewPodSpec extracts a flat PodSpec from a *corev1.Pod.
// This is the one-time conversion cost at pod admission.
func NewPodSpec(pod *corev1.Pod) PodSpec {
	spec := PodSpec{
		RestartPolicy: pod.Spec.RestartPolicy,
		Containers:    make([]ContainerSpec, 0, len(pod.Spec.Containers)),
	}

	for _, c := range pod.Spec.Containers {
		cs := ContainerSpec{
			Name:  c.Name,
			Image: c.Image,
		}
		if c.ReadinessProbe != nil {
			cs.HasReadinessProbe = true
			cs.ReadinessProbe = ProbeSpec{
				InitialDelaySeconds: c.ReadinessProbe.InitialDelaySeconds,
				PeriodSeconds:       c.ReadinessProbe.PeriodSeconds,
				TimeoutSeconds:      c.ReadinessProbe.TimeoutSeconds,
				SuccessThreshold:    c.ReadinessProbe.SuccessThreshold,
				FailureThreshold:    c.ReadinessProbe.FailureThreshold,
			}
		}
		if c.LivenessProbe != nil {
			cs.HasLivenessProbe = true
			cs.LivenessProbe = ProbeSpec{
				InitialDelaySeconds: c.LivenessProbe.InitialDelaySeconds,
				PeriodSeconds:       c.LivenessProbe.PeriodSeconds,
				TimeoutSeconds:      c.LivenessProbe.TimeoutSeconds,
				SuccessThreshold:    c.LivenessProbe.SuccessThreshold,
				FailureThreshold:    c.LivenessProbe.FailureThreshold,
			}
		}
		if c.StartupProbe != nil {
			cs.HasStartupProbe = true
			cs.StartupProbe = ProbeSpec{
				InitialDelaySeconds: c.StartupProbe.InitialDelaySeconds,
				PeriodSeconds:       c.StartupProbe.PeriodSeconds,
				TimeoutSeconds:      c.StartupProbe.TimeoutSeconds,
				SuccessThreshold:    c.StartupProbe.SuccessThreshold,
				FailureThreshold:    c.StartupProbe.FailureThreshold,
			}
		}
		spec.Containers = append(spec.Containers, cs)
	}

	return spec
}

// NewPodState creates a PodState from a newly admitted pod.
// All containers start in PhaseCreating. Readiness defaults to true
// only if the container has no readiness probe; containers with probes
// start unready and must earn readiness via ProbeFacts.
func NewPodState(uid, namespace, name, podIP string, pod *corev1.Pod) PodState {
	spec := NewPodSpec(pod)

	containers := make([]ContainerState, 0, len(spec.Containers))
	for _, cs := range spec.Containers {
		containers = append(containers, ContainerState{
			Name:        cs.Name,
			Image:       cs.Image,
			Phase:       PhaseCreating,
			Ready:       !cs.HasReadinessProbe, // no probe => immediately ready
			Backoff:     RestartBackoffInit,
			LastStarted: time.Now(),
		})
	}

	return PodState{
		UID:        uid,
		Namespace:  namespace,
		Name:       name,
		PodIP:      podIP,
		Spec:       spec,
		Phase:      corev1.PodPending,
		Containers: containers,
	}
}
