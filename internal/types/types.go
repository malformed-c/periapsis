package types

import (
	"time"

	perigeos "github.com/malformed-c/periapsis/internal/runtime"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Fact is a happened event. It is a read-only snapshot of a state change.
// Facts are immutable after creation — no one mutates a Fact after it's
// emitted. This allows safe sharing across goroutines without copying.
//
// Fact is a sealed interface: only types defined in this package can
// implement it, preventing external packages from creating arbitrary facts.
type Fact interface {
	isFact()
}

// ─── Systemd Facts ──────────────────────────────────────────────────────

// UnitFact is emitted when a systemd unit changes substate.
// Source: D-Bus PropertiesChanged signal (reactive path).
type UnitFact struct {
	UID       string
	UnitName  string
	SubState  string // systemd substate: "running", "failed", "start-pre", etc.
	ExitCode  int32  // only set for "failed" substate
	StartedAt metav1.Time
}

func (UnitFact) isFact() {}

// ContainerStateFact is emitted by the BatchWatcher poll when it detects
// a container's MachineState from ListManagedMachines.
// Source: ticker poll (consistency path).
type ContainerStateFact struct {
	UID       string
	Container string
	State     perigeos.MachineState // Running, Creating, Failed, Exited
	ExitCode  int32
}

func (ContainerStateFact) isFact() {}

// ExitFact is emitted when a container process exits with a known result.
type ExitFact struct {
	UID        string
	Container  string
	ExitCode   int32
	Reason     string
	StartedAt  string // RFC3339
	FinishedAt string // RFC3339
	WillRestart bool
}

func (ExitFact) isFact() {}

// ─── Probe Facts ────────────────────────────────────────────────────────

// ProbeFact is emitted when a probe (readiness, liveness, startup) completes.
// Ready is the evaluated readiness after threshold logic, set by the probe
// runner so the state machine doesn't need to duplicate threshold evaluation.
type ProbeFact struct {
	UID              string
	Container        string
	ProbeType        string // "readiness", "liveness", or "startup"
	Success          bool
	Ready            bool   // evaluated readiness after threshold logic
	StartupPassed    bool   // for startup probes: has the startup probe passed?
	SuccessThreshold int32
	FailureThreshold int32
}

func (ProbeFact) isFact() {}

// ─── Spec Facts ─────────────────────────────────────────────────────────

// SpecFact is emitted when a pod spec changes from Kubernetes.
type SpecFact struct {
	UID       string
	Namespace string
	PodName   string
	Pod       *corev1.Pod // the new pod spec
}

func (SpecFact) isFact() {}

// ─── Lifecycle Facts ────────────────────────────────────────────────────

// PodAdmitFact is emitted when a new pod is admitted and should start
// tracking in the state machine.
type PodAdmitFact struct {
	UID       string
	Namespace string
	Name      string
	PodIP     string
	Pod       *corev1.Pod
}

func (PodAdmitFact) isFact() {}

// PodEvictFact is emitted when a pod should be removed from the state machine.
type PodEvictFact struct {
	UID string
}

func (PodEvictFact) isFact() {}

// MarkRunningFact is emitted when CreatePod has started a container and
// wants the state machine to know it was running, even if the D-Bus
// "running" event arrives after the unit exits (fast-exit containers).
type MarkRunningFact struct {
	UID           string
	ContainerName string
}

func (MarkRunningFact) isFact() {}

// ─── Backoff Reset ──────────────────────────────────────────────────────

// BackoffResetFact is emitted when a container has been running stably
// long enough that its CrashLoopBackOff duration should be reset.
type BackoffResetFact struct {
	UID           string
	ContainerName string
}

func (BackoffResetFact) isFact() {}

// ─── Effect ─────────────────────────────────────────────────────────────

// Effect is a side-effect emitted by the Reduce function.
// Effects are the only way the state machine interacts with the outside world.
// The Reduce function is pure — it never performs I/O, never mutates shared
// state, and never calls external APIs. All side effects are captured as
// Effect values and executed by the Syzygy event loop.
type Effect interface {
	isEffect()
}

// UpdateStatus instructs the executor to write the computed PodStatus
// to the Kubernetes API via Horizon.
type UpdateStatus struct {
	UID       string
	Namespace string
	Name      string
	Status    PodStatusPayload
}

func (UpdateStatus) isEffect() {}

// PodStatusPayload carries the computed pod status fields as flat value types.
type PodStatusPayload struct {
	Phase              corev1.PodPhase
	Conditions         []PodCondition
	ContainerStatuses  []ContainerStatusPayload
	HostIP             string
	PodIP              string
}

// PodCondition is a flat value-type mirror of corev1.PodCondition.
type PodCondition struct {
	Type   corev1.PodConditionType
	Status corev1.ConditionStatus
}

// ContainerStatusPayload is the per-container status output.
type ContainerStatusPayload struct {
	Name         string
	Image        string
	Ready        bool
	RestartCount int32
	State        ContainerStatePayload
}

// ContainerStatePayload mirrors corev1.ContainerState as a flat tagged union.
type ContainerStatePayload struct {
	Waiting    *WaitingPayload
	Running    *RunningPayload
	Terminated *TerminatedPayload
}

type WaitingPayload struct {
	Reason string
}

type RunningPayload struct {
	StartedAt metav1.Time
}

type TerminatedPayload struct {
	ExitCode   int32
	Reason     string
	StartedAt  metav1.Time
	FinishedAt metav1.Time
}

// RestartContainer instructs the executor to restart a crashed container.
type RestartContainer struct {
	UID           string
	Namespace     string
	PodName       string
	ContainerName string
	RestartCount  int32
	Backoff       time.Duration
}

func (RestartContainer) isEffect() {}

// SetPodPhase instructs the executor to update the PodStore's phase map.
type SetPodPhase struct {
	UID   string
	Phase corev1.PodPhase
}

func (SetPodPhase) isEffect() {}

// ResetUnit instructs the executor to clean up a dead/failed systemd unit.
type ResetUnit struct {
	UID           string
	ContainerName string
}

func (ResetUnit) isEffect() {}

// RecordEvent instructs the executor to emit a Kubernetes event.
type RecordEvent struct {
	UID       string
	EventType string // corev1.EventTypeNormal or EventTypeWarning
	Reason    string
	Message   string
}

func (RecordEvent) isEffect() {}

// PersistPodState instructs the executor to persist pod state to disk.
type PersistPodState struct {
	UID string
}

func (PersistPodState) isEffect() {}

// InitRestartState instructs the executor to initialize restart/probe
// tracking in PodStore for a newly created pod.
type InitRestartState struct {
	UID string
	Pod *corev1.Pod
}

func (InitRestartState) isEffect() {}
