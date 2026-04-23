package types

import (
        "time"

        perigeos "github.com/malformed-c/periapsis/internal/runtime"
        corev1 "k8s.io/api/core/v1"
        metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Fact is a happened event. It is a read-only snapshot of a state change.
// Facts are immutable after creation - no one mutates a Fact after it's
// emitted. This allows safe sharing across goroutines without copying.
//
// Fact is a sealed interface: only types defined in this package can
// implement it, preventing external packages from creating arbitrary facts.
type Fact interface {
        isFact()
        // UID returns the pod UID this fact belongs to.
        UID() string
}

// baseFact is embedded in every concrete Fact type to provide the UID()
// method. The uid field is unexported so it must be set via the embed -
// callers construct facts with baseFact{uid} as the first field.
type baseFact struct {
        uid string
}

func (b baseFact) UID() string { return b.uid }

// --- Systemd Facts ---

// UnitFact is emitted when a systemd unit changes substate.
// Source: D-Bus PropertiesChanged signal (reactive path).
type UnitFact struct {
        baseFact
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
        baseFact
        Container string
        State     perigeos.MachineState // Running, Creating, Failed, Exited
        ExitCode  int32
}

func (ContainerStateFact) isFact() {}

// ExitFact is emitted when a container process exits with a known result.
type ExitFact struct {
        baseFact
        Container   string
        ExitCode    int32
        Reason      string
        StartedAt   string // RFC3339
        FinishedAt  string // RFC3339
        WillRestart bool
}

func (ExitFact) isFact() {}

// --- Probe Facts ---

// ProbeFact is emitted when a probe (readiness, liveness, startup) completes.
// Ready is the evaluated readiness after threshold logic, set by the probe
// runner so the state machine doesn't need to duplicate threshold evaluation.
type ProbeFact struct {
        baseFact
        Container        string
        ProbeType        string // "readiness", "liveness", or "startup"
        Success          bool
        Ready            bool   // evaluated readiness after threshold logic
        StartupPassed    bool   // for startup probes: has the startup probe passed?
        SuccessThreshold int32
        FailureThreshold int32

        // Probe timing counts (set by ProbeScheduler, stored by foci).
        // These carry the current fail/success counts from ProbeScheduler
        // back into the state machine so ProbeScheduler can read them on
        // the next cycle without querying PodStore.
        StartupFailCount  int32
        LiveFailCount     int32
        ReadyFailCount    int32
        ReadySuccessCount int32
}

func (ProbeFact) isFact() {}

// --- Spec Facts ---

// SpecFact is emitted when a pod spec changes from Kubernetes.
type SpecFact struct {
        baseFact
        Namespace string
        PodName   string
        Pod       *corev1.Pod // the new pod spec
}

func (SpecFact) isFact() {}

// --- Lifecycle Facts ---

// PodAdmitFact is emitted when a new pod is admitted and should start
// tracking in the state machine.
type PodAdmitFact struct {
        baseFact
        Namespace string
        Name      string
        PodIP     string
        Pod       *corev1.Pod
}

func (PodAdmitFact) isFact() {}

// PodEvictFact is emitted when a pod should be removed from the state machine.
type PodEvictFact struct {
        baseFact
}

func (PodEvictFact) isFact() {}

// MarkRunningFact is emitted when CreatePod has started a container and
// wants the state machine to know it was running, even if the D-Bus
// "running" event arrives after the unit exits (fast-exit containers).
type MarkRunningFact struct {
        baseFact
        ContainerName string
}

func (MarkRunningFact) isFact() {}

// --- Backoff Reset ---

// BackoffResetFact is emitted when a container has been running stably
// long enough that its CrashLoopBackOff duration should be reset.
type BackoffResetFact struct {
        baseFact
        ContainerName string
}

func (BackoffResetFact) isFact() {}

// --- Pod Lifecycle Facts (Phase 2) ---
// These Facts drive PodStore updates through Syzygy Effects,
// making PodStore a thin projection updated only via Effects.

// PodRegisterFact is emitted when a pod is first seen (creation path).
// It causes Reduce to create a new PodState and emit RegisterPod +
// InitRestartState + PersistPodState effects.
type PodRegisterFact struct {
        baseFact
        Namespace string
        Name      string
        PodIP     string
        Pod       *corev1.Pod
}

func (PodRegisterFact) isFact() {}

// PodPromoteFact is emitted when a pod transitions to Running.
// It causes Reduce to update PodState.PodIP and emit PromotePodRunning +
// PersistPodState effects.
type PodPromoteFact struct {
        baseFact
        Namespace string
        Name      string
        PodIP     string
        Pod       *corev1.Pod // the full pod with resolved env/ports
}

func (PodPromoteFact) isFact() {}

// PodDeleteFact is emitted when a pod should be deleted.
// It causes Reduce to emit MarkPodDeleting + UnregisterPod + ResetUnit effects.
type PodDeleteFact struct {
        baseFact
        Namespace string
        Name      string
}

func (PodDeleteFact) isFact() {}

// PodMarkDeletingFact is emitted before deletion starts.
// It causes Reduce to emit MarkPodDeleting effect.
type PodMarkDeletingFact struct {
        baseFact
}

func (PodMarkDeletingFact) isFact() {}

// --- Fact constructors ---
//
// These constructors are the canonical way to create Fact values.
// They set the baseFact.uid field, which is unexported and cannot be
// set by external packages directly via struct literal.

func NewUnitFact(uid, unitName, subState string, exitCode int32) *UnitFact {
        return &UnitFact{baseFact: baseFact{uid: uid}, UnitName: unitName, SubState: subState, ExitCode: exitCode}
}

func NewContainerStateFact(uid, container string, state perigeos.MachineState, exitCode int32) *ContainerStateFact {
        return &ContainerStateFact{baseFact: baseFact{uid: uid}, Container: container, State: state, ExitCode: exitCode}
}

func NewExitFact(uid, container string, exitCode int32, reason string, willRestart bool) *ExitFact {
        return &ExitFact{baseFact: baseFact{uid: uid}, Container: container, ExitCode: exitCode, Reason: reason, WillRestart: willRestart}
}

func NewProbeFact(uid, container, probeType string, success, ready, startupPassed bool, successThreshold, failureThreshold int32) *ProbeFact {
        return &ProbeFact{
                baseFact:         baseFact{uid: uid},
                Container:        container,
                ProbeType:        probeType,
                Success:          success,
                Ready:            ready,
                StartupPassed:    startupPassed,
                SuccessThreshold: successThreshold,
                FailureThreshold: failureThreshold,
        }
}

// NewProbeFactWithCounts creates a ProbeFact with probe timing counts.
// This is used by ProbeScheduler to send the current fail/success counts
// back to the state machine.
func NewProbeFactWithCounts(uid, container, probeType string, success, ready, startupPassed bool, successThreshold, failureThreshold int32, startupFailCount, liveFailCount, readyFailCount, readySuccessCount int32) *ProbeFact {
        return &ProbeFact{
                baseFact:          baseFact{uid: uid},
                Container:         container,
                ProbeType:         probeType,
                Success:           success,
                Ready:             ready,
                StartupPassed:     startupPassed,
                SuccessThreshold:  successThreshold,
                FailureThreshold:  failureThreshold,
                StartupFailCount:  startupFailCount,
                LiveFailCount:     liveFailCount,
                ReadyFailCount:    readyFailCount,
                ReadySuccessCount: readySuccessCount,
        }
}

func NewSpecFact(uid, namespace, podName string, pod *corev1.Pod) *SpecFact {
        return &SpecFact{baseFact: baseFact{uid: uid}, Namespace: namespace, PodName: podName, Pod: pod}
}

func NewPodAdmitFact(uid, namespace, name, podIP string, pod *corev1.Pod) *PodAdmitFact {
        return &PodAdmitFact{baseFact: baseFact{uid: uid}, Namespace: namespace, Name: name, PodIP: podIP, Pod: pod}
}

func NewPodEvictFact(uid string) *PodEvictFact {
        return &PodEvictFact{baseFact: baseFact{uid: uid}}
}

func NewMarkRunningFact(uid, containerName string) *MarkRunningFact {
        return &MarkRunningFact{baseFact: baseFact{uid: uid}, ContainerName: containerName}
}

func NewBackoffResetFact(uid, containerName string) *BackoffResetFact {
        return &BackoffResetFact{baseFact: baseFact{uid: uid}, ContainerName: containerName}
}

func NewPodRegisterFact(uid, namespace, name, podIP string, pod *corev1.Pod) *PodRegisterFact {
        return &PodRegisterFact{baseFact: baseFact{uid: uid}, Namespace: namespace, Name: name, PodIP: podIP, Pod: pod}
}

func NewPodPromoteFact(uid, namespace, name, podIP string, pod *corev1.Pod) *PodPromoteFact {
        return &PodPromoteFact{baseFact: baseFact{uid: uid}, Namespace: namespace, Name: name, PodIP: podIP, Pod: pod}
}

func NewPodDeleteFact(uid, namespace, name string) *PodDeleteFact {
        return &PodDeleteFact{baseFact: baseFact{uid: uid}, Namespace: namespace, Name: name}
}

func NewPodMarkDeletingFact(uid string) *PodMarkDeletingFact {
        return &PodMarkDeletingFact{baseFact: baseFact{uid: uid}}
}

// --- Effect ---

// Effect is a side-effect emitted by the Reduce function.
// Effects are the only way the state machine interacts with the outside world.
// The Reduce function is pure - it never performs I/O, never mutates shared
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
        Phase             corev1.PodPhase
        Conditions        []PodCondition
        ContainerStatuses []ContainerStatusPayload
        HostIP            string
        PodIP             string
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

// ContainerInitPayload carries the per-container data needed to initialize
// restart and probe tracking. It is a flat value type - no pointer to
// *corev1.Pod, no DeepCopy required.
type ContainerInitPayload struct {
        Name              string
        HasReadinessProbe bool
}

// InitRestartState instructs the executor to initialize restart/probe
// tracking in PodStore for a newly admitted pod.
// All fields are value types - no *corev1.Pod pointer on the Effect channel.
type InitRestartState struct {
        UID        string
        Namespace  string
        Name       string
        Containers []ContainerInitPayload
}

func (InitRestartState) isEffect() {}

// --- PodStore Projection Effects (Phase 2) ---
// These Effects update PodStore as a thin projection, driven by
// Facts through the foci.Reduce → Syzygy.dispatchEffect pipeline.

// RegisterPod instructs the executor to register a pod in PodStore.
type RegisterPod struct {
        UID       string
        Namespace string
        Name      string
        PodIP     string
        Pod       *corev1.Pod
        InFlight  bool // true if pod is being created
}

func (RegisterPod) isEffect() {}

// PromotePodRunning instructs the executor to promote a pod to Running in PodStore.
type PromotePodRunning struct {
        UID   string
        Pod   *corev1.Pod
        PodIP string
}

func (PromotePodRunning) isEffect() {}

// MarkPodDeleting instructs the executor to mark a pod as deleting in PodStore.
type MarkPodDeleting struct {
        UID string
}

func (MarkPodDeleting) isEffect() {}

// UnregisterPod instructs the executor to remove a pod from PodStore.
type UnregisterPod struct {
        UID       string
        Namespace string
        Name      string
}

func (UnregisterPod) isEffect() {}
