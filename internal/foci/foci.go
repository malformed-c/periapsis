package foci

// A Focus owns one pod's state machine.
//
// It receives Facts (immutable events) and computes a PodStatus.
// All mutations happen inside the Focus — the rest of the system
// only observes the computed status. This eliminates races because:
//
//   - Facts are read-only (sealed interface)
//   - Each Focus owns one pod's state (single writer)
//   - Fact processing is sequential within a Focus (chan delivery)
//   - The state machine is a pure function: (state, Fact) -> (state, actions)
//
// Focus is named after the orbital mechanics theme (point of convergence).
// It is the single place where container states, readiness, restart policy,
// and probe results converge into a coherent pod status.

import (
        "fmt"
        "log/slog"
        "strings"
        "sync"
        "time"

        "github.com/malformed-c/periapsis/internal/types"
        corev1 "k8s.io/api/core/v1"
        metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// containerView is the Focus's local view of a single container's state.
// This is never shared outside the Focus — it's the Focus's private state.
type containerView struct {
        // The k8s-visible container state.
        state corev1.ContainerState

        // Whether the container is ready (probe-passing or no probe defined).
        ready bool

        // Whether the container has been seen running at least once.
        // Prevents premature terminal decisions during systemd unit startup.
        seenRunning bool

        // Whether the container is currently being restarted by the Focus.
        restarting bool

        // Restart bookkeeping.
        restartCount int32
        backoff      time.Duration
        lastStarted  time.Time

        // Probe state.
        probeLastResult map[string]time.Time // "startup"/"liveness"/"readiness" -> last probed
        readyFailCount  int32
        readyPassCount  int32
}

// podView is the Focus's local view of a pod.
type podView struct {
        mu sync.Mutex

        uid   string
        pod   *corev1.Pod // spec (read-only after creation)
        podIP string

        containers map[string]*containerView // containerName -> view
}

// Focus is a per-pod state machine actor.
// It processes Facts and emits pod status updates to the HorizonWriter.
type Focus struct {
        view    podView
        inbox   chan *types.Fact
        horizon HorizonWriter

        logger *slog.Logger
        done   chan struct{}
}

// HorizonWriter is the interface for sending pod status to the k8s API.
// Implemented by horizon.Horizon (Send method) — kept as an interface
// so Focus doesn't depend on the horizon package directly.
type HorizonWriter interface {
        WritePodStatus(pod *corev1.Pod)
}

// FocusConfig holds the configuration for creating a Focus.
type FocusConfig struct {
        UID     string
        Pod     *corev1.Pod
        PodIP   string
        Horizon HorizonWriter
        Logger  *slog.Logger
}

// NewFocus creates a new Focus for a pod.
//
// All containers start in ContainerCreating state. Readiness defaults
// to true only if the container has no readiness probe; containers
// with probes start unready and must earn readiness via ProbeFacts.
func NewFocus(cfg FocusConfig) *Focus {
        containers := make(map[string]*containerView, len(cfg.Pod.Spec.Containers))
        for _, c := range cfg.Pod.Spec.Containers {
                containers[c.Name] = &containerView{
                        state: corev1.ContainerState{
                                Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"},
                        },
                        ready:       c.ReadinessProbe == nil, // no probe => immediately ready
                        backoff:     restartBackoffInit,
                        lastStarted: time.Now(),
                }
        }

        // Init containers follow the same pattern — start in Creating.
        for _, c := range cfg.Pod.Spec.InitContainers {
                containers[c.Name] = &containerView{
                        state: corev1.ContainerState{
                                Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"},
                        },
                        ready:       true, // init containers don't affect pod readiness
                        backoff:     restartBackoffInit,
                        lastStarted: time.Now(),
                }
        }

        logger := cfg.Logger
        if logger == nil {
                logger = slog.Default()
        }

        return &Focus{
                view: podView{
                        uid:        cfg.UID,
                        pod:        cfg.Pod,
                        podIP:      cfg.PodIP,
                        containers: containers,
                },
                inbox:   make(chan *types.Fact, 64),
                horizon: cfg.Horizon,
                logger:  logger.With("component", "focus", "pod", cfg.Pod.Name, "uid", cfg.UID),
                done:    make(chan struct{}),
        }
}

// Send enqueues a Fact for this Focus to process.
// Non-blocking — if the inbox is full, the fact is dropped (the anti-entropy
// loop will reconcile eventually).
func (f *Focus) Send(fact *types.Fact) bool {
        select {
        case f.inbox <- fact:
                return true
        default:
                return false
        }
}

// UID returns the pod UID this Focus manages.
func (f *Focus) UID() string {
        return f.view.uid
}

// Run processes Facts until the context is cancelled.
func (f *Focus) Run(ctx context.Context) {
        defer close(f.done)
        for {
                select {
                case fact, ok := <-f.inbox:
                        if !ok {
                                return
                        }
                        f.process(ctx, fact)
                case <-ctx.Done():
                        return
                }
        }
}

// Done returns a channel that's closed when the Focus has stopped.
func (f *Focus) Done() <-chan struct{} {
        return f.done
}

// process dispatches a Fact to the appropriate handler.
func (f *Focus) process(ctx context.Context, fact *types.Fact) {
        switch ff := (*fact).(type) {
        case *types.UnitFact:
                f.handleUnitFact(ff)
        case *types.ExitFact:
                f.handleExitFact(ff)
        case *types.ContainerFact:
                f.handleContainerFact(ff)
        case *types.ProbeFact:
                f.handleProbeFact(ff)
        case *types.SpecFact:
                f.handleSpecFact(ff)
        default:
                f.logger.Warn("unknown fact type", "type", fmt.Sprintf("%T", ff))
        }
}

// Snapshot returns the current computed PodStatus.
// This is safe to call from any goroutine.
func (f *Focus) Snapshot() *corev1.PodStatus {
        f.view.mu.Lock()
        defer f.view.mu.Unlock()
        return f.computeStatus()
}

// ─── Fact Handlers ─────────────────────────────────────────────────────

// handleUnitFact processes a systemd unit substate change.
// Maps the raw substate to a container state transition.
//
// The unit name format is: perigeos-<pawn>-pod-<uid>-<containerName>.service
// We extract uid and containerName using the pawnName that was provided
// when the Focus was created (stored in the pod's labels or passed via config).
//
// Since the Focus receives UnitFacts that were already routed by UID
// (via FocusRegistry), we only need to extract the container name here.
func (f *Focus) handleUnitFact(fact *types.UnitFact) {
        containerName := ParseContainerFromUnit(fact.UnitName)
        if containerName == "" {
                return
        }

        f.view.mu.Lock()
        defer f.view.mu.Unlock()

        cv, ok := f.view.containers[containerName]
        if !ok {
                return // not a container we track (could be init container already completed)
        }

        // Map substate to container state.
        switch fact.SubState {
        case "running":
                cv.seenRunning = true
                cv.restarting = false
                cv.state = corev1.ContainerState{
                        Running: &corev1.ContainerStateRunning{
                                StartedAt: metav1.Now(),
                        },
                }
        case "failed":
                cv.state = corev1.ContainerState{
                        Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"},
                }
        case "start-pre", "start", "start-post":
                cv.state = corev1.ContainerState{
                        Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"},
                }
        // "dead", "stop-sigterm", "stop-sigkill" — ignored (transient states).
        // The Focus learns about exits via ExitFact, not via transient substates.
        default:
                return
        }

        f.logger.Debug("unit state changed",
                "container", containerName, "substate", fact.SubState,
                "seenRunning", cv.seenRunning)

        f.pushStatusLocked()
}

// handleExitFact processes a container process exit.
// Applies restart policy and emits the appropriate state.
func (f *Focus) handleExitFact(fact *types.ExitFact) {
        f.view.mu.Lock()
        defer f.view.mu.Unlock()

        cv, ok := f.view.containers[fact.Container]
        if !ok {
                return
        }

        policy := f.view.pod.Spec.RestartPolicy
        if policy == "" {
                policy = corev1.RestartPolicyAlways
        }

        // If we never saw it running, defer the terminal decision.
        // The anti-entropy loop (or a subsequent UnitFact with "running")
        // will resolve this.
        if !cv.seenRunning {
                f.logger.Debug("exit received but container never seen running — deferring",
                        "container", fact.Container, "exitCode", fact.ExitCode)
                cv.state = corev1.ContainerState{
                        Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"},
                }
                return
        }

        shouldRestart := false
        switch policy {
        case corev1.RestartPolicyAlways:
                shouldRestart = true
        case corev1.RestartPolicyOnFailure:
                shouldRestart = fact.ExitCode != 0
        case corev1.RestartPolicyNever:
                shouldRestart = false
        }

        if shouldRestart {
                cv.restarting = true
                cv.restartCount++
                cv.backoff = min(cv.backoff*2, maxBackoff)
                cv.state = corev1.ContainerState{
                        Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"},
                }
                f.logger.Info("container exited, restarting",
                        "container", fact.Container, "exitCode", fact.ExitCode,
                        "restartCount", cv.restartCount, "backoff", cv.backoff)
                // TODO: launch restart goroutine, emit UnitFact when complete
        } else {
                finishedAt := metav1.Now()
                if fact.FinishedAt != "" {
                        if t, err := time.Parse(time.RFC3339, fact.FinishedAt); err == nil {
                                finishedAt = metav1.NewTime(t)
                        }
                }
                cv.state = corev1.ContainerState{
                        Terminated: &corev1.ContainerStateTerminated{
                                ExitCode:   fact.ExitCode,
                                Reason:     fact.Reason,
                                FinishedAt: finishedAt,
                        },
                }
                f.logger.Info("container exited (terminal)",
                        "container", fact.Container, "exitCode", fact.ExitCode)
        }

        f.pushStatusLocked()
}

// handleContainerFact processes a container k8s-visible state transition.
// This is used by the lifecycle to signal container creation completion.
func (f *Focus) handleContainerFact(fact *types.ContainerFact) {
        f.view.mu.Lock()
        defer f.view.mu.Unlock()

        cv, ok := f.view.containers[fact.Container]
        if !ok {
                return
        }

        cv.state = fact.State
        cv.ready = fact.Ready
        if fact.State.Running != nil {
                cv.seenRunning = true
                cv.lastStarted = time.Now()
        }

        f.pushStatusLocked()
}

// handleProbeFact processes a probe result.
// Updates readiness state and pushes status if the ready condition changed.
func (f *Focus) handleProbeFact(fact *types.ProbeFact) {
        f.view.mu.Lock()
        defer f.view.mu.Unlock()

        cv, ok := f.view.containers[fact.Container]
        if !ok {
                return
        }

        now := time.Now()
        wasReady := cv.ready

        switch fact.ProbeType {
        case "readiness":
                cv.probeLastResult["readiness"] = now
                if fact.Success {
                        cv.readyFailCount = 0
                        cv.readyPassCount++
                        threshold := fact.SuccessThreshold
                        if threshold <= 0 {
                                threshold = 1
                        }
                        if cv.readyPassCount >= threshold {
                                cv.ready = true
                        }
                } else {
                        cv.readyPassCount = 0
                        cv.readyFailCount++
                        threshold := fact.FailureThreshold
                        if threshold <= 0 {
                                threshold = 3
                        }
                        if cv.readyFailCount >= threshold {
                                cv.ready = false
                        }
                }
        case "startup":
                cv.probeLastResult["startup"] = now
                // TODO: startup probe gate — block readiness/liveness probes until startup passes
        case "liveness":
                cv.probeLastResult["liveness"] = now
                // TODO: liveness restart — kill container and trigger restart
        }

        if wasReady != cv.ready {
                f.logger.Info("readiness changed",
                        "container", fact.Container, "wasReady", wasReady, "nowReady", cv.ready)
        }

        f.pushStatusLocked()
}

// handleSpecFact processes a pod spec change from Kubernetes.
func (f *Focus) handleSpecFact(fact *types.SpecFact) {
        f.view.mu.Lock()
        defer f.view.mu.Unlock()

        // TODO: recompute container views for added/removed containers
        f.logger.Debug("spec changed (not yet implemented)", "pod", fact.PodName)
}

// ─── Status Computation ────────────────────────────────────────────────

// computeStatus builds a PodStatus from the Focus's local state.
// MUST be called with f.view.mu held.
func (f *Focus) computeStatus() *corev1.PodStatus {
        phase := corev1.PodRunning
        allReady := true
        containerStatuses := make([]corev1.ContainerStatus, 0, len(f.view.containers))

        // App containers drive the pod phase and readiness.
        for _, c := range f.view.pod.Spec.Containers {
                cv, ok := f.view.containers[c.Name]
                if !ok {
                        continue
                }

                cs := corev1.ContainerStatus{
                        Name:         c.Name,
                        Image:        c.Image,
                        Ready:        cv.ready,
                        RestartCount: cv.restartCount,
                        State:        cv.state,
                }

                if cv.state.Waiting != nil {
                        phase = corev1.PodPending
                }
                if !cv.ready {
                        allReady = false
                }

                containerStatuses = append(containerStatuses, cs)
        }

        // Init container statuses (not counted in readiness).
        for _, c := range f.view.pod.Spec.InitContainers {
                cv, ok := f.view.containers[c.Name]
                if !ok {
                        continue
                }

                cs := corev1.ContainerStatus{
                        Name:         c.Name,
                        Image:        c.Image,
                        Ready:        cv.ready,
                        RestartCount: cv.restartCount,
                        State:        cv.state,
                }

                containerStatuses = append(containerStatuses, cs)
        }

        readyCondition := corev1.ConditionFalse
        if allReady {
                readyCondition = corev1.ConditionTrue
        }

        return &corev1.PodStatus{
                Phase: phase,
                HostIP: f.view.podIP,
                PodIP:  f.view.podIP,
                Conditions: []corev1.PodCondition{{
                        Type:   corev1.PodReady,
                        Status: readyCondition,
                }},
                ContainerStatuses: containerStatuses,
        }
}

// pushStatusLocked computes the current status and sends it to Horizon.
// MUST be called with f.view.mu held.
func (f *Focus) pushStatusLocked() {
        status := f.computeStatus()
        updated := f.view.pod.DeepCopy()
        status.DeepCopyInto(&updated.Status)
        f.horizon.WritePodStatus(updated)
}

// ─── Constants ─────────────────────────────────────────────────────────

const (
        restartBackoffInit = 1 * time.Second
        maxBackoff         = 5 * time.Minute
)

// ─── Unit Name Parsing ─────────────────────────────────────────────────

// ParseContainerFromUnit extracts the container name from a systemd unit name.
// Format: perigeos-<pawn>-pod-<uid>-<containerName>.service
//
// Since the Focus already knows its UID (facts are pre-routed by UID),
// we only need the container name portion.
//
// This is the foci-local version of gambit.ParseUnitName that doesn't
// require importing the node package.
func ParseContainerFromUnit(unitName string) string {
        suffix := ".service"
        if !strings.HasSuffix(unitName, suffix) {
                return ""
        }
        inner := unitName[:len(unitName)-len(suffix)]

        // Find the last hyphen that separates <uid> from <containerName>.
        // UIDs are standard 36-char UUIDs (8-4-4-4-12 with hyphens = 36 chars).
        // The separator between UID and containerName is at position 36 (0-indexed),
        // since the prefix "perigeos-<pawn>-pod-" is already stripped by routing.
        //
        // Actually, we receive the full unit name here. We need to find where
        // the UID ends. The UID is 36 chars, so the container name starts
        // after the UID + one hyphen separator.
        //
        // Walk backwards from the suffix to find the container name.
        // The container name is everything after the last UID-terminating hyphen.
        // Since UIDs contain hyphens themselves, we need to count from the front.
        prefix := "perigeos-"
        idx := strings.Index(unitName, prefix)
        if idx == -1 {
                return ""
        }
        rest := unitName[idx+len(prefix):] // "<pawn>-pod-<uid>-<containerName>.service"

        // Skip pawn name (up to "-pod-")
        podIdx := strings.Index(rest, "-pod-")
        if podIdx == -1 {
                return ""
        }
        afterPod := rest[podIdx+5:] // "<uid>-<containerName>.service"
        afterPod = afterPod[:len(afterPod)-len(suffix)] // "<uid>-<containerName>"

        // UID is 36 chars (8-4-4-4-12 format).
        // After the UID, there's a hyphen, then the container name.
        if len(afterPod) < 38 { // 36 (UUID) + 1 (hyphen) + at least 1 char
                return ""
        }

        return afterPod[37:]
}
