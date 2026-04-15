package foci

// A Focus owns one pod's state machine.
//
// It receives Facts (immutable events) and computes a FocusSnapshot.
// All mutations happen inside the Focus — the rest of the system
// only observes the computed snapshot. This eliminates races because:
//
//   - Facts are read-only (sealed interface)
//   - Each Focus owns one pod's state (single writer)
//   - Fact processing is sequential within a Focus (chan delivery)
//   - The state machine is a pure function: (state, Fact) -> (state, actions)
//
// Focus is named after the orbital mechanics theme (point of convergence).
// It is the single place where container states, readiness, restart policy,
// and probe results converge into a coherent pod status.
//
// Focus has ZERO k8s imports. It works entirely with flat types.
// Horizon owns the k8s serialization (FocusSnapshot -> corev1.PodStatus).

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/malformed-c/periapsis/internal/types"
)

// ─── Output Types (flat, no k8s) ────────────────────────────────────────

// ContainerSnapshot is the Focus's public view of a single container.
// This is what Horizon uses to build corev1.ContainerStatus.
type ContainerSnapshot struct {
	Name         string
	Image        string
	State        types.ContainerState
	Ready        bool
	RestartCount int32
	IsInit       bool // true for init containers (not counted in pod readiness)
}

// FocusSnapshot is the Focus's public view of a pod.
// This is what Horizon uses to build corev1.PodStatus.
type FocusSnapshot struct {
	UID        string
	PodName    string
	Namespace  string
	PodIP      string
	Phase      types.PodPhase
	AllReady   bool
	Containers []ContainerSnapshot
}

// StatusIntent is what Focus sends to Horizon.
// It carries the full computed state — Horizon maps it to k8s types
// and performs the API write.
type StatusIntent struct {
	Snapshot FocusSnapshot
}

// ─── Container Spec (input, no k8s) ─────────────────────────────────────

// ContainerSpec describes a container from the pod spec, in flat form.
// The caller extracts this from corev1.Container when creating the Focus.
type ContainerSpec struct {
	Name              string
	Image             string
	HasReadinessProbe bool // true => container starts unready, must earn readiness via ProbeFact
	IsInit            bool // true => doesn't affect pod readiness
}

// ─── Internal State ─────────────────────────────────────────────────────

// containerView is the Focus's private per-container state.
// Never shared outside the Focus.
type containerView struct {
	state types.ContainerState

	ready       bool
	seenRunning bool
	restarting  bool

	restartCount int32
	backoff      time.Duration
	lastStarted  time.Time

	probeLastResult map[string]time.Time // "startup"/"liveness"/"readiness" -> last probed
	readyFailCount  int32
	readyPassCount  int32
}

// containerSpec is the immutable spec for a container, stored at Focus creation.
type containerSpec struct {
	name              string
	image             string
	hasReadinessProbe bool
	isInit            bool
}

// podView is the Focus's private view of a pod.
type podView struct {
	mu sync.Mutex

	uid       string
	podName   string
	namespace string
	podIP     string
	policy    string // "Always", "OnFailure", "Never"

	specs      map[string]*containerSpec // containerName -> spec (immutable)
	containers map[string]*containerView // containerName -> mutable state
}

// ─── Focus ──────────────────────────────────────────────────────────────

// Focus is a per-pod state machine actor.
// It processes Facts and emits StatusIntents to the StatusWriter.
type Focus struct {
	view   podView
	inbox  chan *types.Fact
	writer StatusWriter

	logger *slog.Logger
	done   chan struct{}
}

// StatusWriter is the interface for sending computed status to the k8s API layer.
// Implemented by horizon.Horizon — kept as an interface so Focus has zero
// k8s imports and can be unit-tested with a mock writer.
type StatusWriter interface {
	WriteStatus(intent StatusIntent)
}

// FocusConfig holds the configuration for creating a Focus.
// All fields are flat — no k8s types.
type FocusConfig struct {
	UID           string
	PodName       string
	Namespace     string
	PodIP         string
	RestartPolicy string // "Always", "OnFailure", "Never" (default "Always")

	// Container specs extracted from the pod spec by the caller.
	Containers     []ContainerSpec
	InitContainers []ContainerSpec

	Writer StatusWriter
	Logger *slog.Logger
}

// NewFocus creates a new Focus for a pod.
//
// All containers start in Waiting("ContainerCreating") state.
// Readiness defaults to true only if the container has no readiness probe;
// containers with probes start unready and must earn readiness via ProbeFacts.
func NewFocus(cfg FocusConfig) *Focus {
	specs := make(map[string]*containerSpec, len(cfg.Containers)+len(cfg.InitContainers))
	containers := make(map[string]*containerView, len(cfg.Containers)+len(cfg.InitContainers))

	for _, c := range cfg.InitContainers {
		specs[c.Name] = &containerSpec{
			name:              c.Name,
			image:             c.Image,
			hasReadinessProbe: c.HasReadinessProbe,
			isInit:            true,
		}
		containers[c.Name] = &containerView{
			state:           types.WaitingState("ContainerCreating"),
			ready:           true, // init containers don't affect pod readiness
			backoff:         restartBackoffInit,
			lastStarted:     time.Now(),
			probeLastResult: make(map[string]time.Time),
		}
	}

	for _, c := range cfg.Containers {
		specs[c.Name] = &containerSpec{
			name:              c.Name,
			image:             c.Image,
			hasReadinessProbe: c.HasReadinessProbe,
			isInit:            false,
		}
		containers[c.Name] = &containerView{
			state:           types.WaitingState("ContainerCreating"),
			ready:           !c.HasReadinessProbe, // no probe => immediately ready
			backoff:         restartBackoffInit,
			lastStarted:     time.Now(),
			probeLastResult: make(map[string]time.Time),
		}
	}

	if cfg.RestartPolicy == "" {
		cfg.RestartPolicy = "Always"
	}

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	return &Focus{
		view: podView{
			uid:        cfg.UID,
			podName:    cfg.PodName,
			namespace:  cfg.Namespace,
			podIP:      cfg.PodIP,
			policy:     cfg.RestartPolicy,
			specs:      specs,
			containers: containers,
		},
		inbox:  make(chan *types.Fact, 64),
		writer: cfg.Writer,
		logger: logger.With("component", "focus", "pod", cfg.PodName, "uid", cfg.UID),
		done:   make(chan struct{}),
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

// Snapshot returns the current computed FocusSnapshot.
// This is safe to call from any goroutine.
func (f *Focus) Snapshot() FocusSnapshot {
	f.view.mu.Lock()
	defer f.view.mu.Unlock()
	return f.computeSnapshot()
}

// ─── Fact Handlers ─────────────────────────────────────────────────────

// handleUnitFact processes a systemd unit substate change.
// Maps the raw substate to a container state transition.
func (f *Focus) handleUnitFact(fact *types.UnitFact) {
	containerName := ParseContainerFromUnit(fact.UnitName)
	if containerName == "" {
		return
	}

	f.view.mu.Lock()
	defer f.view.mu.Unlock()

	cv, ok := f.view.containers[containerName]
	if !ok {
		return
	}

	switch fact.SubState {
	case "running":
		cv.seenRunning = true
		cv.restarting = false
		cv.state = types.RunningState(time.Now())
	case "failed":
		cv.state = types.WaitingState("CrashLoopBackOff")
	case "start-pre", "start", "start-post":
		cv.state = types.WaitingState("ContainerCreating")
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

	// If we never saw it running, defer the terminal decision.
	if !cv.seenRunning {
		f.logger.Debug("exit received but container never seen running — deferring",
			"container", fact.Container, "exitCode", fact.ExitCode)
		cv.state = types.WaitingState("ContainerCreating")
		return
	}

	shouldRestart := false
	switch f.view.policy {
	case "Always":
		shouldRestart = true
	case "OnFailure":
		shouldRestart = fact.ExitCode != 0
	case "Never":
		shouldRestart = false
	}

	if shouldRestart {
		cv.restarting = true
		cv.restartCount++
		cv.backoff = min(cv.backoff*2, maxBackoff)
		cv.state = types.WaitingState("CrashLoopBackOff")
		f.logger.Info("container exited, restarting",
			"container", fact.Container, "exitCode", fact.ExitCode,
			"restartCount", cv.restartCount, "backoff", cv.backoff)
		// TODO: launch restart goroutine, emit UnitFact when complete
	} else {
		finishedAt := time.Time{}
		if fact.FinishedAt != "" {
			if t, err := time.Parse(time.RFC3339, fact.FinishedAt); err == nil {
				finishedAt = t
			}
		}
		cv.state = types.TerminatedState(fact.ExitCode, finishedAt)
		f.logger.Info("container exited (terminal)",
			"container", fact.Container, "exitCode", fact.ExitCode)
	}

	f.pushStatusLocked()
}

// handleContainerFact processes a container state transition from the lifecycle.
func (f *Focus) handleContainerFact(fact *types.ContainerFact) {
	f.view.mu.Lock()
	defer f.view.mu.Unlock()

	cv, ok := f.view.containers[fact.Container]
	if !ok {
		return
	}

	cv.state = fact.State
	cv.ready = fact.Ready
	if fact.State.Kind == types.StateRunning {
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

// computeSnapshot builds a FocusSnapshot from the Focus's local state.
// MUST be called with f.view.mu held.
func (f *Focus) computeSnapshot() FocusSnapshot {
	phase := types.PhaseRunning
	allReady := true
	containerSnapshots := make([]ContainerSnapshot, 0, len(f.view.specs))

	for name, cv := range f.view.containers {
		spec, ok := f.view.specs[name]
		if !ok {
			continue
		}

		cs := ContainerSnapshot{
			Name:         spec.name,
			Image:        spec.image,
			State:        cv.state,
			Ready:        cv.ready,
			RestartCount: cv.restartCount,
			IsInit:       spec.isInit,
		}

		// Waiting state implies Pending phase (only for app containers).
		if cv.state.Kind == types.StateWaiting && !spec.isInit {
			phase = types.PhasePending
		}

		// All app containers must be ready for pod to be ready.
		if !cv.ready && !spec.isInit {
			allReady = false
		}

		containerSnapshots = append(containerSnapshots, cs)
	}

	return FocusSnapshot{
		UID:        f.view.uid,
		PodName:    f.view.podName,
		Namespace:  f.view.namespace,
		PodIP:      f.view.podIP,
		Phase:      phase,
		AllReady:   allReady,
		Containers: containerSnapshots,
	}
}

// pushStatusLocked computes the current snapshot and sends it as a StatusIntent.
// MUST be called with f.view.mu held.
func (f *Focus) pushStatusLocked() {
	snapshot := f.computeSnapshot()
	f.writer.WriteStatus(StatusIntent{Snapshot: snapshot})
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
// This is the foci-local version of gambit.ParseUnitName that doesn't
// require importing the node package.
func ParseContainerFromUnit(unitName string) string {
	suffix := ".service"
	if !strings.HasSuffix(unitName, suffix) {
		return ""
	}

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
	afterPod := rest[podIdx+5:]                     // "<uid>-<containerName>.service"
	afterPod = afterPod[:len(afterPod)-len(suffix)] // "<uid>-<containerName>"

	// UID is 36 chars (8-4-4-4-12 format).
	if len(afterPod) < 38 {
		return ""
	}

	return afterPod[37:]
}
