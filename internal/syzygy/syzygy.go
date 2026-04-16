package syzygy

// Syzygy is the imperative shell — the single goroutine that owns all pod state.
//
// It receives Facts (immutable events), feeds them through the pure Reduce
// function, stores the resulting PodState, and dispatches Effects. Effects
// split into two categories:
//
//   Local state effects (handled synchronously in this goroutine):
//     - SetPodPhase      → callback into PodStore
//     - PersistPodState  → callback into disk layer
//     - InitRestartState → callback into PodStore
//
//   k8s API effects (forwarded to Horizon's worker pool):
//     - UpdateStatus, RestartContainer, ResetUnit, RecordEvent
//
// This split keeps Horizon as a pure k8s API executor with no PodStore
// dependency. Local state ops run inline here — no concurrency concern
// because Syzygy is single-threaded for state mutations.
//
// Memory at 3000 pods:
//   - map[string]foci.PodState: ~1.5MB (PodState is ~300-500 bytes)
//   - No per-pod goroutines (saves ~16-48MB vs Focus actor model)
//   - No *corev1.Pod stored per pod (saves ~30MB)
//   - No DeepCopy on hot path (PodState is a value type)

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/malformed-c/periapsis/internal/foci"
	"github.com/malformed-c/periapsis/internal/horizon"
	"github.com/malformed-c/periapsis/internal/types"
	corev1 "k8s.io/api/core/v1"
)

// Syzygy is the event loop that owns all pod state.
type Syzygy struct {
	inbox  chan types.Fact
	mu     sync.RWMutex
	closed bool

	logger *slog.Logger

	// states is the single source of truth for all pod state machines.
	// Only accessed from the Run goroutine — no locks needed.
	states map[string]foci.PodState

	// horizon executes k8s API effects.
	horizon *horizon.Horizon

	// Local state callbacks — executed synchronously in the Run goroutine.
	// These ops are not k8s API calls; they're fast local mutations.

	// setPodPhase updates the PodStore's phase map.
	setPodPhase func(uid string, phase corev1.PodPhase)

	// persistPodState persists pod state to disk.
	persistPodState func(uid string)

	// initRestartState initializes restart/probe tracking for a new pod.
	// Receives flat ContainerInitPayload — no *corev1.Pod pointer.
	initRestartState func(uid, namespace, name string, containers []types.ContainerInitPayload)
}

// SyzygyDeps holds the dependencies for creating a Syzygy.
type SyzygyDeps struct {
	Logger  *slog.Logger
	Horizon *horizon.Horizon

	// Optional local state callbacks. If nil, the operation is a no-op.
	SetPodPhase      func(uid string, phase corev1.PodPhase)
	PersistPodState  func(uid string)
	InitRestartState func(uid, namespace, name string, containers []types.ContainerInitPayload)
}

// NewSyzygy creates a new Syzygy event loop.
func NewSyzygy(deps SyzygyDeps) *Syzygy {
	if deps.SetPodPhase == nil {
		deps.SetPodPhase = func(string, corev1.PodPhase) {}
	}
	if deps.PersistPodState == nil {
		deps.PersistPodState = func(string) {}
	}
	if deps.InitRestartState == nil {
		deps.InitRestartState = func(string, string, string, []types.ContainerInitPayload) {}
	}

	return &Syzygy{
		inbox:            make(chan types.Fact, 2048),
		logger:           deps.Logger.With("component", "syzygy"),
		states:           make(map[string]foci.PodState),
		horizon:          deps.Horizon,
		setPodPhase:      deps.SetPodPhase,
		persistPodState:  deps.PersistPodState,
		initRestartState: deps.InitRestartState,
	}
}

// Run is the main event loop. It processes Facts sequentially until
// the context is cancelled, then drains remaining facts during shutdown.
func (s *Syzygy) Run(ctx context.Context) {
	// Anti-entropy loop runs in a separate goroutine.
	var aeWg sync.WaitGroup
	aeCtx, aeCancel := context.WithCancel(ctx)
	defer func() {
		aeCancel()
		aeWg.Wait()
	}()

	aeWg.Add(1)
	go func() {
		defer aeWg.Done()
		s.runAntiEntropyLoop(aeCtx)
	}()

Loop:
	for {
		select {
		case fact, ok := <-s.inbox:
			if !ok {
				break Loop
			}
			s.processFact(ctx, fact)
		case <-ctx.Done():
			break Loop
		}
	}

	s.close()

	// Drain remaining facts during shutdown.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	for fact := range s.inbox {
		s.processFact(shutdownCtx, fact)
	}
}

// Send enqueues a Fact for processing. Non-blocking; returns false if
// the inbox is full or Syzygy is closed.
func (s *Syzygy) Send(fact types.Fact) (ok bool) {
	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()

	if closed {
		return false
	}

	defer func() {
		if recover() != nil {
			ok = false
		}
	}()

	select {
	case s.inbox <- fact:
		return true
	default:
		s.logger.Warn("syzygy inbox full, dropping fact",
			"type", fmt.Sprintf("%T", fact))
		return false
	}
}

// close marks the Syzygy as closed and closes the inbox channel.
func (s *Syzygy) close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.closed {
		s.closed = true
		close(s.inbox)
	}
}

// --- Core Processing ----------------------------------------------------

// processFact is the heart of the event loop. It:
//  1. Extracts the UID from the Fact
//  2. Looks up the current PodState (zero-value if untracked)
//  3. Calls the pure Reduce function
//  4. Stores the new PodState
//  5. Dispatches all returned Effects
func (s *Syzygy) processFact(ctx context.Context, fact types.Fact) {
	uid := factUID(fact)
	if uid == "" {
		s.logger.Debug("fact has no UID, dropping", "type", fmt.Sprintf("%T", fact))
		return
	}

	// Look up current state — zero-value PodState means UID is empty,
	// which Reduce uses to detect untracked pods.
	currentState := s.states[uid]

	// Pure computation — no side effects.
	newState, effects := foci.Reduce(currentState, fact)

	// PodEvictFact returns zero-value PodState — remove from map.
	if newState.UID == "" {
		delete(s.states, uid)
		s.logger.Info("pod evicted from state machine", "uid", uid)
	} else {
		s.states[uid] = newState
	}

	// Execute effects.
	for _, eff := range effects {
		s.executeEffect(ctx, eff)
	}
}

// executeEffect dispatches an Effect to the appropriate handler.
//
// Local state effects are handled synchronously in this goroutine — they
// are fast, non-blocking, and must not race with the state map.
//
// k8s API effects are forwarded to Horizon's worker pool.
func (s *Syzygy) executeEffect(ctx context.Context, eff types.Effect) {
	switch e := eff.(type) {
	// --- Local state effects (handled here, synchronous) ----------------

	case types.SetPodPhase:
		s.setPodPhase(e.UID, e.Phase)

	case types.PersistPodState:
		s.persistPodState(e.UID)

	case types.InitRestartState:
		s.initRestartState(e.UID, e.Namespace, e.Name, e.Containers)

	// --- k8s API effects (forwarded to Horizon) -------------------------

	case types.UpdateStatus:
		s.horizon.Send(e)

	case types.RestartContainer:
		s.horizon.Send(e)

	case types.ResetUnit:
		s.horizon.Send(e)

	case types.RecordEvent:
		s.horizon.Send(e)

	default:
		s.logger.Warn("unknown effect type", "type", fmt.Sprintf("%T", eff))
	}
}

// --- Anti-Entropy --------------------------------------------------------

func (s *Syzygy) runAntiEntropyLoop(ctx context.Context) {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case _, ok := <-ticker.C:
			if !ok {
				return
			}
			if len(s.inbox) == 0 {
				s.runAntiEntropy(ctx)
			}
		case <-ctx.Done():
			return
		}
	}
}

// runAntiEntropy checks for state drift and reconciles.
// TODO: Implement full anti-entropy — compare PodState phases against
// PodStore phases, re-emit ContainerStateFacts for drifted pods.
func (s *Syzygy) runAntiEntropy(_ context.Context) {
	s.logger.Debug("anti-entropy: checking state",
		"pods", len(s.states))
}

// --- Public Accessors ----------------------------------------------------

// PodState returns the PodState for a given UID. Safe to call from any
// goroutine, but the returned value is a snapshot — it may be stale by
// the time the caller reads it.
//
// NOTE: This reads from the states map which is owned by the Run goroutine.
// In the current single-goroutine design this is safe because map reads
// don't conflict with other reads. Add synchronization if concurrent
// writers are ever introduced.
func (s *Syzygy) PodState(uid string) (foci.PodState, bool) {
	state, ok := s.states[uid]
	return state, ok
}

// PodCount returns the number of tracked pods.
func (s *Syzygy) PodCount() int {
	return len(s.states)
}

// UIDs returns all tracked pod UIDs.
func (s *Syzygy) UIDs() []string {
	uids := make([]string, 0, len(s.states))
	for uid := range s.states {
		uids = append(uids, uid)
	}
	return uids
}

// --- Fact UID Extraction -------------------------------------------------

// factUID extracts the UID from any Fact type.
func factUID(fact types.Fact) string {
	switch f := fact.(type) {
	case *types.UnitFact:
		return f.UID
	case types.UnitFact:
		return f.UID
	case *types.ContainerStateFact:
		return f.UID
	case types.ContainerStateFact:
		return f.UID
	case *types.ExitFact:
		return f.UID
	case types.ExitFact:
		return f.UID
	case *types.ProbeFact:
		return f.UID
	case types.ProbeFact:
		return f.UID
	case *types.SpecFact:
		return f.UID
	case types.SpecFact:
		return f.UID
	case *types.PodAdmitFact:
		return f.UID
	case types.PodAdmitFact:
		return f.UID
	case *types.PodEvictFact:
		return f.UID
	case types.PodEvictFact:
		return f.UID
	case *types.MarkRunningFact:
		return f.UID
	case types.MarkRunningFact:
		return f.UID
	case *types.BackoffResetFact:
		return f.UID
	case types.BackoffResetFact:
		return f.UID
	default:
		return ""
	}
}
