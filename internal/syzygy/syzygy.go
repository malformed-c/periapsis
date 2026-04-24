package syzygy

// Syzygy is the imperative shell - the single goroutine that owns all pod state.
//
// Architecture: Functional Core, Imperative Shell with worker pool.
//
//   Main goroutine (single-threaded):
//     1. Receive Fact from inbox
//     2. Look up PodState (zero-value if untracked)
//     3. Call foci.Reduce(state, fact) -> (newState, []Effect)
//     4. Store newState in map
//     5. Push each Effect into the effects channel (non-blocking)
//
//   Effect worker pool (N goroutines):
//     - Drain the effects channel
//     - Local state effects (SetPodPhase, PersistPodState, InitRestartState)
//       are executed directly via injected callbacks
//     - k8s API effects are forwarded to Horizon's own worker pool
//
// This decoupling means the main loop never blocks on I/O or slow callbacks.
// The state map is exclusively owned by the main goroutine - no locks needed.
// Effect workers only call pure callbacks or non-blocking horizon.Send().
//
// Memory at 3000 pods:
//   - map[string]foci.PodState: ~1.5MB
//   - No per-pod goroutines
//   - No *corev1.Pod stored per pod
//   - No DeepCopy on hot path

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

const (
	// defaultEffectWorkers is the number of effect-dispatch goroutines.
	// Tuned so slow callbacks (disk persistence) don't back up the pool.
	defaultEffectWorkers = 4

	// effectsBuf is the capacity of the effects channel.
	// Sized for burst: 3000 pods × ~3 effects per Reduce call = ~9000 in-flight.
	effectsBuf = 16384
)

// Syzygy is the event loop that owns all pod state.
type Syzygy struct {
	inbox   chan types.Fact
	effects chan types.Effect

	mu     sync.RWMutex
	closed bool

	logger *slog.Logger

	// states is the single source of truth for all pod state machines.
	// Only accessed from the Run goroutine - no locks needed.
	states map[string]foci.PodState

	// horizon executes k8s API effects.
	horizon *horizon.Horizon

	// Local state callbacks - called by effect workers.
	// These are fast, non-blocking in-memory operations.
	// The callbacks themselves must be goroutine-safe (called concurrently).

	// setPodPhase updates the PodStore's phase map.
	setPodPhase func(uid string, phase corev1.PodPhase)

	// persistPodState persists pod state to disk.
	persistPodState func(uid string)

	// initRestartState initializes restart/probe tracking for a new pod.
	initRestartState func(uid, namespace, name string, containers []types.ContainerInitPayload)
}

// SyzygyConfig holds the dependencies for creating a Syzygy.
type SyzygyConfig struct {
	Logger  *slog.Logger
	Horizon *horizon.Horizon

	// Optional local state callbacks. If nil, the operation is a no-op.
	// Callbacks are invoked from worker goroutines and must be goroutine-safe.
	SetPodPhase      func(uid string, phase corev1.PodPhase)
	PersistPodState  func(uid string)
	InitRestartState func(uid, namespace, name string, containers []types.ContainerInitPayload)
}

// NewSyzygy creates a new Syzygy event loop.
func NewSyzygy(deps SyzygyConfig) *Syzygy {
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
		effects:          make(chan types.Effect, effectsBuf),
		logger:           deps.Logger.With("component", "syzygy"),
		states:           make(map[string]foci.PodState),
		horizon:          deps.Horizon,
		setPodPhase:      deps.SetPodPhase,
		persistPodState:  deps.PersistPodState,
		initRestartState: deps.InitRestartState,
	}
}

// Run is the main event loop. Takes a workerCount for the effect pool.
// Blocks until the context is cancelled.
func (s *Syzygy) Run(ctx context.Context, workerCount uint8) {
	if workerCount == 0 {
		workerCount = defaultEffectWorkers
	}

	// --- Effect worker pool ---
	// Workers drain the effects channel. Started before the fact loop so
	// there are consumers ready before any effects are produced.
	wg := sync.WaitGroup{}
	for i := uint8(0); i < workerCount; i++ {
		wg.Go(func() {
			// Use a background-derived ctx for the worker so it can
			// finish draining effects after the main context is cancelled.
			// The channel close is what signals final exit.
			for eff := range s.effects {
				s.dispatchEffect(ctx, eff)
			}
		})
	}

	// --- Anti-entropy loop ---
	var aeWg sync.WaitGroup
	aeCtx, aeCancel := context.WithCancel(ctx)
	defer func() {
		aeCancel()
		aeWg.Wait()
	}()

	aeWg.Go(func() {
		defer aeWg.Done()
		s.runAntiEntropyLoop(aeCtx)
	})

	// --- Main fact loop (single goroutine) ---
Loop:
	for {
		select {
		case fact, ok := <-s.inbox:
			if !ok {
				break Loop
			}

			s.processFact(fact)

		case <-ctx.Done():
			break Loop
		}
	}

	s.close()

	// Drain remaining facts during shutdown.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	for fact := range s.inbox {
		s.processFact(fact)
	}
	_ = shutdownCtx // used by dispatchEffect via captured ctx

	// All facts processed, no more effects will be produced.
	// Close effects channel and wait for workers to drain it.
	close(s.effects)
	wg.Wait()
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

func (s *Syzygy) close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.closed {
		s.closed = true
		close(s.inbox)
	}
}

// --- Core Processing ---

// processFact is the heart of the event loop. It:
//  1. Extracts the UID from the Fact
//  2. Looks up the current PodState (zero-value if untracked)
//  3. Calls the pure Reduce function
//  4. Stores the new PodState
//  5. Pushes all returned Effects into the effects channel
//
// processFact is only ever called from a single goroutine. The effects
// channel send is non-blocking - effects are dropped with a warning if
// the pool is full (effectsBuf is sized to make this rare).
func (s *Syzygy) processFact(fact types.Fact) {
	uid := factUID(fact)
	if uid == "" {
		s.logger.Debug("fact has no UID, dropping", "type", fmt.Sprintf("%T", fact))

		return
	}

	currentState := s.states[uid]

	// Pure computation - no side effects.
	newState, effects := foci.Reduce(currentState, fact)

	// PodEvictFact returns zero-value PodState - remove from map.
	// Only log when the pod was actually in the map - without this guard,
	// every subsequent fact for an already-evicted pod causes Reduce to
	// return zero-value state, triggering this log repeatedly.
	_, wasTracked := s.states[uid]
	if newState.UID == "" {
		delete(s.states, uid)
		if wasTracked {
			s.logger.Info("pod evicted from state machine", "uid", uid)
		}
	} else {
		s.states[uid] = newState
	}

	// Push effects to the worker pool. Non-blocking.
	for _, eff := range effects {
		select {
		case s.effects <- eff:
		default:
			s.logger.Warn("syzygy effects pool full, dropping effect",
				"type", fmt.Sprintf("%T", eff),
				"uid", uid)
		}
	}
}

// dispatchEffect is called by effect workers. It routes each Effect to
// the appropriate handler - either a local state callback or Horizon.
//
// dispatchEffect is called concurrently from multiple goroutines.
// All callbacks must be goroutine-safe.
func (s *Syzygy) dispatchEffect(ctx context.Context, eff types.Effect) {
	switch e := eff.(type) {
	// --- Local state effects (callbacks, goroutine-safe) ---

	case types.SetPodPhase:
		s.setPodPhase(e.UID, e.Phase)

	case types.PersistPodState:
		s.persistPodState(e.UID)

	case types.InitRestartState:
		s.initRestartState(e.UID, e.Namespace, e.Name, e.Containers)

	// --- k8s API effects (Horizon's own worker pool) ---

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

// --- Anti-Entropy ---

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
// TODO: Implement full anti-entropy - compare PodState phases against
// PodStore phases, re-emit ContainerStateFacts for drifted pods.
func (s *Syzygy) runAntiEntropy(_ context.Context) {
	s.logger.Debug("anti-entropy: checking state",
		"pods", len(s.states))
}

// --- Public Accessors ---

// PodState returns the PodState for a given UID. Safe to call from any
// goroutine, but the returned value is a snapshot - may be stale by read time.
//
// NOTE: The states map is owned by the Run goroutine. This is safe for
// concurrent reads because Go map reads don't conflict with other reads.
// Add synchronization if concurrent writers are ever introduced.
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

// --- Fact UID Extraction ---

func factUID(fact types.Fact) string {
	return fact.UID()
}
