package syzygy

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"

	"github.com/malformed-c/periapsis/internal/horizon"
	"github.com/malformed-c/periapsis/internal/types"
	"github.com/malformed-c/periapsis/node"
)

// Syzygy is the event bus that routes Facts to per-pod Focus actors.
//
// Syzygy sits between the D-Bus event source (systemd unit state changes)
// and the per-pod state machines (Focus). It does not hold pod state
// itself — each Focus owns one pod's state, and Syzygy just routes.
//
// Flow:
//
//	D-Bus signal → Syzygy.Send(UnitFact) → FocusRegistry.Route() → Focus.process()
//	                                                              ↓
//	                                                     Focus.computeStatus()
//	                                                              ↓
//	                                                     Horizon.WritePodStatus()
//	                                                              ↓
//	                                                     k8s API Status Patch
type Syzygy struct {
	inbox  chan *types.Fact
	mu     sync.RWMutex
	closed bool

	logger *slog.Logger

	ps      *node.PodStore
	horizon *horizon.Horizon
	foci    *FocusRegistry
}

type SyzygyDeps struct {
	Logger *slog.Logger

	Ps      *node.PodStore
	Horizon *horizon.Horizon
}

func NewSyzygy(deps SyzygyDeps) *Syzygy {
	return &Syzygy{
		inbox: make(chan *types.Fact, 1024),

		logger:  deps.Logger,
		ps:      deps.Ps,
		horizon: deps.Horizon,
		foci:    NewFocusRegistry(deps.Logger),
	}
}

// Foci returns the FocusRegistry for direct focus management
// (creating/removing foci for pod lifecycle events).
func (s *Syzygy) Foci() *FocusRegistry {
	return s.foci
}

func (s *Syzygy) Run(ctx context.Context) {
	wg := sync.WaitGroup{}
	wg.Go(func() { s.runAntiEntropyLoop(ctx) })
	defer wg.Wait()

Loop:
	for {
		select {
		case fact, ok := <-s.inbox:
			if !ok {
				break Loop
			}

			s.route(ctx, fact)

		case <-ctx.Done():
			break Loop
		}
	}

	s.close()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	for event := range s.inbox {
		s.route(shutdownCtx, event)
	}
}

// Send enqueues a Fact for processing.
// Non-blocking; if the inbox is full, returns false.
func (s *Syzygy) Send(fact *types.Fact) (ok bool) {
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

	s.inbox <- fact

	return true
}

func (s *Syzygy) close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.closed {
		s.closed = true
		close(s.inbox)
	}
}

// runAntiEntropyLoop handles the low-priority background tasks.
// When the inbox is empty, it verifies Focus state against
// the PodStore and reconciles any drift.
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

// route dispatches a Fact to the appropriate Focus.
// This replaces the old per-type handler switch with a single
// routing call — the Focus handles the type-specific logic.
func (s *Syzygy) route(ctx context.Context, fact *types.Fact) {
	// For ProbeFacts, route directly to the Focus.
	// For all other facts, also route to the Focus.
	//
	// Special cases that don't route to a Focus:
	// - PodStatusFact: forwarded to Horizon directly (bypasses Focus)
	if psf, ok := (*fact).(*types.PodStatusFact); ok {
		s.handlePodStatus(psf)
		return
	}

	if !s.foci.Route(fact) {
		// No Focus exists for this UID.
		// This is normal during pod creation (Focus not yet created)
		// or after deletion (Focus already removed).
		s.logger.Debug("no focus for fact, dropping",
			"type", fmt.Sprintf("%T", *fact),
			"uid", factUID(fact))
	}
}

// handlePodStatus processes a direct pod status write request.
// Forwards to Horizon for the actual k8s API write.
// This path is used for lifecycle-initiated status pushes
// (e.g., ContainerCreating during pod creation).
func (s *Syzygy) handlePodStatus(f *types.PodStatusFact) {
	s.logger.Debug("forwarding pod status to horizon", "uid", f.UID, "phase", f.Status.Phase)

	pod := s.ps.GetPodCopy(f.UID)
	if pod == nil {
		s.logger.Warn("pod not found for status push", "uid", f.UID)
		return
	}

	updated := pod.DeepCopy()
	f.Status.DeepCopyInto(&updated.Status)
	s.horizon.Send(updated)
}

func (s *Syzygy) runAntiEntropy(ctx context.Context) {
	// 1. Get all active UIDs from Foci.
	uids := s.foci.UIDs()
	if len(uids) == 0 {
		return
	}

	s.logger.Debug("anti-entropy: checking focus state", "foci", len(uids))

	// 2. Snapshot Focus-computed statuses.
	focusStatuses := s.foci.SnapshotAll()

	// 3. Cross-reference with PodStore phases.
	// If a Focus thinks a pod is Running but the PodStore says Failed,
	// or vice versa, log the drift. Future: re-emit Facts to reconcile.
	for uid, focusStatus := range focusStatuses {
		storePhase := s.ps.PodPhase(uid)
		if focusStatus.Phase != storePhase {
			s.logger.Info("anti-entropy: drift detected",
				"uid", uid,
				"focusPhase", focusStatus.Phase,
				"storePhase", storePhase)
			// TODO: Re-emit a ContainerFact to force the Focus to
			// re-evaluate, or create a SyncFact type.
		}
	}

	// 4. Find pods in PodStore that have no Focus (orphaned by crash).
	// Create Foci for them so they resume receiving events.
	pods := s.ps.GetPods()
	for _, pod := range pods {
		uid := string(pod.UID)
		if s.foci.Get(uid) == nil {
			if s.ps.PodPhase(uid) == corev1.PodRunning {
				s.logger.Info("anti-entropy: creating missing focus for running pod",
					"pod", pod.Name, "uid", uid)
				// TODO: Create Focus from current PodStore state
			}
		}
	}
}
