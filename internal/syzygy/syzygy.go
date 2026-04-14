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

type Syzygy struct {
	inbox  chan *types.Fact
	mu     sync.RWMutex
	closed bool

	logger *slog.Logger

	ps      *node.PodStore
	horizon *horizon.Horizon
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
	}
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

			s.handler(ctx, fact)

		case <-ctx.Done():
			break Loop
		}
	}

	s.close()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	for event := range s.inbox {
		s.handler(shutdownCtx, event)
	}
}

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

// runAntiEntropyLoop handles the low-priority background tasks
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

func (s *Syzygy) handler(ctx context.Context, fact *types.Fact) {
	switch f := (*fact).(type) {
	case *types.UnitFact:
		// s.handleUnitChange(ctx, f)

	case *types.ExitFact:
		// s.handleContainerExit(ctx, f)

	case *types.SpecFact:
		// s.handleSpecChange(ctx, f)

	default:
		s.logger.Warn("received unknown fact type", "type", fmt.Sprintf("%T", f))
	}
	// pod := s.ps.GetPodCopy(f.UID)
	// if pod == nil {
	// 	s.logger.Error("Pod not found")
	// 	return
	// }

	// err := s.processEvent(ctx, pod, f)
	// if err != nil {
	// 	return
	// }

	// if !s.horizon.Send(pod) {
	// 	return
	// }
}

func (s *Syzygy) processEvent(ctx context.Context, pod *corev1.Pod, event *types.Event) error {
	// RECONCILE:
	switch event.Type {
	case types.TypePod:
	// s.reconcilePod(pod, event)

	case types.TypeContainer:
		// s.reconcileContainer(pod, event)

	case types.TypeSaga:
		// s.reconcileSaga(pod, event)
	}

	return nil
}

func (s *Syzygy) reconcilePod(pod *corev1.Pod, event *types.Event) {
	// 1. Mutate the pod (Horizon)
	pod.Status = event.PodStatus

	// 2. Update the registry
	s.ps.SetPodStatus(event.UID, event.PodStatus)
}

func (s *Syzygy) runAntiEntropy(ctx context.Context) {
	// TODO
	// Example logic:
	// 1. Get a list of all active UIDs from Foci
	// 2. Cross-reference them with the K8s API (via Horizon/Listers)
	// 3. Re-queue events for pods that are out of sync
}
