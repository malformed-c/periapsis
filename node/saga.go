package node

import (
	"context"
	"log/slog"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
)

// compensationTimeout bounds how long the entire compensation sequence may run.
const compensationTimeout = 30 * time.Second

// Saga tracks named compensations for a multi-step pod creation.
// On failure, Compensate runs them in reverse (LIFO) with logging
// and event emission so rollback is observable.
type Saga struct {
	pod    *corev1.Pod
	logger *slog.Logger
	rec    record.EventRecorder
	steps  []sagaStep
}

type sagaStep struct {
	name       string
	compensate func(context.Context)
}

// NewSaga creates a Saga bound to a pod.
func NewSaga(pod *corev1.Pod, logger *slog.Logger, rec record.EventRecorder) *Saga {
	return &Saga{pod: pod, logger: logger, rec: rec}
}

// Add registers a named compensation that will run on rollback.
func (s *Saga) Add(name string, fn func(context.Context)) {
	s.steps = append(s.steps, sagaStep{name: name, compensate: fn})
}

// Compensate runs all registered compensations in reverse order.
// Each step gets a bounded context and is logged with its outcome.
// An event is emitted on the pod summarising the rollback.
func (s *Saga) Compensate() {
	if len(s.steps) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), compensationTimeout)
	defer cancel()

	s.logger.Info("Saga compensating", "pod", s.pod.Name, "steps", len(s.steps))
	s.rec.Eventf(s.pod, corev1.EventTypeWarning, "Compensating",
		"Rolling back %d creation steps", len(s.steps))

	for i := len(s.steps) - 1; i >= 0; i-- {
		step := s.steps[i]
		s.logger.Debug("Compensating step", "step", step.name, "pod", s.pod.Name)

		func() {
			defer func() {
				if r := recover(); r != nil {
					s.logger.Error("Compensation panicked", "step", step.name, "pod", s.pod.Name, "panic", r)
				}
			}()
			step.compensate(ctx)
		}()
	}

	s.logger.Info("Saga compensation complete", "pod", s.pod.Name)
}
