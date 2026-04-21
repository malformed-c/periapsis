package probescheduler

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/malformed-c/periapsis/internal/types"
	"github.com/malformed-c/periapsis/node"
	corev1 "k8s.io/api/core/v1"
)

const (
	maxConcurrentProbes = 50
	probeTickInterval   = 2 * time.Second
)

// SyzygySender is the interface for emitting Facts through the event bus.
// Uses the sealed types.Fact interface - any Fact type satisfies this.
type SyzygySender interface {
	Send(fact types.Fact) bool
}

// ProbeSchedulerDeps holds dependencies.
type ProbeSchedulerDeps struct {
	Store  *node.PodStore
	Syzygy SyzygySender
	Logger *slog.Logger
}

// ProbeScheduler runs probes and emits ProbeFacts.
type ProbeScheduler struct {
	store  *node.PodStore
	syzygy SyzygySender
	logger *slog.Logger
	sem    chan struct{}
}

// NewProbeScheduler creates a new ProbeScheduler.
func NewProbeScheduler(deps ProbeSchedulerDeps) *ProbeScheduler {
	return &ProbeScheduler{
		store:  deps.Store,
		syzygy: deps.Syzygy,
		logger: deps.Logger.With("component", "probe-scheduler"),
		sem:    make(chan struct{}, maxConcurrentProbes),
	}
}

// Run starts the probe scheduler loop.
func (s *ProbeScheduler) Run(ctx context.Context) {
	ticker := time.NewTicker(probeTickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.runProbeCycle(ctx)
		}
	}
}

func (s *ProbeScheduler) runProbeCycle(ctx context.Context) {
	entries := s.store.Snapshot()

	var wg sync.WaitGroup
	for i := range entries {
		e := &entries[i]

		if e.Phase == corev1.PodPending || e.Phase == corev1.PodSucceeded || e.Phase == corev1.PodFailed {
			continue
		}

		for j := range e.Pod.Spec.Containers {
			c := &e.Pod.Spec.Containers[j]

			ps := s.store.ProbeState(e.UID, c.Name)
			if ps == nil {
				continue
			}

			wg.Add(1)
			go func(uid string, pod *corev1.Pod, container *corev1.Container, podIP string) {
				defer wg.Done()

				select {
				case s.sem <- struct{}{}:
				case <-ctx.Done():
					return
				}
				defer func() { <-s.sem }()

				s.probeContainer(ctx, uid, pod, container, podIP)
			}(e.UID, e.Pod, c, e.PodIP)
		}
	}
	wg.Wait()
}

func (s *ProbeScheduler) probeContainer(ctx context.Context, uid string, pod *corev1.Pod, c *corev1.Container, podIP string) {
	ps := s.store.ProbeState(uid, c.Name)
	if ps == nil {
		return
	}

	runner := s.store.ProbeRunner()

	// 1. Startup probe gates liveness/readiness.
	if c.StartupProbe != nil && !ps.StartupPassed {
		if node.IsDue(ps, "startup", c.StartupProbe.PeriodSeconds, c.StartupProbe.InitialDelaySeconds) {
			result := runner.RunProbe(ctx, pod, c.Name, c.StartupProbe, podIP)

			var restart bool
			s.store.UpdateProbeState(uid, c.Name, func(ps *node.ContainerProbeState) {
				node.MarkProbed(ps, "startup")
				restart = node.EvalStartup(ps, c.StartupProbe, result)
			})

			updated := s.store.ProbeState(uid, c.Name)
			if updated != nil {
				s.syzygy.Send(types.NewProbeFact(
					uid, c.Name, "startup",
					result == node.ProbeSuccess,
					updated.StartupPassed,
					updated.StartupPassed,
					safeThreshold(c.StartupProbe.SuccessThreshold, 1),
					safeThreshold(c.StartupProbe.FailureThreshold, 3),
				))
			}

			if restart {
				s.logger.Warn("startup probe failed past threshold, needs restart",
					"pod", pod.Name, "container", c.Name)
			}
		}
		return
	}

	// 2. Liveness probe.
	if c.LivenessProbe != nil && node.IsDue(ps, "liveness", c.LivenessProbe.PeriodSeconds, c.LivenessProbe.InitialDelaySeconds) {
		result := runner.RunProbe(ctx, pod, c.Name, c.LivenessProbe, podIP)

		var restart bool
		s.store.UpdateProbeState(uid, c.Name, func(ps *node.ContainerProbeState) {
			node.MarkProbed(ps, "liveness")
			restart = node.EvalLiveness(ps, c.LivenessProbe, result)
		})

		if restart {
			s.store.ResetProbeState(uid, c.Name)
		}

		updated := s.store.ProbeState(uid, c.Name)
		if updated != nil {
			s.syzygy.Send(types.NewProbeFact(
				uid, c.Name, "liveness",
				result == node.ProbeSuccess,
				!restart,
				updated.StartupPassed,
				safeThreshold(c.LivenessProbe.SuccessThreshold, 1),
				safeThreshold(c.LivenessProbe.FailureThreshold, 3),
			))
		}

		if restart {
			s.logger.Warn("liveness probe failed past threshold, needs restart",
				"pod", pod.Name, "container", c.Name)
			return
		}
	}

	// 3. Readiness probe.
	if c.ReadinessProbe != nil && node.IsDue(ps, "readiness", c.ReadinessProbe.PeriodSeconds, c.ReadinessProbe.InitialDelaySeconds) {
		result := runner.RunProbe(ctx, pod, c.Name, c.ReadinessProbe, podIP)

		s.store.UpdateProbeState(uid, c.Name, func(ps *node.ContainerProbeState) {
			node.MarkProbed(ps, "readiness")
			node.EvalReadiness(ps, c.ReadinessProbe, result)
		})

		updated := s.store.ProbeState(uid, c.Name)
		if updated != nil {
			s.syzygy.Send(types.NewProbeFact(
				uid, c.Name, "readiness",
				result == node.ProbeSuccess,
				updated.Ready,
				updated.StartupPassed,
				safeThreshold(c.ReadinessProbe.SuccessThreshold, 1),
				safeThreshold(c.ReadinessProbe.FailureThreshold, 3),
			))
		}
	}
}

func safeThreshold(val, defaultVal int32) int32 {
	if val <= 0 {
		return defaultVal
	}
	return val
}
