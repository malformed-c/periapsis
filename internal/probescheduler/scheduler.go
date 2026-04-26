package probescheduler

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/malformed-c/periapsis/internal/foci"
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

// StateReader provides read access to foci.PodState from Syzygy's state map.
// ProbeScheduler reads container phase and probe timing from foci.ContainerState
// via this interface, instead of querying PodStore directly.
type StateReader interface {
	PodState(uid string) (foci.PodState, bool)
}

// ProbeSchedulerDeps holds dependencies.
type ProbeSchedulerDeps struct {
	Store  *node.PodStore
	Syzygy SyzygySender
	State  StateReader // reads foci.PodState for probe timing
	Logger *slog.Logger
}

// ProbeScheduler runs probes and emits ProbeFacts.
type ProbeScheduler struct {
	store  *node.PodStore
	syzygy SyzygySender
	state  StateReader
	logger *slog.Logger
	sem    chan struct{}
}

// NewProbeScheduler creates a new ProbeScheduler.
func NewProbeScheduler(deps ProbeSchedulerDeps) *ProbeScheduler {
	return &ProbeScheduler{
		store:  deps.Store,
		syzygy: deps.Syzygy,
		state:  deps.State,
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

			// Read probe timing from foci state if available,
			// otherwise fall back to PodStore's probe state.
			var fociCS *foci.ContainerState
			if s.state != nil {
				if ps, ok := s.state.PodState(e.UID); ok {
					if idx := ps.FindContainer(c.Name); idx != -1 {
						fociCS = &ps.Containers[idx]
					}
				}
			}

			// Check if the container has a startup probe that hasn't passed yet.
			// If foci state is available, use it; otherwise fall back to PodStore.
			startupPassed := true // default: no startup probe or already passed
			if c.StartupProbe != nil {
				if fociCS != nil {
					startupPassed = fociCS.StartupPassed
				} else if ps := s.store.ProbeState(e.UID, c.Name); ps != nil {
					startupPassed = ps.StartupPassed
				}
			}

			// Use foci state for isDue check if available.
			var isDue func(probeType string, periodSeconds, initialDelaySeconds int32) bool
			if fociCS != nil {
				isDue = func(probeType string, periodSeconds, initialDelaySeconds int32) bool {
					return fociIsDue(fociCS, probeType, periodSeconds, initialDelaySeconds)
				}
			} else {
				// Fallback to PodStore's probe state.
				ps := s.store.ProbeState(e.UID, c.Name)
				if ps == nil {
					continue
				}
				isDue = func(probeType string, periodSeconds, initialDelaySeconds int32) bool {
					return node.IsDue(ps, probeType, periodSeconds, initialDelaySeconds)
				}
			}

			wg.Add(1)
			go func(uid string, pod *corev1.Pod, container *corev1.Container, podIP string, startupPassed bool, isDue func(string, int32, int32) bool, fociCS *foci.ContainerState) {
				defer wg.Done()

				select {
				case s.sem <- struct{}{}:
				case <-ctx.Done():
					return
				}
				defer func() { <-s.sem }()

				s.probeContainer(ctx, uid, pod, container, podIP, startupPassed, isDue, fociCS)
			}(e.UID, e.Pod, c, e.PodIP, startupPassed, isDue, fociCS)
		}
	}
	wg.Wait()
}

// fociIsDue checks if a probe is due using foci.ContainerState instead
// of PodStore's ContainerProbeState. Mirrors the logic of node.isDue.
func fociIsDue(cs *foci.ContainerState, probeType string, periodSeconds, initialDelaySeconds int32) bool {
	if periodSeconds <= 0 {
		periodSeconds = 10 // k8s default
	}
	last, ok := cs.LastProbeTime[probeType]
	if !ok {
		// First probe: respect initial delay from container start.
		if initialDelaySeconds > 0 && !cs.ProbeStartedAt.IsZero() {
			return time.Since(cs.ProbeStartedAt) >= time.Duration(initialDelaySeconds)*time.Second
		}
		return true
	}
	return time.Since(last) >= time.Duration(periodSeconds)*time.Second
}

func (s *ProbeScheduler) probeContainer(ctx context.Context, uid string, pod *corev1.Pod, c *corev1.Container, podIP string, startupPassed bool, isDue func(string, int32, int32) bool, fociCS *foci.ContainerState) {
	runner := s.store.ProbeRunner()

	// 1. Startup probe gates liveness/readiness.
	if c.StartupProbe != nil && !startupPassed {
		if isDue("startup", c.StartupProbe.PeriodSeconds, c.StartupProbe.InitialDelaySeconds) {
			result := runner.RunProbe(ctx, pod, c.Name, c.StartupProbe, podIP)

			// Evaluate startup probe using foci state or PodStore fallback.
			var restart bool
			var updatedStartupPassed bool
			var startupFailCount int32

			if fociCS != nil {
				// Compute new counts from foci state.
				startupFailCount = fociCS.StartupFailCount
				if result == node.ProbeSuccess {
					updatedStartupPassed = true
					startupFailCount = 0
				} else {
					startupFailCount++
				}
				threshold := c.StartupProbe.FailureThreshold
				if threshold <= 0 {
					threshold = 3
				}
				restart = startupFailCount >= threshold
				if result == node.ProbeSuccess {
					updatedStartupPassed = true
				}
			} else {
				// Fallback to PodStore.
				s.store.UpdateProbeState(uid, c.Name, func(ps *node.ContainerProbeState) {
					node.MarkProbed(ps, "startup")
					restart = node.EvalStartup(ps, c.StartupProbe, result)
				})
				if updated := s.store.ProbeState(uid, c.Name); updated != nil {
					updatedStartupPassed = updated.StartupPassed
					startupFailCount = updated.StartupFailCount
				}
			}

			// Get current counts for the ProbeFact from foci state.
			var liveFailCount, readyFailCount, readySuccessCount int32
			if fociCS != nil {
				liveFailCount = fociCS.LiveFailCount
				readyFailCount = fociCS.ReadyFailCount
				readySuccessCount = fociCS.ReadySuccessCount
			}

			s.syzygy.Send(types.NewProbeFactWithCounts(
				uid, c.Name, "startup",
				result == node.ProbeSuccess,
				updatedStartupPassed,
				updatedStartupPassed,
				safeThreshold(c.StartupProbe.SuccessThreshold, 1),
				safeThreshold(c.StartupProbe.FailureThreshold, 3),
				startupFailCount,
				liveFailCount,
				readyFailCount,
				readySuccessCount,
			))

			if restart {
				s.logger.Warn("startup probe failed past threshold, needs restart",
					"pod", pod.Name, "container", c.Name)
			}
		}
		return
	}

	// 2. Liveness probe.
	if c.LivenessProbe != nil && isDue("liveness", c.LivenessProbe.PeriodSeconds, c.LivenessProbe.InitialDelaySeconds) {
		result := runner.RunProbe(ctx, pod, c.Name, c.LivenessProbe, podIP)

		var restart bool
		var liveFailCount int32

		if fociCS != nil {
			liveFailCount = fociCS.LiveFailCount
			if result == node.ProbeSuccess {
				liveFailCount = 0
			} else {
				liveFailCount++
			}
			threshold := c.LivenessProbe.FailureThreshold
			if threshold <= 0 {
				threshold = 3
			}
			restart = liveFailCount >= threshold
		} else {
			s.store.UpdateProbeState(uid, c.Name, func(ps *node.ContainerProbeState) {
				node.MarkProbed(ps, "liveness")
				restart = node.EvalLiveness(ps, c.LivenessProbe, result)
			})
			if updated := s.store.ProbeState(uid, c.Name); updated != nil {
				liveFailCount = updated.LiveFailCount
			}
		}

		if restart {
			if fociCS == nil {
				s.store.ResetProbeState(uid, c.Name)
			}
			// When using foci state, reset is handled by the
			// MarkRunningFact that will be emitted when the
			// container restarts (reduceUnitFact resets counts).
		}

		// Get current counts for the ProbeFact.
		var startupFailCount, readyFailCount, readySuccessCount int32
		var startupPassedVal bool
		if fociCS != nil {
			startupFailCount = fociCS.StartupFailCount
			readyFailCount = fociCS.ReadyFailCount
			readySuccessCount = fociCS.ReadySuccessCount
			startupPassedVal = fociCS.StartupPassed
		} else {
			if updated := s.store.ProbeState(uid, c.Name); updated != nil {
				startupFailCount = updated.StartupFailCount
				readyFailCount = updated.ReadyFailCount
				readySuccessCount = updated.ReadySuccessCount
				startupPassedVal = updated.StartupPassed
			}
		}

		s.syzygy.Send(types.NewProbeFactWithCounts(
			uid, c.Name, "liveness",
			result == node.ProbeSuccess,
			!restart,
			startupPassedVal,
			safeThreshold(c.LivenessProbe.SuccessThreshold, 1),
			safeThreshold(c.LivenessProbe.FailureThreshold, 3),
			startupFailCount,
			liveFailCount,
			readyFailCount,
			readySuccessCount,
		))

		if restart {
			s.logger.Warn("liveness probe failed past threshold, needs restart",
				"pod", pod.Name, "container", c.Name)
			return
		}
	}

	// 3. Readiness probe.
	if c.ReadinessProbe != nil && isDue("readiness", c.ReadinessProbe.PeriodSeconds, c.ReadinessProbe.InitialDelaySeconds) {
		result := runner.RunProbe(ctx, pod, c.Name, c.ReadinessProbe, podIP)

		var ready bool
		var readyFailCount, readySuccessCount int32

		if fociCS != nil {
			readyFailCount = fociCS.ReadyFailCount
			readySuccessCount = fociCS.ReadySuccessCount
			if result == node.ProbeSuccess {
				readyFailCount = 0
				readySuccessCount++
				successThreshold := c.ReadinessProbe.SuccessThreshold
				if successThreshold <= 0 {
					successThreshold = 1
				}
				if readySuccessCount >= successThreshold {
					ready = true
				} else {
					ready = fociCS.Ready // keep current state
				}
			} else {
				readySuccessCount = 0
				readyFailCount++
				failThreshold := c.ReadinessProbe.FailureThreshold
				if failThreshold <= 0 {
					failThreshold = 3
				}
				if readyFailCount >= failThreshold {
					ready = false
				} else {
					ready = fociCS.Ready // keep current state
				}
			}
		} else {
			s.store.UpdateProbeState(uid, c.Name, func(ps *node.ContainerProbeState) {
				node.MarkProbed(ps, "readiness")
				node.EvalReadiness(ps, c.ReadinessProbe, result)
				ready = ps.Ready
				readyFailCount = ps.ReadyFailCount
				readySuccessCount = ps.ReadySuccessCount
			})
		}

		// Get remaining counts for the ProbeFact.
		var startupFailCount, liveFailCount int32
		var startupPassedVal bool
		if fociCS != nil {
			startupFailCount = fociCS.StartupFailCount
			liveFailCount = fociCS.LiveFailCount
			startupPassedVal = fociCS.StartupPassed
		} else {
			if updated := s.store.ProbeState(uid, c.Name); updated != nil {
				startupFailCount = updated.StartupFailCount
				liveFailCount = updated.LiveFailCount
				startupPassedVal = updated.StartupPassed
			}
		}

		s.syzygy.Send(types.NewProbeFactWithCounts(
			uid, c.Name, "readiness",
			result == node.ProbeSuccess,
			ready,
			startupPassedVal,
			safeThreshold(c.ReadinessProbe.SuccessThreshold, 1),
			safeThreshold(c.ReadinessProbe.FailureThreshold, 3),
			startupFailCount,
			liveFailCount,
			readyFailCount,
			readySuccessCount,
		))
	}
}

func safeThreshold(val, defaultVal int32) int32 {
	if val <= 0 {
		return defaultVal
	}
	return val
}
