// Copyright (C) 2025-2026 Malformed C. All rights reserved.
// SPDX-License-Identifier: BUSL-1.1

package node

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net"
	"net/http"
	"time"

	perigeos "github.com/malformed-c/periapsis/internal/runtime"
	"github.com/malformed-c/periapsis/node/api"
	corev1 "k8s.io/api/core/v1"
)

// ProbeResult represents the outcome of a single probe execution.
type ProbeResult int

const (
	ProbeSuccess ProbeResult = iota
	ProbeFailure
	ProbeUnknown
)

func probeResultString(r ProbeResult) string {
	switch r {
	case ProbeSuccess:
		return "success"

	case ProbeFailure:
		return "failure"

	default:
		return "unknown"
	}
}

// ContainerProbeState tracks probe state for a single container.
// Deprecated: Probe timing state is now tracked by foci.ContainerState.
// This struct is kept for backward compatibility with ProbeScheduler's
// fallback path and will be removed once the migration is complete.
type ContainerProbeState struct {
	// StartedAt records when the container (re)started. Used by isDue to
	// honour InitialDelaySeconds before firing the first probe.
	StartedAt time.Time

	// Startup probe
	StartupPassed    bool
	StartupFailCount int32

	// Liveness probe
	LiveFailCount int32

	// Readiness
	Ready             bool
	ReadyFailCount    int32
	ReadySuccessCount int32

	// Timing: keyed by "startup", "liveness", "readiness"
	LastProbeTime map[string]time.Time
}

// ProbeRunner executes probes for containers.
type ProbeRunner struct {
	runtime perigeos.Runtime
	logger  *slog.Logger
}

// NewProbeRunner creates a new probe runner.
func NewProbeRunner(rt perigeos.Runtime, logger *slog.Logger) *ProbeRunner {
	return &ProbeRunner{runtime: rt, logger: logger}
}

// RunProbe executes a single probe against a container and returns the result.
func (pr *ProbeRunner) RunProbe(ctx context.Context, pod *corev1.Pod, containerName string, probe *corev1.Probe, podIP string) ProbeResult {
	if probe == nil {
		return ProbeSuccess
	}

	timeout := time.Duration(probe.TimeoutSeconds) * time.Second
	if timeout == 0 {
		timeout = 1 * time.Second
	}

	probeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var result ProbeResult
	switch {
	case probe.HTTPGet != nil:
		result = pr.runHTTPGetProbe(probeCtx, podIP, probe.HTTPGet)

	case probe.TCPSocket != nil:
		result = pr.runTCPSocketProbe(probeCtx, podIP, probe.TCPSocket)

	case probe.Exec != nil:
		result = pr.runExecProbe(probeCtx, pod, containerName, probe.Exec)

	default:
		return ProbeUnknown
	}

	return result
}

// runHTTPGetProbe sends an HTTP GET to podIP:port+path.
// Pod IPs are routable from the host via Cilium, so no nsenter needed.
func (pr *ProbeRunner) runHTTPGetProbe(ctx context.Context, podIP string, action *corev1.HTTPGetAction) ProbeResult {
	scheme := "http"
	if action.Scheme == corev1.URISchemeHTTPS {
		scheme = "https"
	}

	port := action.Port.String()
	path := action.Path
	if path == "" {
		path = "/"
	}

	// When httpGet.host is set, connect to that address (kubelet behaviour).
	// This is required for hostNetwork pods that bind only on 127.0.0.1.
	target := podIP
	if action.Host != "" {
		target = action.Host
	}
	url := fmt.Sprintf("%s://%s:%s%s", scheme, target, port, path)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		pr.logger.Debug("HTTP probe request creation failed", "url", url, "err", err)

		return ProbeFailure
	}

	for _, h := range action.HTTPHeaders {
		req.Header.Set(h.Name, h.Value)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		pr.logger.Debug("HTTP probe failed", "url", url, "err", err)

		return ProbeFailure
	}
	defer resp.Body.Close()

	// Drain body to allow connection reuse.
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode >= 200 && resp.StatusCode < 400 {
		return ProbeSuccess
	}

	pr.logger.Debug("HTTP probe failed (bad status)", "url", url, "status", resp.StatusCode)

	return ProbeFailure
}

// runTCPSocketProbe opens a TCP connection to podIP:port.
// When action.Host is set, connects to that address instead (same as kubelet).
func (pr *ProbeRunner) runTCPSocketProbe(ctx context.Context, podIP string, action *corev1.TCPSocketAction) ProbeResult {
	host := podIP
	if action.Host != "" {
		host = action.Host
	}
	port := action.Port.String()
	addr := net.JoinHostPort(host, port)

	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		pr.logger.Debug("TCP probe failed", "addr", addr, "err", err)

		return ProbeFailure
	}
	conn.Close()

	pr.logger.Debug("TCP probe succeeded", "addr", addr)

	return ProbeSuccess
}

// runExecProbe runs a command inside the container.
// Exit code 0 = success, anything else = failure.
func (pr *ProbeRunner) runExecProbe(ctx context.Context, pod *corev1.Pod, containerName string, action *corev1.ExecAction) ProbeResult {
	err := pr.runtime.RunInContainer(ctx, string(pod.UID), containerName, action.Command, &noopAttachIO{})
	if err != nil {
		pr.logger.Warn("Exec probe failed",
			"container", containerName,
			"cmd", action.Command,
			"err", err,
		)

		return ProbeFailure
	}

	return ProbeSuccess
}

// isDue returns true if enough time has passed since the last probe of this type.
// On the first invocation (no LastProbeTime entry), it respects InitialDelaySeconds
// measured from the container's start time before allowing the probe to fire.
// IsDue reports whether a probe of the given type is due to run.
func IsDue(state *ContainerProbeState, probeType string, periodSeconds, initialDelaySeconds int32) bool {
	return isDue(state, probeType, periodSeconds, initialDelaySeconds)
}

func isDue(state *ContainerProbeState, probeType string, periodSeconds, initialDelaySeconds int32) bool {
	if periodSeconds <= 0 {
		periodSeconds = 10 // k8s default
	}
	_, ok := state.LastProbeTime[probeType]
	if !ok {
		// First probe: seed LastProbeTime with a random jitter in [0, periodSeconds)
		// so pods started at the same time don't all probe simultaneously.
		// The jitter is added on top of initialDelaySeconds so manifest semantics
		// are preserved - the first probe won't fire before initialDelaySeconds.
		if state.LastProbeTime == nil {
			state.LastProbeTime = make(map[string]time.Time)
		}

		jitter := time.Duration(rand.Int63n(int64(periodSeconds))) * time.Second
		initialDelay := time.Duration(initialDelaySeconds) * time.Second
		if !state.StartedAt.IsZero() {
			// Schedule first fire at: startedAt + initialDelay + jitter.
			// Subtract one period so the normal ">= period since last" check fires at the right time.
			state.LastProbeTime[probeType] = state.StartedAt.Add(initialDelay + jitter - time.Duration(periodSeconds)*time.Second)

		} else {
			// No StartedAt - fire after jitter from now.
			state.LastProbeTime[probeType] = time.Now().Add(jitter - time.Duration(periodSeconds)*time.Second)
		}
	}

	last := state.LastProbeTime[probeType]
	if initialDelaySeconds > 0 && !state.StartedAt.IsZero() {
		if time.Since(state.StartedAt) < time.Duration(initialDelaySeconds)*time.Second {
			return false
		}
	}

	return time.Since(last) >= time.Duration(periodSeconds)*time.Second
}

// MarkProbed records that a probe was just executed.
func MarkProbed(state *ContainerProbeState, probeType string) {
	markProbed(state, probeType)
}

// markProbed records that a probe was just executed.
func markProbed(state *ContainerProbeState, probeType string) {
	if state.LastProbeTime == nil {
		state.LastProbeTime = make(map[string]time.Time)
	}

	state.LastProbeTime[probeType] = time.Now()
}

// EvalStartup evaluates a startup probe result.
// Returns true if the container should be restarted (failure threshold exceeded).
func EvalStartup(state *ContainerProbeState, probe *corev1.Probe, result ProbeResult) (restart bool) {
	if result == ProbeSuccess {
		state.StartupPassed = true
		state.StartupFailCount = 0
		return false

	}

	state.StartupFailCount++
	threshold := probe.FailureThreshold
	if threshold <= 0 {
		threshold = 3
	}

	return state.StartupFailCount >= threshold
}

// EvalLiveness evaluates a liveness probe result.
// Returns true if the container should be restarted.
func EvalLiveness(state *ContainerProbeState, probe *corev1.Probe, result ProbeResult) (restart bool) {
	if result == ProbeSuccess {
		state.LiveFailCount = 0

		return false
	}

	state.LiveFailCount++
	threshold := probe.FailureThreshold
	if threshold <= 0 {
		threshold = 3
	}

	return state.LiveFailCount >= threshold
}

// EvalReadiness evaluates a readiness probe result and updates state.Ready.
func EvalReadiness(state *ContainerProbeState, probe *corev1.Probe, result ProbeResult) {
	if result == ProbeSuccess {
		state.ReadyFailCount = 0
		state.ReadySuccessCount++
		successThreshold := probe.SuccessThreshold
		if successThreshold <= 0 {
			successThreshold = 1
		}
		if state.ReadySuccessCount >= successThreshold {
			state.Ready = true
		}

		return
	}
	state.ReadySuccessCount = 0
	state.ReadyFailCount++
	failThreshold := probe.FailureThreshold
	if failThreshold <= 0 {
		failThreshold = 3
	}
	if state.ReadyFailCount >= failThreshold {
		state.Ready = false
	}
}

// --- noopAttachIO ---

// noopAttachIO implements api.AttachIO for exec probes.
// Discards all output, provides no input, and reports no TTY.
type noopAttachIO struct{}

func (n *noopAttachIO) Stdin() io.Reader            { return nil }
func (n *noopAttachIO) Stdout() io.WriteCloser      { return discardWriteCloser{} }
func (n *noopAttachIO) Stderr() io.WriteCloser      { return discardWriteCloser{} }
func (n *noopAttachIO) TTY() bool                   { return false }
func (n *noopAttachIO) Resize() <-chan api.TermSize { return nil }

// discardWriteCloser writes to io.Discard and Close is a no-op.
type discardWriteCloser struct{}

func (discardWriteCloser) Write(p []byte) (int, error) { return len(p), nil }
func (discardWriteCloser) Close() error                { return nil }

// Ensure interface compliance.
var _ api.AttachIO = (*noopAttachIO)(nil)
