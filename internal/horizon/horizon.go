// Copyright (C) 2024-2026 Malformed C. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-only

package horizon

// Horizon is the Kubernetes API command executor.
//
// It receives Effect commands via a channel and executes them against
// the Kubernetes API. Horizon is a pure k8s API executor - it holds no
// pod state and has no dependency on PodStore, PodStore callbacks, or
// any local state. All information needed to execute a command is carried
// in the Effect value itself.
//
// Design principles:
//   - No PodStore dependency - zero callbacks for local state ops
//   - Command channel - all work arrives as types.Effect values
//   - Worker pool - configurable concurrency for API calls
//   - Value-typed commands - UpdateStatus carries flat PodStatusPayload,
//     no *corev1.Pod pointer, no DeepCopy needed on the hot path
//   - UID guard - every status write verifies the pod UID hasn't changed
//     to avoid clobbering a replacement pod
//
// Effects handled here (k8s API only):
//   - UpdateStatus    -> GET + UpdateStatus (with UID guard and retry)
//   - RestartContainer -> callback into Gambit
//   - ResetUnit        -> callback into runtime
//   - RecordEvent      -> Kubernetes EventRecorder
//
// Effects NOT handled here (local state, routed via Syzygy):
//   - SetPodPhase, PersistPodState, InitRestartState

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/malformed-c/periapsis/internal/foci"
	"github.com/malformed-c/periapsis/internal/types"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
)

// Horizon executes k8s API Effect commands.
type Horizon struct {
	inbox  chan types.Effect
	mu     sync.RWMutex
	closed bool

	logger *slog.Logger
	client kubernetes.Interface

	// recordEvent records a Kubernetes event for a pod.
	// Injected so Horizon doesn't need a PodStore dependency for object lookup.
	recordEvent func(uid string, eventType, reason, message string)

	// resetUnit cleans up a dead/failed systemd unit.
	resetUnit func(ctx context.Context, uid, containerName string)

	// restartContainer launches a container restart via Gambit.
	restartContainer func(ctx context.Context, uid, namespace, podName, containerName string, restartCount int32, backoff time.Duration)
}

// HorizonConfig holds all external dependencies for Horizon.
// Only k8s-API-adjacent callbacks are included here. Local state ops
// (SetPodPhase, PersistPodState, InitRestartState) are handled by Syzygy
// directly and never reach Horizon.
type HorizonConfig struct {
	Logger *slog.Logger
	Client kubernetes.Interface

	// Optional. If nil, the operation is a no-op.
	RecordEvent      func(uid string, eventType, reason, message string)
	ResetUnit        func(ctx context.Context, uid, containerName string)
	RestartContainer func(ctx context.Context, uid, namespace, podName, containerName string, restartCount int32, backoff time.Duration)
}

func NewHorizon(deps HorizonConfig) *Horizon {
	if deps.RecordEvent == nil {
		deps.RecordEvent = func(string, string, string, string) {}
	}
	if deps.ResetUnit == nil {
		deps.ResetUnit = func(context.Context, string, string) {}
	}
	if deps.RestartContainer == nil {
		deps.RestartContainer = func(context.Context, string, string, string, string, int32, time.Duration) {}
	}

	return &Horizon{
		inbox:            make(chan types.Effect, 1024),
		logger:           deps.Logger,
		client:           deps.Client,
		recordEvent:      deps.RecordEvent,
		resetUnit:        deps.ResetUnit,
		restartContainer: deps.RestartContainer,
	}
}

// Run starts the Horizon worker pool. It blocks until the context is cancelled.
func (h *Horizon) Run(ctx context.Context, workerCount uint8) {
	var wg sync.WaitGroup

	for i := uint8(0); i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case eff, ok := <-h.inbox:
					if !ok {
						return
					}
					h.executeEffect(ctx, eff)
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	<-ctx.Done()
	wg.Wait()

	h.close()

	// Drain remaining effects during shutdown.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	for eff := range h.inbox {
		h.executeEffect(shutdownCtx, eff)
	}
}

// Send enqueues an Effect for execution. Non-blocking; returns false if closed or full.
func (h *Horizon) Send(eff types.Effect) bool {
	h.mu.RLock()
	closed := h.closed
	h.mu.RUnlock()

	if closed {
		return false
	}

	defer func() {
		if recover() != nil {
			// inbox closed between the check and the send
		}
	}()

	select {
	case h.inbox <- eff:
		return true
	default:
		h.logger.Warn("horizon inbox full, dropping effect",
			"type", fmt.Sprintf("%T", eff))
		return false
	}
}

func (h *Horizon) close() {
	h.mu.Lock()
	defer h.mu.Unlock()

	if !h.closed {
		h.closed = true
		close(h.inbox)
	}
}

// executeEffect dispatches a k8s-API Effect to the appropriate handler.
// Local state effects (SetPodPhase, PersistPodState, InitRestartState)
// are handled by Syzygy and must never reach Horizon - warn and drop.
func (h *Horizon) executeEffect(ctx context.Context, eff types.Effect) {
	switch e := eff.(type) {
	case types.UpdateStatus:
		h.handleUpdateStatus(ctx, e)
	case types.RestartContainer:
		h.handleRestartContainer(ctx, e)
	case types.ResetUnit:
		h.handleResetUnit(ctx, e)
	case types.RecordEvent:
		h.handleRecordEvent(e)
	default:
		h.logger.Warn("horizon: received non-k8s effect - should be handled by Syzygy",
			"type", fmt.Sprintf("%T", eff))
	}
}

// --- Effect Handlers ---

// handleUpdateStatus writes a computed PodStatus to the Kubernetes API.
// All fields needed for the write come from the UpdateStatus value itself -
// no PodStore lookup required.
func (h *Horizon) handleUpdateStatus(ctx context.Context, eff types.UpdateStatus) {
	podStatus := foci.PodStatusPayloadToCorev1(eff.Status)
	h.writePodStatus(ctx, eff.UID, eff.Namespace, eff.Name, podStatus)
}

// handleRestartContainer launches a container restart via the Gambit callback.
func (h *Horizon) handleRestartContainer(ctx context.Context, eff types.RestartContainer) {
	h.restartContainer(ctx, eff.UID, eff.Namespace, eff.PodName, eff.ContainerName, eff.RestartCount, eff.Backoff)
}

// handleResetUnit cleans up a dead/failed systemd unit.
func (h *Horizon) handleResetUnit(ctx context.Context, eff types.ResetUnit) {
	h.resetUnit(ctx, eff.UID, eff.ContainerName)
}

// handleRecordEvent emits a Kubernetes event.
func (h *Horizon) handleRecordEvent(eff types.RecordEvent) {
	h.recordEvent(eff.UID, eff.EventType, eff.Reason, eff.Message)
}

// --- K8s API Write ---

// writePodStatus performs the actual k8s API status update.
// GET + UpdateStatus with UID guard and conflict retry.
// Takes primitives only - no *corev1.Pod, no PodStore interaction.
func (h *Horizon) writePodStatus(ctx context.Context, uid, namespace, name string, status corev1.PodStatus) error {
	const maxRetries = 5

	for attempt := range maxRetries {
		current, err := h.client.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			if k8serrors.IsNotFound(err) {
				return nil // pod deleted
			}
			h.logger.Warn("horizon: GET pod failed", "pod", name, "err", err)
			return err
		}

		// UID guard - if the pod was replaced (same name, new UID), drop it.
		if string(current.UID) != uid {
			h.logger.Debug("horizon: pod UID mismatch, dropping stale status",
				"pod", name, "ourUID", uid, "k8sUID", current.UID)
			return nil
		}

		update := current.DeepCopy()
		status.DeepCopyInto(&update.Status)

		_, err = h.client.CoreV1().Pods(namespace).UpdateStatus(ctx, update, metav1.UpdateOptions{})
		if err == nil {
			return nil
		}
		if k8serrors.IsNotFound(err) {
			return nil
		}
		if k8serrors.IsConflict(err) {
			if attempt < maxRetries-1 {
				h.logger.Debug("horizon: conflict on UpdateStatus, retrying",
					"pod", name, "attempt", attempt+1)
				continue
			}
			h.logger.Warn("horizon: conflict after max retries, dropping",
				"pod", name, "attempts", maxRetries)
			return nil
		}
		h.logger.Warn("horizon: UpdateStatus failed", "pod", name, "err", err)
		return err
	}
	return nil
}

// --- Adapters ---

// EventRecorderAdapter creates a RecordEvent function from a Kubernetes
// EventRecorder and a pod lookup function. This decouples Horizon from
// both the node package and PodStore.
func EventRecorderAdapter(recorder record.EventRecorder, getPod func(uid string) *corev1.Pod) func(uid string, eventType, reason, message string) {
	return func(uid string, eventType, reason, message string) {
		pod := getPod(uid)
		if pod == nil {
			return
		}
		recorder.Eventf(pod, eventType, reason, "%s", message)
	}
}
