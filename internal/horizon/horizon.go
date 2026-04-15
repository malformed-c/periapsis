package horizon

// Horizon is the Kubernetes API command executor.
//
// It receives Effect commands via a channel and executes them against
// the Kubernetes API. Horizon is a pure executor — it holds no pod state
// and has no dependency on PodStore. All the information needed to
// execute a command is carried in the Effect value itself.
//
// Design principles:
//   - No PodStore dependency — Horizon is decoupled from pod state
//   - Command channel — all work arrives as types.Effect values
//   - Worker pool — configurable concurrency for API calls
//   - Value-typed commands — StatusUpdate carries flat PodStatusPayload,
//     no DeepCopy needed on the hot path
//   - UID guard — every status write verifies the pod UID hasn't changed
//     to avoid clobbering a replacement pod

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

// Horizon executes Effect commands against the Kubernetes API.
type Horizon struct {
	inbox  chan types.Effect
	mu     sync.RWMutex
	closed bool

	logger *slog.Logger
	client kubernetes.Interface

	// recordEvent records a Kubernetes event for a pod.
	// Provided as a function so Horizon doesn't need a PodStore dependency
	// for looking up the pod object (the EventRecorder needs *corev1.Pod).
	recordEvent func(uid string, eventType, reason, message string)

	// setPodPhase sets the pod phase in the PodStore.
	// Provided as a function so Horizon doesn't import PodStore.
	setPodPhase func(uid string, phase corev1.PodPhase)

	// initRestartState initializes restart/probe tracking for a new pod.
	// Provided as a function so Horizon doesn't import PodStore.
	initRestartState func(uid string, pod *corev1.Pod)

	// resetUnit cleans up a dead/failed systemd unit.
	// Provided as a function so Horizon doesn't import the runtime.
	resetUnit func(ctx context.Context, uid, containerName string)

	// restartContainer launches a container restart.
	// Provided as a function so Horizon doesn't import Gambit.
	restartContainer func(ctx context.Context, uid, namespace, podName, containerName string, restartCount int32, backoff time.Duration)

	// persistPodState persists pod state to disk.
	// Provided as a function so Horizon doesn't import the disk layer.
	persistPodState func(uid string)

	// getPodCopy retrieves a copy of the pod for UID-guarded writes.
	// This is the ONLY remaining PodStore interaction, and it's injected
	// as a function so Horizon doesn't import the node package.
	getPodCopy func(uid string) *corev1.Pod
}

// HorizonDeps holds all external dependencies for Horizon.
// All PodStore/Runtime/Gambit interactions are injected as functions
// so Horizon remains a pure executor with no package-level dependencies
// on the node package.
type HorizonDeps struct {
	Logger *slog.Logger
	Client kubernetes.Interface

	// Optional function overrides. If nil, the operation is a no-op.
	RecordEvent      func(uid string, eventType, reason, message string)
	SetPodPhase      func(uid string, phase corev1.PodPhase)
	InitRestartState func(uid string, pod *corev1.Pod)
	ResetUnit        func(ctx context.Context, uid, containerName string)
	RestartContainer func(ctx context.Context, uid, namespace, podName, containerName string, restartCount int32, backoff time.Duration)
	PersistPodState  func(uid string)
	GetPodCopy       func(uid string) *corev1.Pod
}

func NewHorizon(deps HorizonDeps) *Horizon {
	if deps.RecordEvent == nil {
		deps.RecordEvent = func(string, string, string, string) {}
	}
	if deps.SetPodPhase == nil {
		deps.SetPodPhase = func(string, corev1.PodPhase) {}
	}
	if deps.InitRestartState == nil {
		deps.InitRestartState = func(string, *corev1.Pod) {}
	}
	if deps.ResetUnit == nil {
		deps.ResetUnit = func(context.Context, string, string) {}
	}
	if deps.RestartContainer == nil {
		deps.RestartContainer = func(context.Context, string, string, string, string, int32, time.Duration) {}
	}
	if deps.PersistPodState == nil {
		deps.PersistPodState = func(string) {}
	}
	if deps.GetPodCopy == nil {
		deps.GetPodCopy = func(string) *corev1.Pod { return nil }
	}

	return &Horizon{
		inbox:            make(chan types.Effect, 1024),
		logger:           deps.Logger,
		client:           deps.Client,
		recordEvent:      deps.RecordEvent,
		setPodPhase:      deps.SetPodPhase,
		initRestartState: deps.InitRestartState,
		resetUnit:        deps.ResetUnit,
		restartContainer: deps.RestartContainer,
		persistPodState:  deps.PersistPodState,
		getPodCopy:       deps.GetPodCopy,
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
			// inbox closed
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

// executeEffect dispatches an Effect to the appropriate handler.
func (h *Horizon) executeEffect(ctx context.Context, eff types.Effect) {
	switch e := eff.(type) {
	case types.UpdateStatus:
		h.handleUpdateStatus(ctx, e)
	case types.RestartContainer:
		h.handleRestartContainer(ctx, e)
	case types.SetPodPhase:
		h.handleSetPodPhase(e)
	case types.ResetUnit:
		h.handleResetUnit(ctx, e)
	case types.RecordEvent:
		h.handleRecordEvent(e)
	case types.PersistPodState:
		h.handlePersistPodState(e)
	case types.InitRestartState:
		h.handleInitRestartState(e)
	default:
		h.logger.Warn("unknown effect type", "type", fmt.Sprintf("%T", eff))
	}
}

// --- Effect Handlers -----------------------------------------------------

// handleUpdateStatus writes a computed PodStatus to the Kubernetes API.
// The PodStatusPayload is converted to corev1.PodStatus via the foci
// conversion layer. The UID guard prevents clobbering replacement pods.
func (h *Horizon) handleUpdateStatus(ctx context.Context, eff types.UpdateStatus) {
	podStatus := foci.PodStatusPayloadToCorev1(eff.Status)

	// Get the current pod from PodStore for UID-guarded writes.
	pod := h.getPodCopy(eff.UID)
	if pod == nil {
		h.logger.Debug("pod not found for status write, dropping",
			"uid", eff.UID, "name", eff.Name)
		return
	}

	updated := pod.DeepCopy()
	podStatus.DeepCopyInto(&updated.Status)

	h.writePodStatus(ctx, updated)
}

// handleRestartContainer launches a container restart via the Gambit callback.
func (h *Horizon) handleRestartContainer(ctx context.Context, eff types.RestartContainer) {
	h.restartContainer(ctx, eff.UID, eff.Namespace, eff.PodName, eff.ContainerName, eff.RestartCount, eff.Backoff)
}

// handleSetPodPhase updates the PodStore's phase map.
func (h *Horizon) handleSetPodPhase(eff types.SetPodPhase) {
	h.setPodPhase(eff.UID, eff.Phase)
}

// handleResetUnit cleans up a dead/failed systemd unit.
func (h *Horizon) handleResetUnit(ctx context.Context, eff types.ResetUnit) {
	h.resetUnit(ctx, eff.UID, eff.ContainerName)
}

// handleRecordEvent emits a Kubernetes event.
func (h *Horizon) handleRecordEvent(eff types.RecordEvent) {
	h.recordEvent(eff.UID, eff.EventType, eff.Reason, eff.Message)
}

// handlePersistPodState persists pod state to disk.
func (h *Horizon) handlePersistPodState(eff types.PersistPodState) {
	h.persistPodState(eff.UID)
}

// handleInitRestartState initializes restart/probe tracking for a new pod.
func (h *Horizon) handleInitRestartState(eff types.InitRestartState) {
	h.initRestartState(eff.UID, eff.Pod)
}

// --- K8s API Write -------------------------------------------------------

// writePodStatus performs the actual k8s API status update.
// GET + UpdateStatus with UID guard and conflict retry.
func (h *Horizon) writePodStatus(ctx context.Context, pod *corev1.Pod) error {
	const maxRetries = 5

	for attempt := range maxRetries {
		current, err := h.client.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
		if err != nil {
			if k8serrors.IsNotFound(err) {
				return nil // pod deleted
			}
			h.logger.Warn("horizon: GET pod failed", "pod", pod.Name, "err", err)
			return err
		}

		// UID guard — if the pod was replaced (same name, new UID), drop it.
		if current.UID != pod.UID {
			h.logger.Debug("horizon: pod UID mismatch, dropping stale status",
				"pod", pod.Name, "ourUID", pod.UID, "k8sUID", current.UID)
			return nil
		}

		update := current.DeepCopy()
		pod.Status.DeepCopyInto(&update.Status)

		_, err = h.client.CoreV1().Pods(pod.Namespace).UpdateStatus(ctx, update, metav1.UpdateOptions{})
		if err == nil {
			return nil
		}
		if k8serrors.IsNotFound(err) {
			return nil
		}
		if k8serrors.IsConflict(err) {
			if attempt < maxRetries-1 {
				h.logger.Debug("horizon: conflict on UpdateStatus, retrying",
					"pod", pod.Name, "attempt", attempt+1)
				continue
			}
			h.logger.Warn("horizon: conflict after max retries, dropping",
				"pod", pod.Name, "attempts", maxRetries)
			return nil
		}
		h.logger.Warn("horizon: UpdateStatus failed", "pod", pod.Name, "err", err)
		return err
	}
	return nil
}

// --- Adapters ------------------------------------------------------------

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
