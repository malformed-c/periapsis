package horizon

// Horizon is the Kubernetes API status writer actor.
//
// It receives StatusIntents from Foci and performs the actual k8s API
// UpdateStatus call. This is the ONLY component that touches k8s types
// in the Focus -> Syzygy -> Horizon pipeline.
//
// Horizon owns the mapping from flat types (foci.FocusSnapshot) to
// k8s types (corev1.PodStatus). Focus is pure state machine, Horizon
// is pure k8s serialization.
//
// During migration, Horizon also accepts legacy *corev1.Pod writes
// via SendLegacyPod. This path will be removed once all status flows
// through Focus -> StatusIntent.

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/malformed-c/periapsis/internal/foci"
	"github.com/malformed-c/periapsis/internal/types"
	"github.com/malformed-c/periapsis/node"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// statusWrite is the internal type for Horizon's inbox.
// It unifies the new StatusIntent path and the legacy pod path.
type statusWrite struct {
	intent foci.StatusIntent
	legacy *corev1.Pod // set only for legacy bypass writes
}

type Horizon struct {
	inbox  chan statusWrite
	mu     sync.RWMutex
	closed bool

	logger *slog.Logger

	ps     *node.PodStore
	client kubernetes.Interface
}

type HorizonDeps struct {
	Logger *slog.Logger

	Ps     *node.PodStore
	Client kubernetes.Interface
}

func NewHorizon(deps HorizonDeps) *Horizon {
	return &Horizon{
		inbox: make(chan statusWrite, 1024),

		logger: deps.Logger,

		ps:     deps.Ps,
		client: deps.Client,
	}
}

func (h *Horizon) Run(ctx context.Context, workerCount uint8) {
	wg := sync.WaitGroup{}

	for i := uint8(0); i < workerCount; i++ {
		wg.Go(func() {
			for {
				select {
				case sw, ok := <-h.inbox:
					if !ok {
						return
					}
					h.processWrite(ctx, sw)
				case <-ctx.Done():
					return
				}
			}
		})
	}

	<-ctx.Done()
	wg.Wait()

	h.close()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	for sw := range h.inbox {
		h.processWrite(shutdownCtx, sw)
	}
}

// WriteStatus implements foci.StatusWriter.
// It enqueues a StatusIntent for the k8s API write.
func (h *Horizon) WriteStatus(intent foci.StatusIntent) {
	if !h.trySend(statusWrite{intent: intent}) {
		h.logger.Debug("status intent dropped (inbox full)",
			"uid", intent.Snapshot.UID)
	}
}

// SendLegacyPod enqueues a *corev1.Pod for a direct status write.
// This is the migration path for the legacy PodStatusFact bypass.
// TODO: Remove once all status flows through Focus -> StatusIntent.
func (h *Horizon) SendLegacyPod(pod *corev1.Pod) {
	if !h.trySend(statusWrite{legacy: pod}) {
		h.logger.Debug("legacy pod write dropped (inbox full)",
			"uid", string(pod.UID))
	}
}

func (h *Horizon) trySend(sw statusWrite) bool {
	h.mu.RLock()
	closed := h.closed
	h.mu.RUnlock()

	if closed {
		return false
	}

	defer func() {
		if recover() != nil {
			// inbox full
		}
	}()

	h.inbox <- sw
	return true
}

func (h *Horizon) close() {
	h.mu.Lock()
	defer h.mu.Unlock()

	if !h.closed {
		h.closed = true
		close(h.inbox)
	}
}

// processWrite dispatches to the appropriate handler.
func (h *Horizon) processWrite(ctx context.Context, sw statusWrite) {
	if sw.legacy != nil {
		h.processLegacyPod(ctx, sw.legacy)
		return
	}
	h.processIntent(ctx, sw.intent)
}

// processIntent converts a flat StatusIntent to k8s types and writes to the API.
func (h *Horizon) processIntent(ctx context.Context, intent foci.StatusIntent) {
	snapshot := intent.Snapshot

	podStatus := snapshotToPodStatus(snapshot)

	// Get the current pod from PodStore for the full object.
	pod := h.ps.GetPodCopy(snapshot.UID)
	if pod == nil {
		h.logger.Warn("pod not found for status write", "uid", snapshot.UID, "pod", snapshot.PodName)
		return
	}

	updated := pod.DeepCopy()
	podStatus.DeepCopyInto(&updated.Status)

	h.writePodStatus(ctx, updated)
}

// processLegacyPod handles the legacy bypass path.
// Writes a pre-built *corev1.Pod directly to the k8s API.
// TODO: Remove once all status flows through Focus -> StatusIntent.
func (h *Horizon) processLegacyPod(ctx context.Context, pod *corev1.Pod) {
	h.writePodStatus(ctx, pod)
}

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

		// UID guard - if the pod was replaced (same name, new UID), drop it.
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

// ─── K8s Mapping Functions ─────────────────────────────────────────────

// snapshotToPodStatus maps a flat FocusSnapshot to a corev1.PodStatus.
// This is the ONLY place in the Focus/Syzygy/Horizon pipeline that
// creates k8s types from flat types.
func snapshotToPodStatus(s foci.FocusSnapshot) corev1.PodStatus {
	containerStatuses := make([]corev1.ContainerStatus, 0, len(s.Containers))

	for _, cs := range s.Containers {
		containerStatuses = append(containerStatuses, corev1.ContainerStatus{
			Name:         cs.Name,
			Image:        cs.Image,
			Ready:        cs.Ready,
			RestartCount: cs.RestartCount,
			State:        containerStateToK8s(cs.State),
		})
	}

	readyCondition := corev1.ConditionFalse
	if s.AllReady {
		readyCondition = corev1.ConditionTrue
	}

	return corev1.PodStatus{
		Phase:  phaseToK8s(s.Phase),
		HostIP: s.PodIP,
		PodIP:  s.PodIP,
		Conditions: []corev1.PodCondition{{
			Type:   corev1.PodReady,
			Status: readyCondition,
		}},
		ContainerStatuses: containerStatuses,
	}
}

// containerStateToK8s maps a flat types.ContainerState to corev1.ContainerState.
func containerStateToK8s(s types.ContainerState) corev1.ContainerState {
	switch s.Kind {
	case types.StateWaiting:
		return corev1.ContainerState{
			Waiting: &corev1.ContainerStateWaiting{
				Reason: s.Reason,
			},
		}
	case types.StateRunning:
		startedAt := metav1.Now()
		if !s.StartedAt.IsZero() {
			startedAt = metav1.NewTime(s.StartedAt)
		}
		return corev1.ContainerState{
			Running: &corev1.ContainerStateRunning{
				StartedAt: startedAt,
			},
		}
	case types.StateTerminated:
		finishedAt := metav1.Now()
		if !s.FinishedAt.IsZero() {
			finishedAt = metav1.NewTime(s.FinishedAt)
		}
		return corev1.ContainerState{
			Terminated: &corev1.ContainerStateTerminated{
				ExitCode:   s.ExitCode,
				FinishedAt: finishedAt,
			},
		}
	default:
		return corev1.ContainerState{
			Waiting: &corev1.ContainerStateWaiting{Reason: "Unknown"},
		}
	}
}

// phaseToK8s maps a flat types.PodPhase to corev1.PodPhase.
func phaseToK8s(p types.PodPhase) corev1.PodPhase {
	switch p {
	case types.PhasePending:
		return corev1.PodPending
	case types.PhaseRunning:
		return corev1.PodRunning
	case types.PhaseSucceeded:
		return corev1.PodSucceeded
	case types.PhaseFailed:
		return corev1.PodFailed
	default:
		return corev1.PodUnknown
	}
}

// PodPhaseToFlat maps a corev1.PodPhase to a flat types.PodPhase.
// Used by Syzygy's anti-entropy loop to compare PodStore phases
// against Focus-computed phases.
func PodPhaseToFlat(p corev1.PodPhase) types.PodPhase {
	switch p {
	case corev1.PodPending:
		return types.PhasePending
	case corev1.PodRunning:
		return types.PhaseRunning
	case corev1.PodSucceeded:
		return types.PhaseSucceeded
	case corev1.PodFailed:
		return types.PhaseFailed
	default:
		return types.PhaseUnknown
	}
}
