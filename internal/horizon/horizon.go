package horizon

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/malformed-c/periapsis/node"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type Horizon struct {
	inbox  chan *corev1.Pod
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
		inbox: make(chan *corev1.Pod, 1024),

		logger: deps.Logger,

		ps:     deps.Ps,
		client: deps.Client,
	}
}

func (h *Horizon) Run(ctx context.Context, workerCount uint8) {
	wg := sync.WaitGroup{}

	// Start a worker pool
	for i := uint8(0); i < workerCount; i++ {
		wg.Go(func() {
			for {
				select {
				case pod, ok := <-h.inbox:
					if !ok {
						return
					}

					h.processPod(ctx, pod)

				case <-ctx.Done():
					return
				}
			}
		})
	}

	// Block on context, then wait for workers to finish
	<-ctx.Done()
	wg.Wait()

	h.close()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	for pod := range h.inbox {
		h.processPod(shutdownCtx, pod)
	}
}

func (h *Horizon) Send(pod *corev1.Pod) (ok bool) {
	h.mu.RLock()
	closed := h.closed
	h.mu.RUnlock()

	if closed {
		return false
	}

	defer func() {
		if recover() != nil {
			ok = false
		}
	}()

	h.inbox <- pod

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

func (h *Horizon) processPod(ctx context.Context, pod *corev1.Pod) error {
	const maxRetries = 5

	for attempt := range maxRetries {
		// GET the current pod to obtain a fresh ResourceVersion.
		// UpdateStatus rejects writes without a matching RV.
		current, err := h.client.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
		if err != nil {
			if k8serrors.IsNotFound(err) {
				return nil // pod deleted, nothing to update
			}
			h.logger.Warn("horizon: GET pod failed", "pod", pod.Name, "err", err)
			return err
		}

		// UID guard — if the pod was replaced (same name, new UID), the status
		// we computed is for the old pod. Drop it.
		if current.UID != pod.UID {
			h.logger.Debug("horizon: pod UID mismatch, dropping stale status",
				"pod", pod.Name, "ourUID", pod.UID, "k8sUID", current.UID)
			return nil
		}

		// Stamp our status onto the current object so ResourceVersion is correct.
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

// WritePodStatus is the HorizonWriter interface implementation.
// It enqueues the pod for a k8s API status write, same as Send.
// This alias exists so Focus can depend on the HorizonWriter interface
// instead of importing the horizon package directly.
func (h *Horizon) WritePodStatus(pod *corev1.Pod) {
	h.Send(pod)
}
