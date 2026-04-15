package horizon

import (
        "context"
        "log/slog"
        "sync"
        "time"

        "github.com/malformed-c/periapsis/node"
        corev1 "k8s.io/api/core/v1"
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

        return nil
}

// WritePodStatus is the HorizonWriter interface implementation.
// It enqueues the pod for a k8s API status write, same as Send.
// This alias exists so Focus can depend on the HorizonWriter interface
// instead of importing the horizon package directly.
func (h *Horizon) WritePodStatus(pod *corev1.Pod) {
        h.Send(pod)
}
