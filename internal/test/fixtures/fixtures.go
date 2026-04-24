package fixtures

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	perigeos "github.com/malformed-c/periapsis/internal/runtime"
	"github.com/malformed-c/periapsis/node/api"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// RuntimeFixture is a mock perigeos.Runtime that records calls.
type RuntimeFixture struct {
	Machines []perigeos.PodMetadata
	Stopped  []string
	Status   perigeos.MachineState
	ExitCode int32
}

func (m *RuntimeFixture) RunMachine(_ context.Context, _ string, _ perigeos.PodConfig) error {
	return nil
}
func (m *RuntimeFixture) StopMachine(_ context.Context, uid, containerName string) error {
	m.Stopped = append(m.Stopped, uid+"/"+containerName)
	return nil
}
func (m *RuntimeFixture) MachineStatus(_ context.Context, _, _ string) (perigeos.MachineState, error) {
	if m.Status == "" {
		return perigeos.StateRunning, nil
	}
	return m.Status, nil
}
func (m *RuntimeFixture) MachineExitCode(_ context.Context, _, _ string) int32 {
	return m.ExitCode
}
func (m *RuntimeFixture) WaitForMachineExit(_ context.Context, _, _ string, _ time.Duration) (perigeos.MachineState, error) {
	return perigeos.StateExited, nil
}
func (m *RuntimeFixture) ListManagedMachines(_ context.Context) ([]perigeos.PodMetadata, error) {
	return m.Machines, nil
}
func (m *RuntimeFixture) GetLogStream(_ context.Context, _, _ string, _ api.ContainerLogOpts) (io.ReadCloser, error) {
	return io.NopCloser(nil), nil
}
func (m *RuntimeFixture) RunInContainer(_ context.Context, _, _ string, _ []string, _ api.AttachIO) error {
	return nil
}
func (m *RuntimeFixture) AttachContainer(_ context.Context, _, _ string, _ api.AttachIO) error {
	return nil
}
func (m *RuntimeFixture) InitPawnSlice(_ context.Context, _ perigeos.PawnSliceConfig) error {
	return nil
}
func (m *RuntimeFixture) CheckMachined(_ context.Context) error {
	return nil
}
func (m *RuntimeFixture) SubscribeEvents(_ context.Context) <-chan perigeos.UnitEvent {
	return nil
}
func (m *RuntimeFixture) MakeSharedMounts(_ context.Context, _, _ string, _ []perigeos.BindMount) error {
	return nil
}
func (m *RuntimeFixture) ResetUnit(_ context.Context, _, _ string) error {
	return nil
}
func (m *RuntimeFixture) CleanupStaleUnits(_ context.Context, _ map[string]bool) (int, error) {
	return 0, nil
}
func (m *RuntimeFixture) SliceActive(ctx context.Context) bool {
	return true
}
func (m *RuntimeFixture) PortForward(ctx context.Context, podUID, containerName string, port int32, stream io.ReadWriteCloser) error {
	return nil
}

// NetworkFixture is a mock network.NetworkManager that records calls.
type NetworkFixture struct {
	TornDown []string
}

func (m *NetworkFixture) Setup(_ context.Context, podUID, _, _, _ string) (string, string, error) {
	return "/var/run/netns/" + podUID, "10.88.0.2", nil
}
func (m *NetworkFixture) Teardown(_ context.Context, podUID, _, _ string) error {
	m.TornDown = append(m.TornDown, podUID)
	return nil
}

// PodListerFixture is a mock pod lister.
type PodListerFixture struct {
	Pods []*corev1.Pod
}

func (m *PodListerFixture) List(_ labels.Selector) ([]*corev1.Pod, error) {
	return m.Pods, nil
}
func (m *PodListerFixture) Get(name string) (*corev1.Pod, error) {
	for _, p := range m.Pods {
		if p.Name == name {
			return p, nil
		}
	}
	return nil, nil
}

// ProviderFixture is a mock that implements both PodProvider and NodeProvider.
type ProviderFixture struct {
	Pods sync.Map

	Creates          *WaitableInt
	Updates          *WaitableInt
	Deletes          *WaitableInt
	AttemptedDeletes *WaitableInt

	ErrorOnDelete error

	PodNotifier  func(*corev1.Pod)
	NodeNotifier func(*corev1.Node)

	// Node provider fields
	CustomPingFunction func(context.Context) error
	LastPingTime       time.Time
	MaxPingInterval    time.Duration
	PingMu             sync.Mutex

	StatusHandlers []func(*corev1.Node)
}

func NewProviderFixture() *ProviderFixture {
	return &ProviderFixture{
		Creates:          NewWaitableInt(),
		Updates:          NewWaitableInt(),
		Deletes:          NewWaitableInt(),
		AttemptedDeletes: NewWaitableInt(),
	}
}

func (p *ProviderFixture) CreatePod(ctx context.Context, pod *corev1.Pod) error {
	p.Creates.Increment()
	key := pod.Namespace + "/" + pod.Name

	if pod.Status.Phase == "" {
		now := metav1.NewTime(time.Now())
		pod.Status = corev1.PodStatus{
			Phase:     corev1.PodRunning,
			HostIP:    "1.2.3.4",
			PodIP:     "5.6.7.8",
			StartTime: &now,
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodInitialized,
					Status: corev1.ConditionTrue,
				},
				{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				},
				{
					Type:   corev1.PodScheduled,
					Status: corev1.ConditionTrue,
				},
			},
		}

		for _, container := range pod.Spec.Containers {
			pod.Status.ContainerStatuses = append(pod.Status.ContainerStatuses, corev1.ContainerStatus{
				Name:         container.Name,
				Image:        container.Image,
				Ready:        true,
				RestartCount: 0,
				State: corev1.ContainerState{
					Running: &corev1.ContainerStateRunning{
						StartedAt: now,
					},
				},
			})
		}
	}

	p.Pods.Store(key, pod.DeepCopy())
	if p.PodNotifier != nil {
		p.PodNotifier(pod)
	}
	return nil
}

func (p *ProviderFixture) UpdatePod(ctx context.Context, pod *corev1.Pod) error {
	p.Updates.Increment()
	p.Pods.Store(pod.Namespace+"/"+pod.Name, pod.DeepCopy())
	if p.PodNotifier != nil {
		p.PodNotifier(pod)
	}
	return nil
}

func (p *ProviderFixture) DeletePod(ctx context.Context, pod *corev1.Pod) error {
	p.AttemptedDeletes.Increment()
	key := pod.Namespace + "/" + pod.Name

	if p.ErrorOnDelete != nil {
		return p.ErrorOnDelete
	}

	p.Deletes.Increment()
	if _, exists := p.Pods.Load(key); !exists {
		return fmt.Errorf("pod not found")
	}

	now := metav1.Now()
	pod.Status.Phase = corev1.PodSucceeded
	for idx := range pod.Status.ContainerStatuses {
		pod.Status.ContainerStatuses[idx].Ready = false
		pod.Status.ContainerStatuses[idx].State = corev1.ContainerState{
			Terminated: &corev1.ContainerStateTerminated{
				FinishedAt: now,
				Reason:     "MockProviderPodContainerDeleted",
			},
		}
	}

	if p.PodNotifier != nil {
		p.PodNotifier(pod)
	}
	p.Pods.Delete(key)
	return nil
}

func (p *ProviderFixture) GetPod(ctx context.Context, namespace, name string) (*corev1.Pod, error) {
	if pod, ok := p.Pods.Load(namespace+"/"+name); ok {
		return pod.(*corev1.Pod).DeepCopy(), nil
	}
	return nil, fmt.Errorf("pod not found")
}

func (p *ProviderFixture) GetPodStatus(ctx context.Context, namespace, name string) (*corev1.PodStatus, error) {
	pod, err := p.GetPod(ctx, namespace, name)
	if err != nil || pod == nil {
		return nil, err
	}
	return &pod.Status, nil
}

func (p *ProviderFixture) GetPods(ctx context.Context) ([]*corev1.Pod, error) {
	var pods []*corev1.Pod
	p.Pods.Range(func(_, value any) bool {
		pods = append(pods, value.(*corev1.Pod).DeepCopy())
		return true
	})
	return pods, nil
}

func (p *ProviderFixture) NotifyPods(ctx context.Context, f func(*corev1.Pod)) {
	p.PodNotifier = f
}

func (p *ProviderFixture) Ping(ctx context.Context) error {
	if p.CustomPingFunction != nil {
		return p.CustomPingFunction(ctx)
	}

	now := time.Now()
	p.PingMu.Lock()
	defer p.PingMu.Unlock()

	if !p.LastPingTime.IsZero() {
		interval := now.Sub(p.LastPingTime)
		if interval > p.MaxPingInterval {
			p.MaxPingInterval = interval
		}
	}
	p.LastPingTime = now
	return nil
}

func (p *ProviderFixture) NotifyNodeStatus(ctx context.Context, f func(*corev1.Node)) {
	p.NodeNotifier = f
}

func (p *ProviderFixture) TriggerStatusUpdate(n *corev1.Node) {
	for _, h := range p.StatusHandlers {
		h(n)
	}
	if p.NodeNotifier != nil {
		p.NodeNotifier(n)
	}
}

// WaitableInt is a thread-safe integer that can be waited on.
type WaitableInt struct {
	mu sync.Mutex
	v  int
	ch chan struct{}
}

func NewWaitableInt() *WaitableInt {
	return &WaitableInt{ch: make(chan struct{}, 1)}
}

func (w *WaitableInt) Increment() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.v++
	select {
	case w.ch <- struct{}{}:
	default:
	}
}

func (w *WaitableInt) Value() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.v
}

func (w *WaitableInt) Until(ctx context.Context, f func(int) bool) error {
	for {
		w.mu.Lock()
		v := w.v
		w.mu.Unlock()
		if f(v) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-w.ch:
		}
	}
}
