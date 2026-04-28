package node

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	perigeos "github.com/malformed-c/periapsis/internal/runtime"
	"github.com/malformed-c/periapsis/node/api"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// --- Mock Runtime ---

type mockRuntime struct {
	machines []perigeos.PodMetadata
	stopped  []string
}

func (m *mockRuntime) RunMachine(_ context.Context, _ string, _ perigeos.PodConfig) error {
	return nil
}
func (m *mockRuntime) StopMachine(_ context.Context, uid, containerName string) error {
	m.stopped = append(m.stopped, uid+"/"+containerName)
	return nil
}
func (m *mockRuntime) MachineStatus(_ context.Context, _, _ string) (perigeos.MachineState, error) {
	return perigeos.StateRunning, nil
}
func (m *mockRuntime) WaitForMachineExit(_ context.Context, _, _ string, _ time.Duration) (perigeos.MachineState, error) {
	return perigeos.StateExited, nil
}
func (m *mockRuntime) ListManagedMachines(_ context.Context) ([]perigeos.PodMetadata, error) {
	return m.machines, nil
}
func (m *mockRuntime) GetLogStream(_ context.Context, _, _ string, _ api.ContainerLogOpts) (io.ReadCloser, error) {
	return io.NopCloser(nil), nil
}
func (m *mockRuntime) RunInContainer(_ context.Context, _, _ string, _ []string, _ api.AttachIO) error {
	return nil
}
func (m *mockRuntime) AttachContainer(_ context.Context, _, _ string, _ api.AttachIO) error {
	return nil
}
func (m *mockRuntime) InitPawnSlice(_ context.Context, _ perigeos.PawnSliceConfig) error {
	return nil
}
func (m *mockRuntime) CheckMachined(_ context.Context) error {
	return nil
}
func (m *mockRuntime) SubscribeEvents(_ context.Context) <-chan perigeos.UnitEvent {
	return nil
}
func (m *mockRuntime) MakeSharedMounts(_ context.Context, _, _ string, _ []perigeos.BindMount) error {
	return nil
}
func (m *mockRuntime) ResetUnit(_ context.Context, _, _ string) error {
	return nil
}
func (m *mockRuntime) CleanupStaleUnits(_ context.Context, _ map[string]bool) (int, error) {
	return 0, nil
}
func (m *mockRuntime) SliceActive(ctx context.Context) bool {
	return true
}

func (m *mockRuntime) GetContainerExitInfo(_ context.Context, _, _ string) perigeos.ContainerExitInfo {
	return perigeos.ContainerExitInfo{}
}

func (r *mockRuntime) PortForward(ctx context.Context, podUID, containerName string, port int32, stream io.ReadWriteCloser) error {
	return nil
}

// --- Mock Network ---

type mockNetwork struct {
	tornDown []string
}

func (m *mockNetwork) Setup(_ context.Context, podUID, _, _, _ string) (string, string, error) {
	return "/var/run/netns/" + podUID, "10.88.0.2", nil
}
func (m *mockNetwork) Teardown(_ context.Context, podUID, _, _ string) error {
	m.tornDown = append(m.tornDown, podUID)
	return nil
}

// --- Mock Pod Lister ---

type mockPodLister struct {
	pods []*corev1.Pod
}

func (m *mockPodLister) List(_ labels.Selector) ([]*corev1.Pod, error) {
	return m.pods, nil
}
func (m *mockPodLister) Get(name string) (*corev1.Pod, error) {
	for _, p := range m.pods {
		if p.Name == name {
			return p, nil
		}
	}
	return nil, nil
}

// --- Helpers ---

func newTestReconciler(rt *mockRuntime, nm *mockNetwork, lister *mockPodLister) *TestReconciler {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewReconcilerForTest(rt, nm, lister, logger)
}

// --- Tests ---

func TestReconciler_OrphanMachineIsStopped(t *testing.T) {
	rt := &mockRuntime{
		machines: []perigeos.PodMetadata{
			{UID: "orphan-uid", Name: "orphan", Namespace: "default", ContainerName: "main"},
		},
	}
	nm := &mockNetwork{}
	r := newTestReconciler(rt, nm, &mockPodLister{})

	r.RunOnce(context.Background())

	if len(rt.stopped) != 1 || rt.stopped[0] != "orphan-uid/main" {
		t.Errorf("expected [orphan-uid/main] stopped, got %v", rt.stopped)
	}
	if len(nm.tornDown) != 1 || nm.tornDown[0] != "orphan-uid" {
		t.Errorf("expected network teardown for orphan-uid, got %v", nm.tornDown)
	}
}

func TestReconciler_InFlightMachineIsSkipped(t *testing.T) {
	rt := &mockRuntime{
		machines: []perigeos.PodMetadata{
			{UID: "inflight-uid", Name: "mypod", Namespace: "default", ContainerName: "main"},
		},
	}
	nm := &mockNetwork{}
	r := newTestReconciler(rt, nm, &mockPodLister{})
	r.MarkInFlight("inflight-uid")

	r.RunOnce(context.Background())

	if len(rt.stopped) != 0 {
		t.Errorf("in-flight machine should not be stopped, got %v", rt.stopped)
	}
}

func TestReconciler_KnownPodIsSkipped(t *testing.T) {
	rt := &mockRuntime{
		machines: []perigeos.PodMetadata{
			{UID: "known-uid", Name: "mypod", Namespace: "default", ContainerName: "main"},
		},
	}
	nm := &mockNetwork{}
	r := newTestReconciler(rt, nm, &mockPodLister{})
	r.MarkHasPod("known-uid")

	r.RunOnce(context.Background())

	if len(rt.stopped) != 0 {
		t.Errorf("known pod should not be stopped, got %v", rt.stopped)
	}
}

func TestReconciler_K8sPodListerMatchSkips(t *testing.T) {
	rt := &mockRuntime{
		machines: []perigeos.PodMetadata{
			{UID: "k8s-uid", Name: "mypod", Namespace: "default", ContainerName: "main"},
		},
	}
	nm := &mockNetwork{}
	lister := &mockPodLister{
		pods: []*corev1.Pod{makePod("k8s-uid", "default", "mypod", "200m", "100Mi")},
	}
	r := newTestReconciler(rt, nm, lister)

	r.RunOnce(context.Background())

	if len(rt.stopped) != 0 {
		t.Errorf("pod in K8s lister should not be stopped, got %v", rt.stopped)
	}
	// Forward reconciler: should have requested re-sync for the pod that
	// exists in K8s but isn't tracked by Gambit.
	if len(r.SyncRequests) != 1 || r.SyncRequests[0] != "default/mypod" {
		t.Errorf("expected sync request for default/mypod, got %v", r.SyncRequests)
	}
}

func TestReconciler_MultipleOrphans(t *testing.T) {
	rt := &mockRuntime{
		machines: []perigeos.PodMetadata{
			{UID: "uid-1", ContainerName: "app"},
			{UID: "uid-2", ContainerName: "sidecar"},
			{UID: "uid-3", ContainerName: "app"},
		},
	}
	nm := &mockNetwork{}
	r := newTestReconciler(rt, nm, &mockPodLister{})

	r.RunOnce(context.Background())

	if len(rt.stopped) != 3 {
		t.Errorf("expected 3 stopped, got %d: %v", len(rt.stopped), rt.stopped)
	}
}

func TestReconciler_MixedOrphanAndKnown(t *testing.T) {
	rt := &mockRuntime{
		machines: []perigeos.PodMetadata{
			{UID: "orphan-1", ContainerName: "main"},
			{UID: "known-1", ContainerName: "main"},
			{UID: "orphan-2", ContainerName: "sidecar"},
		},
	}
	nm := &mockNetwork{}
	r := newTestReconciler(rt, nm, &mockPodLister{})
	r.MarkHasPod("known-1")

	r.RunOnce(context.Background())

	if len(rt.stopped) != 2 {
		t.Errorf("expected 2 orphans stopped, got %d: %v", len(rt.stopped), rt.stopped)
	}
	for _, s := range rt.stopped {
		if s == "known-1/main" {
			t.Error("known-1 should not have been stopped")
		}
	}
}
