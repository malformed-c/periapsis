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
        metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
        "k8s.io/apimachinery/pkg/labels"
        "k8s.io/apimachinery/pkg/types"
)

// --- Mock Runtime ---

type mockRuntimeForGhosts struct {
        machines []perigeos.PodMetadata
        stopped  []string
}

func (m *mockRuntimeForGhosts) RunMachine(_ context.Context, _ string, _ perigeos.PodConfig) error {
        return nil
}
func (m *mockRuntimeForGhosts) StopMachine(_ context.Context, uid, containerName string) error {
        m.stopped = append(m.stopped, uid+"/"+containerName)
        return nil
}
func (m *mockRuntimeForGhosts) MachineStatus(_ context.Context, _, _ string) (perigeos.MachineState, error) {
        return perigeos.StateRunning, nil
}
func (m *mockRuntimeForGhosts) MachineExitCode(_ context.Context, _, _ string) int32 {
        return 0
}
func (m *mockRuntimeForGhosts) WaitForMachineExit(_ context.Context, _, _ string, _ time.Duration) (perigeos.MachineState, error) {
        return perigeos.StateExited, nil
}
func (m *mockRuntimeForGhosts) ListManagedMachines(_ context.Context) ([]perigeos.PodMetadata, error) {
        return m.machines, nil
}
func (m *mockRuntimeForGhosts) GetLogStream(_ context.Context, _, _ string, _ api.ContainerLogOpts) (io.ReadCloser, error) {
        return io.NopCloser(nil), nil
}
func (m *mockRuntimeForGhosts) RunInContainer(_ context.Context, _, _ string, _ []string, _ api.AttachIO) error {
        return nil
}
func (m *mockRuntimeForGhosts) AttachContainer(_ context.Context, _, _ string, _ api.AttachIO) error {
        return nil
}
func (m *mockRuntimeForGhosts) InitPawnSlice(_ context.Context, _ perigeos.PawnSliceConfig) error {
        return nil
}
func (m *mockRuntimeForGhosts) CheckMachined(_ context.Context) error {
        return nil
}
func (m *mockRuntimeForGhosts) SubscribeEvents(_ context.Context) <-chan perigeos.UnitEvent {
        return nil
}
func (m *mockRuntimeForGhosts) MakeSharedMounts(_ context.Context, _, _ string, _ []perigeos.BindMount) error {
        return nil
}
func (m *mockRuntimeForGhosts) ResetUnit(_ context.Context, _, _ string) error {
        return nil
}
func (m *mockRuntimeForGhosts) CleanupStaleUnits(_ context.Context, _ map[string]bool) (int, error) {
        return 0, nil
}
func (m *mockRuntimeForGhosts) SliceActive(ctx context.Context) bool {
        return true
}
func (r *mockRuntimeForGhosts) PortForward(ctx context.Context, podUID, containerName string, port int32, stream io.ReadWriteCloser) error {
        return nil
}

// --- Mock Network ---

type mockNetworkForGhosts struct {
        tornDown []string
}

func (m *mockNetworkForGhosts) Setup(_ context.Context, podUID, _, _, _ string) (string, string, error) {
        return "/var/run/netns/" + podUID, "10.88.0.2", nil
}
func (m *mockNetworkForGhosts) Teardown(_ context.Context, podUID, _, _ string) error {
        m.tornDown = append(m.tornDown, podUID)
        return nil
}

// --- Mock Pod Lister ---

type mockPodListerForGhosts struct {
        pods []*corev1.Pod
}

func (m *mockPodListerForGhosts) List(_ labels.Selector) ([]*corev1.Pod, error) {
        return m.pods, nil
}
func (m *mockPodListerForGhosts) Get(name string) (*corev1.Pod, error) {
        for _, p := range m.pods {
                if p.Name == name {
                        return p, nil
                }
        }
        return nil, nil
}

// --- Helpers ---

func newTestReconcilerForGhosts(
        rt *mockRuntimeForGhosts,
        nm *mockNetworkForGhosts,
        lister *mockPodListerForGhosts,
) *TestReconciler {
        logger := slog.New(slog.NewTextHandler(io.Discard, nil))
        return NewReconcilerForTest(rt, nm, lister, logger)
}

func makePodForGhosts(name, namespace, uid string) *corev1.Pod {
        return &corev1.Pod{
                ObjectMeta: metav1.ObjectMeta{
                        Name:      name,
                        Namespace: namespace,
                        UID:       types.UID(uid),
                },
        }
}

// --- Tests ---

// TestCleanGhosts_GhostPodIsEvicted verifies that pods in Gambit's map but not in
// Kubernetes are evicted by cleanGhosts().
func TestCleanGhosts_GhostPodIsEvicted(t *testing.T) {
        rt := &mockRuntimeForGhosts{machines: []perigeos.PodMetadata{}}
        nm := &mockNetworkForGhosts{}
        lister := &mockPodListerForGhosts{pods: []*corev1.Pod{}}
        r := newTestReconcilerForGhosts(rt, nm, lister)

        // Pod is in Gambit but not in Kubernetes
        r.MarkHasPod("ghost-uid")

        // cleanGhosts should evict it
        r.cleanGhosts(context.Background())

        // Verify the pod was evicted from the tracker
        uids := r.tracker.PodUIDs()
        if len(uids) != 0 {
                t.Errorf("expected ghost pod to be evicted, but tracker still has %v", uids)
        }
        // Verify network teardown was called
        if len(nm.tornDown) != 1 || nm.tornDown[0] != "ghost-uid" {
                t.Errorf("expected network teardown for ghost-uid, got %v", nm.tornDown)
        }
}

// TestCleanGhosts_K8sPodIsNotEvicted verifies that pods in both Gambit and Kubernetes
// are NOT evicted.
func TestCleanGhosts_K8sPodIsNotEvicted(t *testing.T) {
        rt := &mockRuntimeForGhosts{machines: []perigeos.PodMetadata{}}
        nm := &mockNetworkForGhosts{}
        lister := &mockPodListerForGhosts{
                pods: []*corev1.Pod{
                        makePodForGhosts("my-pod", "default", "known-uid"),
                },
        }
        r := newTestReconcilerForGhosts(rt, nm, lister)

        // Pod is in both Gambit and Kubernetes
        r.MarkHasPod("known-uid")

        r.cleanGhosts(context.Background())

        // Verify the pod was NOT evicted
        uids := r.tracker.PodUIDs()
        if len(uids) != 1 {
                t.Errorf("expected known pod to remain, but tracker has %v", uids)
        }
        if _, ok := uids["known-uid"]; !ok {
                t.Errorf("expected known-uid to remain, but it's not in tracker")
        }
}

// TestCleanGhosts_InFlightPodIsNotEvicted verifies that pods marked as in-flight
// are NOT evicted even if Kubernetes doesn't know about them yet.
func TestCleanGhosts_InFlightPodIsNotEvicted(t *testing.T) {
        rt := &mockRuntimeForGhosts{machines: []perigeos.PodMetadata{}}
        nm := &mockNetworkForGhosts{}
        lister := &mockPodListerForGhosts{pods: []*corev1.Pod{}}
        r := newTestReconcilerForGhosts(rt, nm, lister)

        // Pod is in Gambit, in-flight (being created), but not yet in Kubernetes
        r.MarkHasPod("inflight-uid")
        r.MarkInFlight("inflight-uid")

        r.cleanGhosts(context.Background())

        // Verify the pod was NOT evicted
        uids := r.tracker.PodUIDs()
        if len(uids) != 1 {
                t.Errorf("expected in-flight pod to remain, but tracker has %v", uids)
        }
        if _, ok := uids["inflight-uid"]; !ok {
                t.Errorf("expected inflight-uid to remain, but it's not in tracker")
        }
}

// TestCleanGhosts_MultiplePods tests a mix of ghost, known, and in-flight pods.
func TestCleanGhosts_MultiplePods(t *testing.T) {
        rt := &mockRuntimeForGhosts{machines: []perigeos.PodMetadata{}}
        nm := &mockNetworkForGhosts{}
        lister := &mockPodListerForGhosts{
                pods: []*corev1.Pod{
                        makePodForGhosts("known", "default", "known-uid"),
                        makePodForGhosts("also-known", "default", "also-known-uid"),
                },
        }
        r := newTestReconcilerForGhosts(rt, nm, lister)

        // Set up the scenario:
        // - "ghost-uid" is a ghost (in Gambit, not in K8s)
        // - "known-uid" is known (in both)
        // - "also-known-uid" is known (in both)
        // - "inflight-uid" is in-flight (being created, not yet in K8s)
        r.MarkHasPod("ghost-uid")
        r.MarkHasPod("known-uid")
        r.MarkHasPod("also-known-uid")
        r.MarkHasPod("inflight-uid")
        r.MarkInFlight("inflight-uid")

        r.cleanGhosts(context.Background())

        // Verify the state after cleanGhosts
        uids := r.tracker.PodUIDs()
        if len(uids) != 3 {
                t.Errorf("expected 3 pods remaining (known-uid, also-known-uid, inflight-uid), got %d: %v", len(uids), uids)
        }

        for _, uid := range []string{"known-uid", "also-known-uid", "inflight-uid"} {
                if _, ok := uids[uid]; !ok {
                        t.Errorf("expected %s to remain after cleanGhosts, but it was evicted", uid)
                }
        }

        if _, ok := uids["ghost-uid"]; ok {
                t.Error("expected ghost-uid to be evicted, but it remains")
        }
}

// TestCleanGhosts_RunOnceCallsBothCleans verifies that RunOnce() calls both
// cleanOrphans and cleanGhosts.
func TestCleanGhosts_RunOnceCallsBothCleans(t *testing.T) {
        // Set up a scenario where we have both orphan machines and ghost pods
        rt := &mockRuntimeForGhosts{
                machines: []perigeos.PodMetadata{
                        // This machine is not in Gambit's pods, so it's an orphan
                        {UID: "orphan-machine-uid", Name: "orphan", Namespace: "default", ContainerName: "main"},
                },
        }
        nm := &mockNetworkForGhosts{}
        lister := &mockPodListerForGhosts{
                pods: []*corev1.Pod{
                        makePodForGhosts("my-pod", "default", "known-uid"),
                },
        }
        r := newTestReconcilerForGhosts(rt, nm, lister)

        // Ghost pod in Gambit but not in K8s
        r.MarkHasPod("ghost-uid")
        // Known pod in both
        r.MarkHasPod("known-uid")

        r.RunOnce(context.Background())

        // Verify cleanOrphans ran: the orphan machine should have been stopped
        if len(rt.stopped) != 1 || rt.stopped[0] != "orphan-machine-uid/main" {
                t.Errorf("cleanOrphans did not work correctly, expected [orphan-machine-uid/main] stopped, got %v", rt.stopped)
        }

        // Verify cleanGhosts ran: the ghost pod should have been evicted
        uids := r.tracker.PodUIDs()
        if len(uids) != 1 {
                t.Errorf("cleanGhosts did not work correctly, expected only known-uid remaining, got %v", uids)
        }
        if _, ok := uids["known-uid"]; !ok {
                t.Errorf("expected known-uid to remain after cleanGhosts")
        }

        // Verify network teardown for both the orphan and the ghost
        tornDownSet := map[string]bool{}
        for _, uid := range nm.tornDown {
                tornDownSet[uid] = true
        }
        if !tornDownSet["orphan-machine-uid"] {
                t.Errorf("expected network teardown for orphan-machine-uid, got %v", nm.tornDown)
        }
        if !tornDownSet["ghost-uid"] {
                t.Errorf("expected network teardown for ghost-uid, got %v", nm.tornDown)
        }
}

// TestCleanGhosts_EmptyLister doesn't evict if lister is nil (graceful degradation).
func TestCleanGhosts_EmptyLister(t *testing.T) {
        rt := &mockRuntimeForGhosts{machines: []perigeos.PodMetadata{}}
        nm := &mockNetworkForGhosts{}
        r := newTestReconcilerForGhosts(rt, nm, &mockPodListerForGhosts{})

        // Add a ghost pod
        r.MarkHasPod("ghost-uid")

        // If podLister is nil, cleanGhosts should exit early
        r.Reconciler.podLister = nil
        r.cleanGhosts(context.Background())

        // Ghost should still be there because cleanGhosts skipped processing
        uids := r.tracker.PodUIDs()
        if len(uids) != 1 {
                t.Errorf("expected ghost to remain when lister is nil, got %v", uids)
        }
}

// TestCleanGhosts_EmptyGambitMap doesn't process if Gambit has no pods.
func TestCleanGhosts_EmptyGambitMap(t *testing.T) {
        rt := &mockRuntimeForGhosts{machines: []perigeos.PodMetadata{}}
        nm := &mockNetworkForGhosts{}
        lister := &mockPodListerForGhosts{
                pods: []*corev1.Pod{
                        makePodForGhosts("my-pod", "default", "some-uid"),
                },
        }
        r := newTestReconcilerForGhosts(rt, nm, lister)

        // No pods in Gambit
        r.cleanGhosts(context.Background())

        // Verify no errors and tracker is still empty
        uids := r.tracker.PodUIDs()
        if len(uids) != 0 {
                t.Errorf("expected empty tracker, got %v", uids)
        }
}
