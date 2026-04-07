package node

import (
	"log/slog"
	"os"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// ─── Test Helpers ───────────────────────────────────────────────────────────

// makePod creates a basic pod with requested CPU and Memory for testing.
func makePod(uid, namespace, name, cpuReq, memReq string) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:       types.UID(uid),
			Namespace: namespace,
			Name:      name,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name: "main-container",
				},
			},
		},
	}

	requests := corev1.ResourceList{}
	if cpuReq != "" {
		requests[corev1.ResourceCPU] = resource.MustParse(cpuReq)
	}
	if memReq != "" {
		requests[corev1.ResourceMemory] = resource.MustParse(memReq)
	}

	if len(requests) > 0 {
		pod.Spec.Containers[0].Resources = corev1.ResourceRequirements{
			Requests: requests,
		}
	}

	return pod
}

func setupTestStore() *PodStore {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	// Passing nil for perigeos.Runtime assuming the tests here don't trigger active probes
	return NewPodStore(nil, 10, logger)
}

// waitForSnapshot waits for the asynchronous background aggregator to catch up.
func waitForSnapshot(t *testing.T, store *PodStore, expected int) []PodSnapshot {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		snap := store.LoadSnapshot()
		if len(snap) == expected {
			return snap
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("Timeout waiting for snapshot to reach size %d (got %d)", expected, len(store.LoadSnapshot()))
	return nil
}

// ─── Tests ──────────────────────────────────────────────────────────────────

func TestPodStore_BasicLifecycle(t *testing.T) {
	store := setupTestStore()
	pod := makePod("uid-1", "default", "web", "", "")

	// 1. Register Pending
	dummySaga := &podSaga{done: make(chan struct{})} // Mocking unexported saga
	store.RegisterPending("uid-1", pod, dummySaga)

	if !store.HasPod("uid-1") {
		t.Fatal("Expected pod to exist after RegisterPending")
	}
	if !store.IsInFlight("uid-1") {
		t.Fatal("Expected pod to be in flight")
	}
	if store.PodPhase("uid-1") != corev1.PodPending {
		t.Fatalf("Expected phase Pending, got %s", store.PodPhase("uid-1"))
	}
	if store.PodCount() != 1 {
		t.Fatalf("Expected 1 pod, got %d", store.PodCount())
	}

	// 2. Promote to Running
	store.PromoteRunning("uid-1", pod, "10.0.0.5")

	if store.IsInFlight("uid-1") {
		t.Fatal("Expected pod to no longer be in flight after promotion")
	}
	if store.PodIP("uid-1") != "10.0.0.5" {
		t.Fatalf("Expected IP 10.0.0.5, got %s", store.PodIP("uid-1"))
	}
	if store.PodPhase("uid-1") != corev1.PodRunning {
		t.Fatalf("Expected phase Running, got %s", store.PodPhase("uid-1"))
	}

	// Wait for the async aggregator
	snap := waitForSnapshot(t, store, 1)
	if snap[0].IP != "10.0.0.5" {
		t.Fatalf("Snapshot IP mismatch: %s", snap[0].IP)
	}

	// 3. Deletion and Unregister
	store.MarkDeleting("uid-1")
	if !store.IsDeleting("uid-1") {
		t.Fatal("Expected pod to be marked for deletion")
	}
	if !store.DeletionsInProgress() {
		t.Fatal("Expected deletions in progress to be true")
	}

	store.Unregister("uid-1", "default", "web")
	if store.HasPod("uid-1") {
		t.Fatal("Expected pod to be removed")
	}
	if store.CompletedPodUID("default", "web") != "uid-1" {
		t.Fatal("Expected pod to be in completed index")
	}

	waitForSnapshot(t, store, 0)
}

func TestPodStore_ResourceAdmission(t *testing.T) {
	store := setupTestStore()

	// Node Capacity: 1000m CPU, 1Gi Mem
	nodeCPU := resource.MustParse("1000m")
	nodeMem := resource.MustParse("1Gi")

	// Pod 1 requests 600m CPU, 500Mi Mem
	pod1 := makePod("uid-1", "default", "heavy-1", "600m", "500Mi")
	store.RegisterPending("uid-1", pod1, nil)

	// Pod 2 requests 500m CPU (should fail admission because 600+500 > 1000)
	pod2 := makePod("uid-2", "default", "heavy-2", "500m", "200Mi")
	reason := store.AdmitPod(pod2, nodeCPU, nodeMem)
	if reason == "" {
		t.Fatal("Expected AdmitPod to reject pod2 due to CPU exhaustion")
	}

	// Pod 3 requests 200m CPU (should pass)
	pod3 := makePod("uid-3", "default", "light-3", "200m", "100Mi")
	reason = store.AdmitPod(pod3, nodeCPU, nodeMem)
	if reason != "" {
		t.Fatalf("Expected AdmitPod to admit pod3, got rejection: %s", reason)
	}
	store.RegisterPending("uid-3", pod3, nil)

	// Test ComputeAllocatable
	capacity := corev1.ResourceList{
		corev1.ResourceCPU:    nodeCPU,
		corev1.ResourceMemory: nodeMem,
	}
	allocatable := store.ComputeAllocatable(capacity)

	// 1000m - 600m - 200m = 200m left
	cpuAlloc := allocatable[corev1.ResourceCPU]
	if cpuAlloc.MilliValue() != 200 {
		t.Fatalf("Expected 200m CPU allocatable, got %v", cpuAlloc.MilliValue())
	}

	// Remove pod 1, resources should free up instantly
	store.Unregister("uid-1", "default", "heavy-1")
	allocatableAfter := store.ComputeAllocatable(capacity)

	// 1000m - 200m = 800m left
	cpuAllocAfter := allocatableAfter[corev1.ResourceCPU]
	if cpuAllocAfter.MilliValue() != 800 {
		t.Fatalf("Expected 800m CPU allocatable after unregister, got %v", cpuAllocAfter.MilliValue())
	}
}

func TestPodStore_ProbesAndRestarts(t *testing.T) {
	store := setupTestStore()
	pod := makePod("uid-1", "default", "web", "", "")
	store.RegisterPending("uid-1", pod, nil)

	// Init state
	store.InitRestartState(pod)

	cName := "main-container"

	// Verify initial defaults
	if !store.IsContainerReady("uid-1", cName) {
		t.Fatal("Expected container to default to ready (no probe defined)")
	}

	// Bump backoff
	count, backoff := store.BumpBackoff("uid-1", cName)
	if count != 1 {
		t.Fatalf("Expected restart count 1, got %d", count)
	}
	if backoff < time.Second {
		t.Fatal("Expected backoff to be initialized")
	}

	// Mark Restarted and check counts
	store.IncrementRestart("uid-1", cName)
	counts := store.RestartCounts("uid-1")
	if counts[cName] != 2 {
		t.Fatalf("Expected restart count 2, got %d", counts[cName])
	}

	// Update Probe State
	store.UpdateProbeState("uid-1", cName, func(ps *ContainerProbeState) {
		ps.Ready = false
	})
	if store.IsContainerReady("uid-1", cName) {
		t.Fatal("Expected container to be unready after probe update")
	}
}

func TestPodStore_Hydration(t *testing.T) {
	store := setupTestStore()
	pod := makePod("uid-hydrated", "default", "restored", "100m", "100Mi")

	// Hydrate
	store.RegisterHydratedBatch([]hydratedEntry{
		{uid: "uid-hydrated", pod: pod, ip: "10.0.0.10"},
	})

	if !store.HasPod("uid-hydrated") {
		t.Fatal("Expected hydrated pod to exist")
	}

	uids := store.HydratedUIDs()
	if !uids["uid-hydrated"] {
		t.Fatal("Expected UID to be in HydratedUIDs")
	}

	// Test AlreadyRunning with a hydrated pod
	exists, wasStub := store.AlreadyRunning("uid-hydrated", pod)
	if !exists {
		t.Fatal("Expected AlreadyRunning to return true for hydrated pod")
	}
	if wasStub {
		t.Fatal("Did not expect pod to be a stub")
	}

	// Verify that checking AlreadyRunning cleared the hydrated flag (k8s confirmed it)
	uidsAfter := store.HydratedUIDs()
	if uidsAfter["uid-hydrated"] {
		t.Fatal("Expected UID to be cleared from hydrated set after AlreadyRunning")
	}

	// Verify resources were allocated correctly on hydration
	capNode := corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1000m")}
	alloc := store.ComputeAllocatable(capNode)
	cpuAllocHydrated := alloc[corev1.ResourceCPU]
	if cpuAllocHydrated.MilliValue() != 900 {
		t.Fatalf("Expected hydration to consume resources (900m left), got %d", cpuAllocHydrated.MilliValue())
	}

	// Test Purge Hydrated (Ghost pods that API server didn't confirm)
	store.RegisterHydrated("uid-ghost", makePod("uid-ghost", "default", "ghost", "200m", ""), "")
	store.PurgeHydrated([]string{"uid-ghost"})

	if store.HasPod("uid-ghost") {
		t.Fatal("Expected ghost pod to be purged")
	}
}

func TestPodStore_SnapshotAggregatorConcurrency(t *testing.T) {
	// This test ensures the channel-triggered background aggregator
	// does not block hot paths and updates eventually.
	store := setupTestStore()

	// Fire off 100 pod registrations rapidly
	for i := 0; i < 100; i++ {
		uid := string(rune(i))
		pod := makePod(uid, "ns", "pod", "", "")
		store.RegisterPending(uid, pod, nil)
	}

	// Wait for the snapshot aggregator to catch up
	snap := waitForSnapshot(t, store, 100)
	if len(snap) != 100 {
		t.Fatalf("Expected 100 items in snapshot, got %d", len(snap))
	}
}

func TestPodStore_EvictGhost(t *testing.T) {
	store := setupTestStore()
	pod := makePod("uid-ghost", "default", "ghost", "500m", "100Mi")

	store.RegisterPending("uid-ghost", pod, nil)

	// Resources should be consumed
	if store.usedCPU.Load() != 500 {
		t.Fatalf("Expected 500m used CPU, got %d", store.usedCPU.Load())
	}

	store.EvictGhost("uid-ghost")

	// Ghost should be gone, resources should be restored
	if store.HasPod("uid-ghost") {
		t.Fatal("Expected ghost pod to be evicted")
	}
	if store.usedCPU.Load() != 0 {
		t.Fatalf("Expected 0 used CPU after eviction, got %d", store.usedCPU.Load())
	}
}
