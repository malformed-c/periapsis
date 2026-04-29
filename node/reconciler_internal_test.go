package node

import (
	"context"
	"io"
	"log/slog"
	"testing"

	perigeos "github.com/malformed-c/periapsis/internal/runtime"
	"github.com/malformed-c/periapsis/internal/test/fixtures"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// --- Helpers ---

func newTestReconcilerForGhosts(
	rt *fixtures.RuntimeFixture,
	nm *fixtures.NetworkFixture,
	lister *fixtures.PodListerFixture,
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
	rt := fixtures.NewRuntimeFixture()
	nm := fixtures.NewNetworkFixture()
	lister := &fixtures.PodListerFixture{Pods: []*corev1.Pod{}}
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
	if len(nm.TeardownCalled) != 1 || nm.TeardownCalled[0] != "ghost-uid" {
		t.Errorf("expected network teardown for ghost-uid, got %v", nm.TeardownCalled)
	}
}

// TestCleanGhosts_K8sPodIsNotEvicted verifies that pods in both Gambit and Kubernetes
// are NOT evicted.
func TestCleanGhosts_K8sPodIsNotEvicted(t *testing.T) {
	rt := fixtures.NewRuntimeFixture()
	nm := fixtures.NewNetworkFixture()
	lister := &fixtures.PodListerFixture{
		Pods: []*corev1.Pod{
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
	rt := fixtures.NewRuntimeFixture()
	nm := fixtures.NewNetworkFixture()
	lister := &fixtures.PodListerFixture{Pods: []*corev1.Pod{}}
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
	rt := fixtures.NewRuntimeFixture()
	nm := fixtures.NewNetworkFixture()
	lister := &fixtures.PodListerFixture{
		Pods: []*corev1.Pod{
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
	rt := &fixtures.RuntimeFixture{
		Machines: []perigeos.PodMetadata{
			// This machine is not in Gambit's pods, so it's an orphan
			{UID: "orphan-machine-uid", Name: "orphan", Namespace: "default", ContainerName: "main"},
		},
	}
	nm := fixtures.NewNetworkFixture()
	lister := &fixtures.PodListerFixture{
		Pods: []*corev1.Pod{
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
	if len(rt.Stopped) != 1 || rt.Stopped[0] != "orphan-machine-uid/main" {
		t.Errorf("cleanOrphans did not work correctly, expected [orphan-machine-uid/main] stopped, got %v", rt.Stopped)
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
	for _, uid := range nm.TeardownCalled {
		tornDownSet[uid] = true
	}
	if !tornDownSet["orphan-machine-uid"] {
		t.Errorf("expected network teardown for orphan-machine-uid, got %v", nm.TeardownCalled)
	}
	if !tornDownSet["ghost-uid"] {
		t.Errorf("expected network teardown for ghost-uid, got %v", nm.TeardownCalled)
	}
}

// TestCleanGhosts_EmptyLister doesn't evict if lister is nil (graceful degradation).
func TestCleanGhosts_EmptyLister(t *testing.T) {
	rt := fixtures.NewRuntimeFixture()
	nm := fixtures.NewNetworkFixture()
	r := newTestReconcilerForGhosts(rt, nm, &fixtures.PodListerFixture{})

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
	rt := fixtures.NewRuntimeFixture()
	nm := fixtures.NewNetworkFixture()
	lister := &fixtures.PodListerFixture{
		Pods: []*corev1.Pod{
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
