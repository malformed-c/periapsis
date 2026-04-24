package node

import (
	"context"
	"io"
	"log/slog"
	"testing"

	perigeos "github.com/malformed-c/periapsis/internal/runtime"
	"github.com/malformed-c/periapsis/internal/test/fixtures"
	corev1 "k8s.io/api/core/v1"
)

// --- Helpers ---

func newTestReconciler(rt *fixtures.RuntimeFixture, nm *fixtures.NetworkFixture, lister *fixtures.PodListerFixture) *TestReconciler {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewReconcilerForTest(rt, nm, lister, logger)
}

// --- Tests ---

func TestReconciler_OrphanMachineIsStopped(t *testing.T) {
	rt := &fixtures.RuntimeFixture{
		Machines: []perigeos.PodMetadata{
			{UID: "orphan-uid", Name: "orphan", Namespace: "default", ContainerName: "main"},
		},
	}
	nm := &fixtures.NetworkFixture{}
	r := newTestReconciler(rt, nm, &fixtures.PodListerFixture{})

	r.RunOnce(context.Background())

	if len(rt.Stopped) != 1 || rt.Stopped[0] != "orphan-uid/main" {
		t.Errorf("expected [orphan-uid/main] stopped, got %v", rt.Stopped)
	}
	if len(nm.TornDown) != 1 || nm.TornDown[0] != "orphan-uid" {
		t.Errorf("expected network teardown for orphan-uid, got %v", nm.TornDown)
	}
}

func TestReconciler_InFlightMachineIsSkipped(t *testing.T) {
	rt := &fixtures.RuntimeFixture{
		Machines: []perigeos.PodMetadata{
			{UID: "inflight-uid", Name: "mypod", Namespace: "default", ContainerName: "main"},
		},
	}
	nm := &fixtures.NetworkFixture{}
	r := newTestReconciler(rt, nm, &fixtures.PodListerFixture{})
	r.MarkInFlight("inflight-uid")

	r.RunOnce(context.Background())

	if len(rt.Stopped) != 0 {
		t.Errorf("in-flight machine should not be stopped, got %v", rt.Stopped)
	}
}

func TestReconciler_KnownPodIsSkipped(t *testing.T) {
	rt := &fixtures.RuntimeFixture{
		Machines: []perigeos.PodMetadata{
			{UID: "known-uid", Name: "mypod", Namespace: "default", ContainerName: "main"},
		},
	}
	nm := &fixtures.NetworkFixture{}
	r := newTestReconciler(rt, nm, &fixtures.PodListerFixture{})
	r.MarkHasPod("known-uid")

	r.RunOnce(context.Background())

	if len(rt.Stopped) != 0 {
		t.Errorf("known pod should not be stopped, got %v", rt.Stopped)
	}
}

func TestReconciler_K8sPodListerMatchSkips(t *testing.T) {
	rt := &fixtures.RuntimeFixture{
		Machines: []perigeos.PodMetadata{
			{UID: "k8s-uid", Name: "mypod", Namespace: "default", ContainerName: "main"},
		},
	}
	nm := &fixtures.NetworkFixture{}
	lister := &fixtures.PodListerFixture{
		Pods: []*corev1.Pod{makePod("k8s-uid", "default", "mypod", "200m", "100Mi")},
	}
	r := newTestReconciler(rt, nm, lister)

	r.RunOnce(context.Background())

	if len(rt.Stopped) != 0 {
		t.Errorf("pod in K8s lister should not be stopped, got %v", rt.Stopped)
	}
	// Forward reconciler: should have requested re-sync for the pod that
	// exists in K8s but isn't tracked by Gambit.
	if len(r.SyncRequests) != 1 || r.SyncRequests[0] != "default/mypod" {
		t.Errorf("expected sync request for default/mypod, got %v", r.SyncRequests)
	}
}

func TestReconciler_MultipleOrphans(t *testing.T) {
	rt := &fixtures.RuntimeFixture{
		Machines: []perigeos.PodMetadata{
			{UID: "uid-1", ContainerName: "app"},
			{UID: "uid-2", ContainerName: "sidecar"},
			{UID: "uid-3", ContainerName: "app"},
		},
	}
	nm := &fixtures.NetworkFixture{}
	r := newTestReconciler(rt, nm, &fixtures.PodListerFixture{})

	r.RunOnce(context.Background())

	if len(rt.Stopped) != 3 {
		t.Errorf("expected 3 stopped, got %d: %v", len(rt.Stopped), rt.Stopped)
	}
}

func TestReconciler_MixedOrphanAndKnown(t *testing.T) {
	rt := &fixtures.RuntimeFixture{
		Machines: []perigeos.PodMetadata{
			{UID: "orphan-1", ContainerName: "main"},
			{UID: "known-1", ContainerName: "main"},
			{UID: "orphan-2", ContainerName: "sidecar"},
		},
	}
	nm := &fixtures.NetworkFixture{}
	r := newTestReconciler(rt, nm, &fixtures.PodListerFixture{})
	r.MarkHasPod("known-1")

	r.RunOnce(context.Background())

	if len(rt.Stopped) != 2 {
		t.Errorf("expected 2 orphans stopped, got %d: %v", len(rt.Stopped), rt.Stopped)
	}
	for _, s := range rt.Stopped {
		if s == "known-1/main" {
			t.Error("known-1 should not have been stopped")
		}
	}
}
