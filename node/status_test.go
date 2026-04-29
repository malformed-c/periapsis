package node

import (
	"log/slog"
	"testing"
	"time"

	perigeos "github.com/malformed-c/periapsis/internal/runtime"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// TestBuildPodStatusUnknownRunningPodNotReady verifies that when a pod is
// already Running and a container transitions to Unknown, the container is
// marked Not Ready (ContainerCreating) rather than preserving stale Running
// state. Terminal phase decisions are handled by checkPod/SetPhase.
func TestBuildPodStatusUnknownRunningPodNotReady(t *testing.T) {
	g := &Gambit{
		Logger: slog.Default(),
		store:  setupTestStore(),
		node:   &PawnNode{startTime: time.Now()},
	}
	t.Cleanup(g.store.Close)

	uid := "uid-1"
	startedAt := metav1.Now()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:       types.UID(uid),
			Namespace: "default",
			Name:      "security-test",
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "main", Image: "nginx"}},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  "main",
				Ready: true,
				State: corev1.ContainerState{
					Running: &corev1.ContainerStateRunning{StartedAt: startedAt},
				},
			}},
		},
	}

	g.store.RegisterPending(uid, pod, nil)
	g.store.SetContainerMachineState(uid, "main", perigeos.StateUnknown, 0)
	status := g.buildPodStatus(pod)

	// Pod phase stays Running - only checkPod/SetPhase can set terminal phases.
	if status.Phase != corev1.PodRunning {
		t.Fatalf("expected phase %q, got %q", corev1.PodRunning, status.Phase)
	}
	// Container should be Not Ready with ContainerCreating, not preserving stale Running.
	if status.ContainerStatuses[0].State.Waiting == nil || status.ContainerStatuses[0].State.Waiting.Reason != "ContainerCreating" {
		t.Fatalf("expected ContainerCreating waiting state, got %#v", status.ContainerStatuses[0].State)
	}
	if status.ContainerStatuses[0].Ready {
		t.Fatal("expected ready=false for unknown container in running pod")
	}
}

// TestBuildPodStatusUnknownPendingPodKeepsRunning verifies that during pod
// creation (Pending phase), a container that was previously reported as
// Running keeps that state when the state lookup returns Unknown. This
// prevents transient D-Bus misses from regressing a visible Running container.
func TestBuildPodStatusUnknownPendingPodKeepsRunning(t *testing.T) {
	g := &Gambit{
		Logger: slog.Default(),
		store:  setupTestStore(),
		node:   &PawnNode{startTime: time.Now()},
	}
	t.Cleanup(g.store.Close)

	uid := "uid-1b"
	startedAt := metav1.Now()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:       types.UID(uid),
			Namespace: "default",
			Name:      "pending-test",
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				// sidecar is first so StateCreating sets podPhase=Pending
				// before main's Unknown check.
				{Name: "sidecar", Image: "busybox"},
				{Name: "main", Image: "nginx"},
			},
		},
		Status: corev1.PodStatus{
			// Pod is still Pending (not all containers running yet).
			Phase: corev1.PodPending,
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  "main",
				Ready: true,
				State: corev1.ContainerState{
					Running: &corev1.ContainerStateRunning{StartedAt: startedAt},
				},
			}},
		},
	}

	// Register the pod so SetContainerMachineState can write to the podState.
	g.store.RegisterPending(uid, pod, nil)

	// "main" returns Unknown (transient D-Bus miss), "sidecar" is still creating.
	g.store.SetContainerMachineState(uid, "sidecar", perigeos.StateCreating, 0)
	g.store.SetContainerMachineState(uid, "main", perigeos.StateUnknown, 0)
	status := g.buildPodStatus(pod)

	if status.Phase != corev1.PodPending {
		t.Fatalf("expected phase %q, got %q", corev1.PodPending, status.Phase)
	}
	// main is at index 1 (sidecar is 0): should preserve Running state.
	if status.ContainerStatuses[1].State.Running == nil {
		t.Fatalf("expected running state preserved for main during Pending, got %#v", status.ContainerStatuses[1].State)
	}
	if !status.ContainerStatuses[1].Ready {
		t.Fatal("expected ready=true preserved for main during Pending")
	}
}

// TestBuildPodStatusUnknownContainerCreating verifies that when all containers
// are Unknown with no previous state, they show ContainerCreating. The pod
// phase stays Running because buildPodStatus no longer forces Pending for
// Unknown - the actual Pending phase is managed upstream by GetPodStatus
// checking the store's phase map.
func TestBuildPodStatusUnknownContainerCreating(t *testing.T) {
	g := &Gambit{
		Logger: slog.Default(),
		store:  setupTestStore(),
		node:   &PawnNode{startTime: time.Now()},
	}
	t.Cleanup(g.store.Close)

	uid := "uid-2"
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:       types.UID(uid),
			Namespace: "default",
			Name:      "security-test",
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "main", Image: "nginx"}},
		},
	}

	// No SetContainerMachineState call - store returns StateUnknown by default.
	status := g.buildPodStatus(pod)

	// Phase stays Running - buildPodStatus doesn't demote to Pending for Unknown.
	// Upstream GetPodStatus handles the Pending case via store phase check.
	if status.Phase != corev1.PodRunning {
		t.Fatalf("expected phase %q, got %q", corev1.PodRunning, status.Phase)
	}
	if status.ContainerStatuses[0].State.Waiting == nil || status.ContainerStatuses[0].State.Waiting.Reason != "ContainerCreating" {
		t.Fatalf("expected ContainerCreating waiting state, got %#v", status.ContainerStatuses[0].State)
	}
}
