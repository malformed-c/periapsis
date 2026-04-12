package node

import (
	"testing"
	"time"

	perigeos "github.com/malformed-c/periapsis/internal/runtime"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestBuildPodStatusUnknownKeepsPreviousRunningState(t *testing.T) {
	g := &Gambit{
		store: setupTestStore(),
		node:  &PawnNode{startTime: time.Now()},
	}
	t.Cleanup(g.store.Close)

	startedAt := metav1.Now()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:       types.UID("uid-1"),
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

	status := g.buildPodStatus(pod, func(_, _ string) perigeos.MachineState {
		return perigeos.StateUnknown
	})

	if status.Phase != corev1.PodRunning {
		t.Fatalf("expected phase %q, got %q", corev1.PodRunning, status.Phase)
	}
	if status.ContainerStatuses[0].State.Running == nil {
		t.Fatalf("expected running state to be preserved, got %#v", status.ContainerStatuses[0].State)
	}
	if !status.ContainerStatuses[0].Ready {
		t.Fatal("expected ready=true to be preserved for running container")
	}
}

func TestBuildPodStatusUnknownWithoutPreviousRunningIsPending(t *testing.T) {
	g := &Gambit{
		store: setupTestStore(),
		node:  &PawnNode{startTime: time.Now()},
	}
	t.Cleanup(g.store.Close)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:       types.UID("uid-2"),
			Namespace: "default",
			Name:      "security-test",
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "main", Image: "nginx"}},
		},
	}

	status := g.buildPodStatus(pod, func(_, _ string) perigeos.MachineState {
		return perigeos.StateUnknown
	})

	if status.Phase != corev1.PodPending {
		t.Fatalf("expected phase %q, got %q", corev1.PodPending, status.Phase)
	}
	if status.ContainerStatuses[0].State.Waiting == nil || status.ContainerStatuses[0].State.Waiting.Reason != "ContainerCreating" {
		t.Fatalf("expected ContainerCreating waiting state, got %#v", status.ContainerStatuses[0].State)
	}
}
