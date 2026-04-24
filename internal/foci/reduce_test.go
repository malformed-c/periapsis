package foci

import (
	"testing"

	"github.com/malformed-c/periapsis/internal/types"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestReduce_PodRegisterFact(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:       "uid-1",
			Namespace: "default",
			Name:      "web",
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{Name: "nginx", Image: "nginx"},
			},
			RestartPolicy: corev1.RestartPolicyAlways,
		},
	}

	fact := types.NewPodRegisterFact("uid-1", "default", "web", "10.0.0.1", pod)

	// Initial state is zero value
	state := PodState{}

	newState, effects := Reduce(state, fact)

	if newState.UID != "uid-1" {
		t.Errorf("expected UID uid-1, got %s", newState.UID)
	}
	if newState.Phase != corev1.PodPending {
		t.Errorf("expected phase Pending, got %s", newState.Phase)
	}
	if len(newState.Containers) != 1 {
		t.Errorf("expected 1 container, got %d", len(newState.Containers))
	}

	// Check effects
	hasRegister := false
	hasInitRestart := false
	for _, eff := range effects {
		switch eff.(type) {
		case types.RegisterPod:
			hasRegister = true
		case types.InitRestartState:
			hasInitRestart = true
		}
	}
	if !hasRegister {
		t.Error("expected RegisterPod effect")
	}
	if !hasInitRestart {
		t.Error("expected InitRestartState effect")
	}
}

func TestReduce_PodPromoteFact(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:       "uid-1",
			Namespace: "default",
			Name:      "web",
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "nginx", Image: "nginx"}},
		},
	}

	state := NewPodState("uid-1", "default", "web", "", pod)
	fact := types.NewPodPromoteFact("uid-1", "default", "web", "10.0.0.1", pod)

	newState, effects := Reduce(state, fact)

	if newState.PodIP != "10.0.0.1" {
		t.Errorf("expected IP 10.0.0.1, got %s", newState.PodIP)
	}

	hasPromote := false
	for _, eff := range effects {
		if _, ok := eff.(types.PromotePodRunning); ok {
			hasPromote = true
		}
	}
	if !hasPromote {
		t.Error("expected PromotePodRunning effect")
	}
}
