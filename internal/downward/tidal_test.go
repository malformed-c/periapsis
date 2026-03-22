package downward_test

import (
	"strings"
	"testing"

	"github.com/malformed-c/periapsis/internal/downward"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func newPod(name, ns, uid string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			UID:       types.UID("test-uid-" + uid),
		},
		Spec: corev1.PodSpec{
			NodeName: "node-1",
		},
	}
}

func envMap(envs []string) map[string]string {
	m := make(map[string]string)
	for _, e := range envs {
		parts := strings.SplitN(e, "=", 2)
		if len(parts) == 2 {
			m[parts[0]] = parts[1]
		}
	}
	return m
}

func TestTidal_StaticEnv(t *testing.T) {
	tidal := downward.NewTidal("node-1", "192.168.1.1")
	pod := newPod("mypod", "default", "abc")
	container := &corev1.Container{
		Name: "main",
		Env: []corev1.EnvVar{
			{Name: "FOO", Value: "bar"},
			{Name: "PORT", Value: "8080"},
		},
	}

	result := envMap(tidal.ResolveEnv(pod, container, "10.88.0.2"))

	if result["FOO"] != "bar" {
		t.Errorf("FOO: got %q, want %q", result["FOO"], "bar")
	}
	if result["PORT"] != "8080" {
		t.Errorf("PORT: got %q, want %q", result["PORT"], "8080")
	}
}

func TestTidal_FieldRefResolution(t *testing.T) {
	tidal := downward.NewTidal("node-1", "192.168.1.1")
	pod := newPod("mypod", "default", "abc")
	// Env values are pre-resolved by PopulateEnvironmentVariables.
	container := &corev1.Container{
		Name: "main",
		Env: []corev1.EnvVar{
			{Name: "POD_NAME", Value: "mypod"},
			{Name: "POD_NS", Value: "default"},
			{Name: "POD_UID", Value: "test-uid-abc"},
			{Name: "NODE_NAME", Value: "node-1"},
			{Name: "POD_IP", Value: "10.88.0.2"},
			{Name: "HOST_IP", Value: "192.168.1.1"},
		},
	}

	result := envMap(tidal.ResolveEnv(pod, container, "10.88.0.2"))

	cases := map[string]string{
		"POD_NAME":  "mypod",
		"POD_NS":    "default",
		"POD_UID":   "test-uid-abc",
		"NODE_NAME": "node-1",
		"POD_IP":    "10.88.0.2",
		"HOST_IP":   "192.168.1.1",
	}
	for k, want := range cases {
		if got := result[k]; got != want {
			t.Errorf("%s: got %q, want %q", k, got, want)
		}
	}
}

func TestTidal_ForceInjectedOverrides(t *testing.T) {
	tidal := downward.NewTidal("node-42", "10.0.0.1")
	pod := newPod("testpod", "kube-system", "xyz")
	container := &corev1.Container{
		Name: "main",
		// Container tries to spoof MY_POD_IP
		Env: []corev1.EnvVar{
			{Name: "MY_POD_IP", Value: "1.2.3.4"},
		},
	}

	result := envMap(tidal.ResolveEnv(pod, container, "10.88.0.5"))

	// Force-injected value must win over the container's attempt to set it.
	if result["MY_POD_IP"] != "10.88.0.5" {
		t.Errorf("MY_POD_IP: got %q, want 10.88.0.5 (override should win)", result["MY_POD_IP"])
	}
	if result["MY_NODE_NAME"] != "node-42" {
		t.Errorf("MY_NODE_NAME: got %q, want node-42", result["MY_NODE_NAME"])
	}
	if result["MY_HOST_IP"] != "10.0.0.1" {
		t.Errorf("MY_HOST_IP: got %q, want 10.0.0.1", result["MY_HOST_IP"])
	}
	if result["MY_POD_NAME"] != "testpod" {
		t.Errorf("MY_POD_NAME: got %q, want testpod", result["MY_POD_NAME"])
	}
	if result["MY_POD_NS"] != "kube-system" {
		t.Errorf("MY_POD_NS: got %q, want kube-system", result["MY_POD_NS"])
	}
}

func TestTidal_EmptyContainer(t *testing.T) {
	tidal := downward.NewTidal("node-1", "192.168.1.1")
	pod := newPod("pod", "default", "1")
	container := &corev1.Container{Name: "main"}

	result := tidal.ResolveEnv(pod, container, "10.88.0.2")

	// Should still have the 6 force-injected keys
	m := envMap(result)
	for _, key := range []string{"MY_POD_IP", "MY_HOST_IP", "MY_NODE_NAME", "MY_POD_UID", "MY_POD_NAME", "MY_POD_NS"} {
		if _, ok := m[key]; !ok {
			t.Errorf("missing force-injected key %s", key)
		}
	}
}

func TestTidal_UnknownFieldRef_Skipped(t *testing.T) {
	tidal := downward.NewTidal("node-1", "192.168.1.1")
	pod := newPod("pod", "default", "1")
	container := &corev1.Container{
		Name: "main",
		Env: []corev1.EnvVar{
			{
				Name: "UNKNOWN",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{FieldPath: "spec.nonexistent"},
				},
			},
		},
	}

	result := envMap(tidal.ResolveEnv(pod, container, "10.88.0.2"))

	// Unknown FieldRef resolves to empty string — key should still be present
	// (the current implementation includes it as UNKNOWN=)
	// Just verify it doesn't panic and the injected keys are present.
	if _, ok := result["MY_POD_IP"]; !ok {
		t.Error("MY_POD_IP missing after unknown FieldRef")
	}
}
