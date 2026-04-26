package node

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestExtractResourceLimits_PrefersContainerResources(t *testing.T) {
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Resources: &corev1.ResourceRequirements{
				Limits: corev1.ResourceList{
					corev1.ResourceMemory: resource.MustParse("1Gi"),
					corev1.ResourceCPU:    resource.MustParse("2"),
				},
				Requests: corev1.ResourceList{
					corev1.ResourceCPU: resource.MustParse("1500m"),
				},
			},
		},
	}
	container := &corev1.Container{
		Resources: corev1.ResourceRequirements{
			Limits: corev1.ResourceList{
				corev1.ResourceMemory: resource.MustParse("256Mi"),
				corev1.ResourceCPU:    resource.MustParse("500m"),
			},
			Requests: corev1.ResourceList{
				corev1.ResourceCPU: resource.MustParse("250m"),
			},
		},
	}

	mem, _, cpuLimit, cpuReq := extractResourceLimits(pod, container)

	res := resource.MustParse("256Mi")
	if mem != uint64(res.Value()) {
		t.Fatalf("unexpected mem limit: got %d", mem)
	}
	if cpuLimit != 500 {
		t.Fatalf("unexpected cpu limit: got %d", cpuLimit)
	}
	if cpuReq != 250 {
		t.Fatalf("unexpected cpu request: got %d", cpuReq)
	}
}

func TestExtractResourceLimits_FallsBackToPodResources(t *testing.T) {
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Resources: &corev1.ResourceRequirements{
				Limits: corev1.ResourceList{
					corev1.ResourceMemory: resource.MustParse("768Mi"),
					corev1.ResourceCPU:    resource.MustParse("1200m"),
				},
				Requests: corev1.ResourceList{
					corev1.ResourceCPU: resource.MustParse("600m"),
				},
			},
		},
	}
	container := &corev1.Container{}

	mem, _, cpuLimit, cpuReq := extractResourceLimits(pod, container)

	res := resource.MustParse("768Mi")

	if mem != uint64(res.Value()) {
		t.Fatalf("unexpected mem limit: got %d", mem)
	}
	if cpuLimit != 1200 {
		t.Fatalf("unexpected cpu limit: got %d", cpuLimit)
	}
	if cpuReq != 600 {
		t.Fatalf("unexpected cpu request: got %d", cpuReq)
	}
}

func TestEffectiveRunAs_ContainerOverridesPod(t *testing.T) {
	podUID := int64(1000)
	podGID := int64(3000)
	containerUID := int64(2000)
	containerGID := int64(4000)

	pod := &corev1.Pod{Spec: corev1.PodSpec{SecurityContext: &corev1.PodSecurityContext{
		RunAsUser:  &podUID,
		RunAsGroup: &podGID,
	}}}
	container := &corev1.Container{SecurityContext: &corev1.SecurityContext{
		RunAsUser:  &containerUID,
		RunAsGroup: &containerGID,
	}}

	runAsUser, runAsGroup := effectiveRunAs(pod, container)
	if runAsUser == nil || *runAsUser != containerUID {
		t.Fatalf("unexpected runAsUser: %#v", runAsUser)
	}
	if runAsGroup == nil || *runAsGroup != containerGID {
		t.Fatalf("unexpected runAsGroup: %#v", runAsGroup)
	}
}

func TestBuildContainerRuntimeProfiles_PerContainerMap(t *testing.T) {
	podUID := int64(1000)
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			SecurityContext: &corev1.PodSecurityContext{RunAsUser: &podUID},
			Resources: &corev1.ResourceRequirements{
				Limits: corev1.ResourceList{
					corev1.ResourceMemory: resource.MustParse("512Mi"),
					corev1.ResourceCPU:    resource.MustParse("1000m"),
				},
			},
			InitContainers: []corev1.Container{
				{Name: "init-1"},
			},
			Containers: []corev1.Container{
				{
					Name: "main",
					Resources: corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							corev1.ResourceMemory: resource.MustParse("128Mi"),
						},
					},
				},
			},
		},
	}

	profiles := buildContainerRuntimeProfiles(pod)
	if len(profiles) != 2 {
		t.Fatalf("expected 2 profiles, got %d", len(profiles))
	}

	initProfile, ok := profiles["init-1"]
	if !ok {
		t.Fatalf("missing init profile")
	}
	if initProfile.CPULimitMillis != 1000 {
		t.Fatalf("unexpected init cpu limit: got %d", initProfile.CPULimitMillis)
	}
	if initProfile.RunAsUser == nil || *initProfile.RunAsUser != podUID {
		t.Fatalf("unexpected init runAsUser: %#v", initProfile.RunAsUser)
	}

	mainProfile, ok := profiles["main"]
	if !ok {
		t.Fatalf("missing main profile")
	}

	res := resource.MustParse("128Mi")
	if mainProfile.MemoryLimitBytes != uint64(res.Value()) {
		t.Fatalf("unexpected main mem limit: got %d", mainProfile.MemoryLimitBytes)
	}
	if mainProfile.CPULimitMillis != 1000 {
		t.Fatalf("unexpected main cpu limit fallback: got %d", mainProfile.CPULimitMillis)
	}
}
