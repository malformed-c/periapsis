package node

// Dispatch helpers route PodLifecycleHandler calls to Gambit directly when
// available, bypassing the syncProviderWrapper indirection.  When pc.gambit
// is nil (e.g. in unit tests using a mock provider), calls fall through to
// the interface-based pc.provider path.
//
// For delete operations: pc.deletePod in pod.go handles terminal status
// notification when gambit is set. pc.deleteDanglingPod is a simpler path
// for orphaned pods that just need cleanup.

import (
	"context"

	corev1 "k8s.io/api/core/v1"
)

func (pc *PodController) getPod(ctx context.Context, namespace, name string) (*corev1.Pod, error) {
	if pc.gambit != nil {
		return pc.gambit.GetPod(ctx, namespace, name)
	}
	return pc.provider.GetPod(ctx, namespace, name)
}

func (pc *PodController) getPods(ctx context.Context) ([]*corev1.Pod, error) {
	if pc.gambit != nil {
		return pc.gambit.GetPods(ctx)
	}
	return pc.provider.GetPods(ctx)
}

func (pc *PodController) createPod(ctx context.Context, pod *corev1.Pod) error {
	if pc.gambit != nil {
		return pc.gambit.CreatePod(ctx, pod)
	}
	return pc.provider.CreatePod(ctx, pod)
}

func (pc *PodController) updatePod(ctx context.Context, pod *corev1.Pod) error {
	if pc.gambit != nil {
		return pc.gambit.UpdatePod(ctx, pod)
	}
	return pc.provider.UpdatePod(ctx, pod)
}

// deleteDanglingPod deletes a pod that exists in the provider but not in
// Kubernetes. No terminal status notification needed — k8s already forgot it.
func (pc *PodController) deleteDanglingPod(ctx context.Context, pod *corev1.Pod) error {
	if pc.gambit != nil {
		return pc.gambit.DeletePod(ctx, pod.DeepCopy())
	}
	return pc.provider.DeletePod(ctx, pod.DeepCopy())
}
