package node

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/malformed-c/periapsis/internal/volume"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestIntegration_VolumeLifecycle(t *testing.T) {
	h := newHarness(t)
	uid := "uid-vol-test"

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-vol",
			Namespace: "default",
			UID:       types.UID(uid),
		},
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{
				{
					Name: "empty-dir",
					VolumeSource: corev1.VolumeSource{
						EmptyDir: &corev1.EmptyDirVolumeSource{},
					},
				},
			},
			Containers: []corev1.Container{
				{
					Name:  "main",
					Image: "dummy-image",
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      "empty-dir",
							MountPath: "/data",
						},
					},
				},
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	volResolver := volume.NewResolver(h.baseDir, h.pawnName, uid, nil, nil, nil)
	mounts, err := volResolver.Resolve(ctx, pod, &pod.Spec.Containers[0])
	require.NoError(t, err)
	require.Len(t, mounts, 1)

	// Check if the emptyDir directory was created on the host
	hostPath := mounts[0].HostPath
	_, err = os.Stat(hostPath)
	assert.NoError(t, err, "emptyDir host path should exist")

	// Clean up
	err = volResolver.Cleanup()
	assert.NoError(t, err)

	_, err = os.Stat(hostPath)
	assert.True(t, os.IsNotExist(err), "emptyDir host path should be removed")
}
