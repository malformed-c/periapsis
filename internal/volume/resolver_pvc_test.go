package volume

import (
	"context"
	"os"
	"testing"

	"github.com/malformed-c/periapsis/internal/test/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestResolve_PVC(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	baseDir, err := os.MkdirTemp("", "periapsis-test-pvc-*")
	require.NoError(t, err)
	defer os.RemoveAll(baseDir)

	mockKubeClient := mocks.NewMockInterface(ctrl)
	mockCoreV1 := mocks.NewMockCoreV1Interface(ctrl)
	mockPVC := mocks.NewMockPersistentVolumeClaimInterface(ctrl)
	mockPV := mocks.NewMockPersistentVolumeInterface(ctrl)

	r := NewResolver(baseDir, "pawn-1", "pod-uid", nil, nil, mockKubeClient)

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pvc",
			Namespace: "default",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: "test-pv",
		},
		Status: corev1.PersistentVolumeClaimStatus{
			Phase: corev1.ClaimBound,
		},
	}

	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pv",
		},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/mnt/data",
				},
			},
		},
	}

	mockKubeClient.EXPECT().CoreV1().Return(mockCoreV1).AnyTimes()
	mockCoreV1.EXPECT().PersistentVolumeClaims("default").Return(mockPVC)
	mockPVC.EXPECT().Get(gomock.Any(), "test-pvc", gomock.Any()).Return(pvc, nil)
	mockCoreV1.EXPECT().PersistentVolumes().Return(mockPV)
	mockPV.EXPECT().Get(gomock.Any(), "test-pv", gomock.Any()).Return(pv, nil)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			UID:       "pod-uid",
		},
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{
				{
					Name: "pvc-vol",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: "test-pvc",
						},
					},
				},
			},
		},
	}

	container := &corev1.Container{
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      "pvc-vol",
				MountPath: "/data",
			},
		},
	}

	mounts, err := r.Resolve(context.Background(), pod, container)
	require.NoError(t, err)
	require.Len(t, mounts, 1)
	assert.Equal(t, "/mnt/data", mounts[0].HostPath)
}
