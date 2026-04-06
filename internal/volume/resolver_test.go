package volume

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/malformed-c/periapsis/internal/test/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestResolve_HostPath(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	baseDir, err := os.MkdirTemp("", "periapsis-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(baseDir)

	r := NewResolver(baseDir, "pawn-1", "pod-uid", nil, nil, nil)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
			UID:       "pod-uid",
		},
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{
				{
					Name: "host-path-vol",
					VolumeSource: corev1.VolumeSource{
						HostPath: &corev1.HostPathVolumeSource{
							Path: "/tmp",
						},
					},
				},
			},
		},
	}

	container := &corev1.Container{
		Name: "test-container",
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      "host-path-vol",
				MountPath: "/mnt/tmp",
			},
		},
	}

	mounts, err := r.Resolve(context.Background(), pod, container)
	require.NoError(t, err)
	require.Len(t, mounts, 1)
	assert.Equal(t, "/tmp", mounts[0].HostPath)
	assert.Equal(t, "/mnt/tmp", mounts[0].ContainerPath)
}

func TestResolve_EmptyDir(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	baseDir, err := os.MkdirTemp("", "periapsis-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(baseDir)

	r := NewResolver(baseDir, "pawn-1", "pod-uid", nil, nil, nil)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID: "pod-uid",
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
		},
	}

	container := &corev1.Container{
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      "empty-dir",
				MountPath: "/data",
			},
		},
	}

	mounts, err := r.Resolve(context.Background(), pod, container)
	require.NoError(t, err)
	require.Len(t, mounts, 1)
	assert.Contains(t, mounts[0].HostPath, filepath.Join("pawns", "pawn-1", "pods", "pod-uid", "volumes", "emptydir", "empty-dir"))

	// Check if directory exists
	_, err = os.Stat(mounts[0].HostPath)
	assert.NoError(t, err)
}

func TestResolve_ConfigMap(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	baseDir, err := os.MkdirTemp("", "periapsis-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(baseDir)

	mockCMLister := mocks.NewMockConfigMapLister(ctrl)
	mockCMNamespaceLister := mocks.NewMockConfigMapNamespaceLister(ctrl)

	r := NewResolver(baseDir, "pawn-1", "pod-uid", mockCMLister, nil, nil)

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cm",
			Namespace: "default",
		},
		Data: map[string]string{
			"key1": "value1",
		},
	}

	mockCMLister.EXPECT().ConfigMaps("default").Return(mockCMNamespaceLister)
	mockCMNamespaceLister.EXPECT().Get("test-cm").Return(cm, nil)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			UID:       "pod-uid",
		},
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{
				{
					Name: "cm-vol",
					VolumeSource: corev1.VolumeSource{
						ConfigMap: &corev1.ConfigMapVolumeSource{
							LocalObjectReference: corev1.LocalObjectReference{
								Name: "test-cm",
							},
						},
					},
				},
			},
		},
	}

	container := &corev1.Container{
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      "cm-vol",
				MountPath: "/etc/config",
			},
		},
	}

	mounts, err := r.Resolve(context.Background(), pod, container)
	require.NoError(t, err)
	require.Len(t, mounts, 1)

	// Check if file is actually written
	hostPath := mounts[0].HostPath
	content, err := os.ReadFile(filepath.Join(hostPath, "key1"))
	require.NoError(t, err)
	assert.Equal(t, "value1", string(content))
}

func TestResolve_Secret(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	baseDir, err := os.MkdirTemp("", "periapsis-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(baseDir)

	mockSecretLister := mocks.NewMockSecretLister(ctrl)
	mockSecretNamespaceLister := mocks.NewMockSecretNamespaceLister(ctrl)

	r := NewResolver(baseDir, "pawn-1", "pod-uid", nil, mockSecretLister, nil)

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-secret",
			Namespace: "default",
		},
		Data: map[string][]byte{
			"key1": []byte("secret1"),
		},
	}

	mockSecretLister.EXPECT().Secrets("default").Return(mockSecretNamespaceLister)
	mockSecretNamespaceLister.EXPECT().Get("test-secret").Return(secret, nil)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			UID:       "pod-uid",
		},
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{
				{
					Name: "secret-vol",
					VolumeSource: corev1.VolumeSource{
						Secret: &corev1.SecretVolumeSource{
							SecretName: "test-secret",
						},
					},
				},
			},
		},
	}

	container := &corev1.Container{
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      "secret-vol",
				MountPath: "/etc/secret",
			},
		},
	}

	mounts, err := r.Resolve(context.Background(), pod, container)
	require.NoError(t, err)
	require.Len(t, mounts, 1)

	// Check if file is actually written
	hostPath := mounts[0].HostPath
	content, err := os.ReadFile(filepath.Join(hostPath, "key1"))
	require.NoError(t, err)
	assert.Equal(t, "secret1", string(content))
}

func TestCleanup(t *testing.T) {
	baseDir, err := os.MkdirTemp("", "periapsis-cleanup-*")
	require.NoError(t, err)
	defer os.RemoveAll(baseDir)

	r := NewResolver(baseDir, "pawn-1", "pod-uid", nil, nil, nil)

	podStateDir := filepath.Join(baseDir, "pawns", "pawn-1", "pods", "pod-uid", "volumes")
	err = os.MkdirAll(podStateDir, 0755)
	require.NoError(t, err)

	err = r.Cleanup()
	assert.NoError(t, err)

	_, err = os.Stat(podStateDir)
	assert.True(t, os.IsNotExist(err))
}

func TestRefreshConfigMapDirect(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "periapsis-refresh-cm-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	vol := &corev1.Volume{
		Name: "cm-vol",
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: "test-cm",
				},
			},
		},
	}

	cm := &corev1.ConfigMap{
		Data: map[string]string{
			"file1": "content1",
		},
	}

	err = RefreshConfigMapDirect(cm, vol, tempDir)
	require.NoError(t, err)

	data, err := os.ReadFile(filepath.Join(tempDir, "file1"))
	require.NoError(t, err)
	assert.Equal(t, "content1", string(data))

	// Update
	cm.Data["file1"] = "content1-updated"
	cm.Data["file2"] = "content2"
	err = RefreshConfigMapDirect(cm, vol, tempDir)
	require.NoError(t, err)

	data, err = os.ReadFile(filepath.Join(tempDir, "file1"))
	require.NoError(t, err)
	assert.Equal(t, "content1-updated", string(data))

	data, err = os.ReadFile(filepath.Join(tempDir, "file2"))
	require.NoError(t, err)
	assert.Equal(t, "content2", string(data))

	// Delete
	delete(cm.Data, "file1")
	err = RefreshConfigMapDirect(cm, vol, tempDir)
	require.NoError(t, err)

	_, err = os.Stat(filepath.Join(tempDir, "file1"))
	assert.True(t, os.IsNotExist(err))
}

func TestRefreshSecretDirect(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "periapsis-refresh-secret-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	vol := &corev1.Volume{
		Name: "secret-vol",
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName: "test-secret",
			},
		},
	}

	secret := &corev1.Secret{
		Data: map[string][]byte{
			"file1": []byte("content1"),
		},
	}

	err = RefreshSecretDirect(secret, vol, tempDir)
	require.NoError(t, err)

	data, err := os.ReadFile(filepath.Join(tempDir, "file1"))
	require.NoError(t, err)
	assert.Equal(t, "content1", string(data))

	// Update
	secret.Data["file1"] = []byte("content1-updated")
	err = RefreshSecretDirect(secret, vol, tempDir)
	require.NoError(t, err)

	data, err = os.ReadFile(filepath.Join(tempDir, "file1"))
	require.NoError(t, err)
	assert.Equal(t, "content1-updated", string(data))
}
