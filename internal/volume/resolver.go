// Package volume resolves Kubernetes pod volume specs + container volume mounts
// into concrete host→container bind mounts for systemd-nspawn.
//
// Supported volume types:
//   - hostPath  — direct bind mount of a host path
//   - emptyDir  — host-side tmpfs/dir created under the pod's state dir
//   - configMap — files written to a host-side dir, then bind-mounted read-only
//   - secret    — files written to a host-side dir, then bind-mounted read-only
//   - projected — kube-api-access (service account token + CA cert + downward API)
//   - persistentVolumeClaim — resolved to the underlying PV hostPath (local-path and local PV types) or CSI volumes
package volume

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"time"

	authv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	listersv1 "k8s.io/client-go/listers/core/v1"

	"github.com/malformed-c/periapsis/errdefs"
	perigeos "github.com/malformed-c/periapsis/internal/runtime"
)

// Resolver resolves pod volume declarations into BindMounts.
type Resolver struct {
	// stateDir is the per-pod directory on the host used for emptyDir and
	// projected volumes: <baseDir>/pawns/<pawn>/pods/<uid>/volumes/
	stateDir string

	// hostNodeName is the name of the real host node (for VolumeAttachment
	// creation when mounting CSI volumes).
	hostNodeName string

	// cmLister and secretLister are optional top-level listers.
	// They are scoped to the pod's namespace at resolution time.
	cmLister     listersv1.ConfigMapLister
	secretLister listersv1.SecretLister

	// kubeClient is used for TokenRequest (projected service account volumes)
	// and VolumeAttachment creation (CSI volumes).
	// Optional — if nil, those volume types are unsupported.
	kubeClient kubernetes.Interface
}

// NewResolver creates a Resolver for the given pod UID.
// hostNodeName should be the real host node name (e.g., from os.Hostname()).
func NewResolver(
	baseDir, pawnName, podUID, hostNodeName string,
	cmLister listersv1.ConfigMapLister,
	secretLister listersv1.SecretLister,
	kubeClient kubernetes.Interface,
) *Resolver {
	return &Resolver{
		stateDir:     filepath.Join(baseDir, "pawns", pawnName, "pods", podUID, "volumes"),
		hostNodeName: hostNodeName,
		cmLister:     cmLister,
		secretLister: secretLister,
		kubeClient:   kubeClient,
	}
}

// Resolve returns the BindMounts for the given container within a pod.
// It creates any necessary host-side directories/files.
func (r *Resolver) Resolve(ctx context.Context, pod *corev1.Pod, container *corev1.Container) ([]perigeos.BindMount, error) {
	// Build a map from volume name → volume spec for quick lookup.
	volByName := make(map[string]*corev1.Volume, len(pod.Spec.Volumes))
	for i := range pod.Spec.Volumes {
		volByName[pod.Spec.Volumes[i].Name] = &pod.Spec.Volumes[i]
	}

	var mounts []perigeos.BindMount
	for _, vm := range container.VolumeMounts {
		vol, ok := volByName[vm.Name]
		if !ok {
			return nil, fmt.Errorf("volume %q referenced by container %q not found in pod spec", vm.Name, container.Name)
		}

		bm, err := r.resolveVolume(ctx, pod, vol, vm)
		if err != nil {
			return nil, fmt.Errorf("resolve volume %q: %w", vol.Name, err)
		}
		mounts = append(mounts, bm)
	}
	return mounts, nil
}

// Cleanup removes host-side state (emptyDir, projected, CSI mounts) for a pod.
func (r *Resolver) Cleanup() error {
	return os.RemoveAll(r.stateDir)
}

// CleanupCSI performs CSI-specific cleanup: NodeUnpublishVolume, NodeUnstageVolume,
// and VolumeAttachment deletion. It requires the pod and kubeClient to identify which
// volumes are CSI-backed.
func (r *Resolver) CleanupCSI(ctx context.Context, pod *corev1.Pod) error {
	if r.kubeClient == nil || pod == nil {
		return nil // No cleanup needed if no kubeClient or pod
	}

	// Iterate pod volumes and clean up any CSI volumes.
	for _, vol := range pod.Spec.Volumes {
		if vol.PersistentVolumeClaim == nil {
			continue
		}

		// Get the PV to check if it's CSI
		pvc, err := r.kubeClient.CoreV1().PersistentVolumeClaims(pod.Namespace).Get(ctx, vol.PersistentVolumeClaim.ClaimName, metav1.GetOptions{})
		if err != nil {
			// If we can't find the PVC, skip cleanup for this volume
			continue
		}

		pvName := pvc.Spec.VolumeName
		pv, err := r.kubeClient.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
		if err != nil || pv.Spec.CSI == nil {
			// Not a CSI volume or can't get it, skip
			continue
		}

		// This is a CSI volume, clean it up
		if err := r.cleanupCSIVolume(ctx, pv); err != nil {
			// Log but don't fail the overall cleanup
			// (in production, you'd use a logger here)
			_ = err
		}
	}

	return nil
}

// cleanupCSIVolume performs cleanup for a single CSI volume.
func (r *Resolver) cleanupCSIVolume(ctx context.Context, pv *corev1.PersistentVolume) error {
	driverName := pv.Spec.CSI.Driver
	volumeID := pv.Spec.CSI.VolumeHandle
	pvName := pv.Name

	stagingPath := filepath.Join("/var/lib/kubelet/plugins/kubernetes.io/csi", pvName, "globalmount")
	targetPath := filepath.Join(r.stateDir, "csi", pvName, "mount")

	// Create CSI client
	csiClient, err := NewCSIClient(driverName)
	if err != nil {
		// If we can't connect to the CSI driver, try to clean up the directories anyway
		// The VolumeAttachment will eventually be garbage collected
		_ = os.RemoveAll(stagingPath)
		_ = os.RemoveAll(targetPath)
		return nil
	}
	defer csiClient.Close()

	// Call NodeUnpublishVolume
	_ = csiClient.NodeUnpublishVolume(ctx, volumeID, targetPath)

	// Call NodeUnstageVolume
	_ = csiClient.NodeUnstageVolume(ctx, volumeID, stagingPath)

	// Clean up directories
	_ = os.RemoveAll(stagingPath)
	_ = os.RemoveAll(targetPath)

	// Try to delete the VolumeAttachment
	vaName := sha256Hash(pvName + "/" + r.hostNodeName)
	_ = r.kubeClient.StorageV1().VolumeAttachments().Delete(ctx, vaName, metav1.DeleteOptions{})

	return nil
}

func (r *Resolver) resolveVolume(
	ctx context.Context,
	pod *corev1.Pod,
	vol *corev1.Volume,
	vm corev1.VolumeMount,
) (perigeos.BindMount, error) {
	namespace := pod.Namespace
	propagation := ""
	if vm.MountPropagation != nil {
		propagation = string(*vm.MountPropagation)
	}

	base := perigeos.BindMount{
		ContainerPath: vm.MountPath,
		ReadOnly:      vm.ReadOnly,
		Propagation:   propagation,
	}

	switch {
	case vol.HostPath != nil:
		hostPath := vol.HostPath.Path
		if err := ensurePath(hostPath, vol.HostPath.Type); err != nil {
			return perigeos.BindMount{}, err
		}
		base.HostPath = hostPath
		return base, nil

	case vol.EmptyDir != nil:
		dir := filepath.Join(r.stateDir, "emptydir", vol.Name)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return perigeos.BindMount{}, fmt.Errorf("create emptyDir %s: %w", dir, err)
		}
		base.HostPath = dir
		return base, nil

	case vol.ConfigMap != nil:
		dir, err := r.writeConfigMap(ctx, namespace, vol)
		if err != nil {
			return perigeos.BindMount{}, err
		}
		base.HostPath = dir
		base.ReadOnly = true // configMaps are always ro
		return base, nil

	case vol.Secret != nil:
		dir, err := r.writeSecret(ctx, namespace, vol)
		if err != nil {
			return perigeos.BindMount{}, err
		}
		base.HostPath = dir
		base.ReadOnly = true
		return base, nil

	case vol.PersistentVolumeClaim != nil:
		hostPath, err := r.resolvePVC(ctx, namespace, vol.PersistentVolumeClaim.ClaimName)
		if err != nil {
			return perigeos.BindMount{}, err
		}
		base.HostPath = hostPath
		if vol.PersistentVolumeClaim.ReadOnly {
			base.ReadOnly = true
		}
		return base, nil

	case vol.Projected != nil:
		dir, err := r.writeProjected(ctx, pod, vol)
		if err != nil {
			return perigeos.BindMount{}, err
		}
		base.HostPath = dir
		base.ReadOnly = true
		return base, nil

	default:
		return perigeos.BindMount{}, fmt.Errorf("unsupported volume type for %q", vol.Name)
	}
}

// resolvePVC looks up a PVC and its bound PV, returning the hostPath of the
// underlying volume. Supports hostPath, local, and CSI PV types.
func (r *Resolver) resolvePVC(ctx context.Context, namespace, claimName string) (string, error) {
	if r.kubeClient == nil {
		return "", fmt.Errorf("PVC volumes require kubeClient")
	}

	pvc, err := r.kubeClient.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, claimName, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("get PVC %s/%s: %w", namespace, claimName, err)
	}
	if pvc.Status.Phase != corev1.ClaimBound {
		err := fmt.Errorf("PVC %s/%s is not bound (phase: %s)", namespace, claimName, pvc.Status.Phase)
		// Pending phase is transient — the provisioner is working on creating the PV
		if pvc.Status.Phase == corev1.ClaimPending {
			return "", errdefs.AsTransient(err)
		}
		// Other phases (Lost, etc.) are not transient
		return "", err
	}

	pvName := pvc.Spec.VolumeName
	pv, err := r.kubeClient.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("get PV %s: %w", pvName, err)
	}

	// local-path-provisioner creates hostPath PVs.
	if pv.Spec.HostPath != nil {
		return pv.Spec.HostPath.Path, nil
	}
	// Also handle local PVs (e.g. from local volume provisioner).
	if pv.Spec.Local != nil {
		return pv.Spec.Local.Path, nil
	}
	// Handle CSI PVs.
	if pv.Spec.CSI != nil {
		return r.resolveCSI(ctx, pvName, pv)
	}

	return "", fmt.Errorf("PV %s has unsupported type (only hostPath, local, and CSI are supported)", pvName)
}

// resolveCSI handles CSI volume mounting. It:
// 1. Creates or gets a VolumeAttachment for the host node
// 2. Waits for the attach to complete
// 3. Calls NodeStageVolume and NodePublishVolume via the CSI driver
// 4. Returns the target mount path for bind-mounting into the container
func (r *Resolver) resolveCSI(ctx context.Context, pvName string, pv *corev1.PersistentVolume) (string, error) {
	if r.hostNodeName == "" {
		return "", fmt.Errorf("CSI volumes require hostNodeName")
	}

	driverName := pv.Spec.CSI.Driver
	volumeID := pv.Spec.CSI.VolumeHandle

	// Create VolumeAttachment for the host node (deterministic name based on PV).
	vaName := sha256Hash(pvName + "/" + r.hostNodeName)
	va := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{
			Name: vaName,
		},
		Spec: storagev1.VolumeAttachmentSpec{
			Attacher: driverName,
			Source: storagev1.VolumeAttachmentSource{
				PersistentVolumeName: &pvName,
			},
			NodeName: r.hostNodeName,
		},
	}

	// Create or update the VolumeAttachment.
	existingVA, err := r.kubeClient.StorageV1().VolumeAttachments().Get(ctx, vaName, metav1.GetOptions{})
	if err == nil {
		va = existingVA // Use existing VA if it already exists
	} else {
		va, err = r.kubeClient.StorageV1().VolumeAttachments().Create(ctx, va, metav1.CreateOptions{})
		if err != nil {
			return "", fmt.Errorf("create VolumeAttachment for PV %s: %w", pvName, err)
		}
	}

	// Poll until the VolumeAttachment is attached.
	if err := waitForAttachment(ctx, r.kubeClient, vaName); err != nil {
		return "", fmt.Errorf("wait for CSI attach for PV %s: %w", pvName, err)
	}

	// Refresh the VolumeAttachment to get the publish context.
	va, err = r.kubeClient.StorageV1().VolumeAttachments().Get(ctx, vaName, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("get VolumeAttachment for PV %s: %w", pvName, err)
	}

	publishContext := map[string]string{}
	if va.Status.AttachmentMetadata != nil {
		publishContext = va.Status.AttachmentMetadata
	}

	// Create the CSI client and mount the volume.
	csiClient, err := NewCSIClient(driverName)
	if err != nil {
		return "", fmt.Errorf("create CSI client for driver %s: %w", driverName, err)
	}
	defer csiClient.Close()

	stagingPath := filepath.Join("/var/lib/kubelet/plugins/kubernetes.io/csi", pvName, "globalmount")
	targetPath := filepath.Join(r.stateDir, "csi", pvName, "mount")

	// Create staging and target directories.
	if err := os.MkdirAll(stagingPath, 0o750); err != nil {
		return "", fmt.Errorf("create staging path %s: %w", stagingPath, err)
	}
	if err := os.MkdirAll(targetPath, 0o750); err != nil {
		return "", fmt.Errorf("create target path %s: %w", targetPath, err)
	}

	// Call NodeStageVolume.
	volumeContext := pv.Spec.CSI.VolumeAttributes
	if volumeContext == nil {
		volumeContext = make(map[string]string)
	}

	if err := csiClient.NodeStageVolume(ctx, volumeID, stagingPath, pv.Spec.AccessModes, volumeContext, publishContext, map[string]string{}); err != nil {
		return "", fmt.Errorf("NodeStageVolume for PV %s: %w", pvName, err)
	}

	// Call NodePublishVolume.
	readOnly := containsReadOnlyMode(pv.Spec.AccessModes)
	if err := csiClient.NodePublishVolume(ctx, volumeID, stagingPath, targetPath, pv.Spec.AccessModes, volumeContext, publishContext, readOnly); err != nil {
		// Clean up staging if publish fails.
		_ = csiClient.NodeUnstageVolume(ctx, volumeID, stagingPath)
		return "", fmt.Errorf("NodePublishVolume for PV %s: %w", pvName, err)
	}

	return targetPath, nil
}

// writeConfigMap materialises a ConfigMap volume to a host directory.
func (r *Resolver) writeConfigMap(ctx context.Context, namespace string, vol *corev1.Volume) (string, error) {
	cmName := vol.ConfigMap.Name
	var cm *corev1.ConfigMap
	if r.cmLister != nil {
		var err error
		cm, err = r.cmLister.ConfigMaps(namespace).Get(cmName)
		if err != nil {
			cm = nil // fall through to API call
		}
	}
	if cm == nil {
		if r.kubeClient == nil {
			return "", fmt.Errorf("get configMap %s/%s: not in cache and no kubeClient", namespace, cmName)
		}
		var err error
		cm, err = r.kubeClient.CoreV1().ConfigMaps(namespace).Get(ctx, cmName, metav1.GetOptions{})
		if err != nil {
			return "", fmt.Errorf("get configMap %s/%s: %w", namespace, cmName, err)
		}
	}

	dir := filepath.Join(r.stateDir, "configmap", vol.Name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("create configMap dir: %w", err)
	}

	// Determine which keys to project.
	keyToPath := make(map[string]string)
	if len(vol.ConfigMap.Items) > 0 {
		for _, item := range vol.ConfigMap.Items {
			keyToPath[item.Key] = item.Path
		}
	} else {
		for k := range cm.Data {
			keyToPath[k] = k
		}
		for k := range cm.BinaryData {
			keyToPath[k] = k
		}
	}

	for key, path := range keyToPath {
		dest := filepath.Join(dir, path)
		if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
			return "", err
		}
		if data, ok := cm.Data[key]; ok {
			if err := os.WriteFile(dest, []byte(data), 0o644); err != nil {
				return "", fmt.Errorf("write configMap key %s: %w", key, err)
			}
		} else if data, ok := cm.BinaryData[key]; ok {
			if err := os.WriteFile(dest, data, 0o644); err != nil {
				return "", fmt.Errorf("write configMap binary key %s: %w", key, err)
			}
		}
	}
	return dir, nil
}

// writeSecret materialises a Secret volume to a host directory.
func (r *Resolver) writeSecret(_ context.Context, namespace string, vol *corev1.Volume) (string, error) {
	if r.secretLister == nil {
		return "", fmt.Errorf("secret volumes not supported: no lister")
	}
	secretName := vol.Secret.SecretName
	secret, err := r.secretLister.Secrets(namespace).Get(secretName)
	if err != nil {
		return "", fmt.Errorf("get secret %s/%s: %w", namespace, secretName, err)
	}

	dir := filepath.Join(r.stateDir, "secret", vol.Name)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("create secret dir: %w", err)
	}

	keyToPath := make(map[string]string)
	if len(vol.Secret.Items) > 0 {
		for _, item := range vol.Secret.Items {
			keyToPath[item.Key] = item.Path
		}
	} else {
		for k := range secret.Data {
			keyToPath[k] = k
		}
	}

	for key, path := range keyToPath {
		dest := filepath.Join(dir, path)
		if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
			return "", err
		}
		if err := os.WriteFile(dest, secret.Data[key], 0o600); err != nil {
			return "", fmt.Errorf("write secret key %s: %w", key, err)
		}
	}
	return dir, nil
}

// writeProjected materialises a projected volume (kube-api-access pattern)
// to a host directory. Handles ServiceAccountToken, ConfigMap, and DownwardAPI
// sources — which covers the default kube-api-access volume injected by k8s.
func (r *Resolver) writeProjected(ctx context.Context, pod *corev1.Pod, vol *corev1.Volume) (string, error) {
	dir := filepath.Join(r.stateDir, "projected", vol.Name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("create projected dir: %w", err)
	}

	for _, src := range vol.Projected.Sources {
		switch {
		case src.ServiceAccountToken != nil:
			token, err := r.requestToken(ctx, pod, src.ServiceAccountToken)
			if err != nil {
				return "", fmt.Errorf("token request: %w", err)
			}
			dest := filepath.Join(dir, src.ServiceAccountToken.Path)
			if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
				return "", err
			}
			if err := os.WriteFile(dest, []byte(token), 0o600); err != nil {
				return "", fmt.Errorf("write token: %w", err)
			}

		case src.ConfigMap != nil:
			if r.cmLister == nil && r.kubeClient == nil {
				return "", fmt.Errorf("projected configMap source requires cm lister or kubeClient")
			}
			var cmData map[string]string
			var cmBinaryData map[string][]byte
			if r.cmLister != nil {
				cm, err := r.cmLister.ConfigMaps(pod.Namespace).Get(src.ConfigMap.Name)
				if err == nil {
					cmData = cm.Data
					cmBinaryData = cm.BinaryData
				}
			}
			// Fall back to direct API call if lister missed it (e.g. cache not yet
			// populated for this namespace, or kube-root-ca.crt not yet replicated).
			if cmData == nil && r.kubeClient != nil {
				cm, err := r.kubeClient.CoreV1().ConfigMaps(pod.Namespace).Get(ctx, src.ConfigMap.Name, metav1.GetOptions{})
				if err != nil {
					return "", fmt.Errorf("get projected configMap %s: %w", src.ConfigMap.Name, err)
				}
				cmData = cm.Data
				cmBinaryData = cm.BinaryData
			}
			if cmData == nil {
				return "", fmt.Errorf("projected configMap %s not found", src.ConfigMap.Name)
			}
			items := src.ConfigMap.Items
			if len(items) == 0 {
				for k := range cmData {
					items = append(items, corev1.KeyToPath{Key: k, Path: k})
				}
			}
			for _, item := range items {
				dest := filepath.Join(dir, item.Path)
				if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
					return "", err
				}
				data := []byte(cmData[item.Key])
				if bd, ok := cmBinaryData[item.Key]; ok {
					data = bd
				}
				if err := os.WriteFile(dest, data, 0o644); err != nil {
					return "", fmt.Errorf("write projected configMap key %s: %w", item.Key, err)
				}
			}

		case src.DownwardAPI != nil:
			for _, item := range src.DownwardAPI.Items {
				dest := filepath.Join(dir, item.Path)
				if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
					return "", err
				}
				value, err := resolveDownwardAPIField(pod, item)
				if err != nil {
					return "", fmt.Errorf("downward API field %s: %w", item.Path, err)
				}
				if err := os.WriteFile(dest, []byte(value), 0o644); err != nil {
					return "", fmt.Errorf("write downward API %s: %w", item.Path, err)
				}
			}

		case src.Secret != nil:
			if r.secretLister == nil {
				return "", fmt.Errorf("projected secret source requires secret lister")
			}
			secret, err := r.secretLister.Secrets(pod.Namespace).Get(src.Secret.Name)
			if err != nil {
				return "", fmt.Errorf("get projected secret %s: %w", src.Secret.Name, err)
			}
			items := src.Secret.Items
			if len(items) == 0 {
				for k := range secret.Data {
					items = append(items, corev1.KeyToPath{Key: k, Path: k})
				}
			}
			for _, item := range items {
				dest := filepath.Join(dir, item.Path)
				if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
					return "", err
				}
				if err := os.WriteFile(dest, secret.Data[item.Key], 0o600); err != nil {
					return "", fmt.Errorf("write projected secret key %s: %w", item.Key, err)
				}
			}
		}
	}
	return dir, nil
}

// requestToken calls the TokenRequest API to get a bound service account token.
func (r *Resolver) requestToken(ctx context.Context, pod *corev1.Pod, src *corev1.ServiceAccountTokenProjection) (string, error) {
	if r.kubeClient == nil {
		return "", fmt.Errorf("TokenRequest requires kubeClient")
	}
	sa := pod.Spec.ServiceAccountName
	if sa == "" {
		sa = "default"
	}
	req := &authv1.TokenRequest{
		Spec: authv1.TokenRequestSpec{
			ExpirationSeconds: src.ExpirationSeconds,
			BoundObjectRef: &authv1.BoundObjectReference{
				APIVersion: "v1",
				Kind:       "Pod",
				Name:       pod.Name,
				UID:        pod.UID,
			},
		},
	}
	if src.Audience != "" {
		req.Spec.Audiences = []string{src.Audience}
	}
	if len(req.Spec.Audiences) == 0 {
		req.Spec.Audiences = []string{"https://kubernetes.default.svc.cluster.local"}
	}
	resp, err := r.kubeClient.CoreV1().ServiceAccounts(pod.Namespace).
		CreateToken(ctx, sa, req, metav1.CreateOptions{})
	if err != nil {
		return "", fmt.Errorf("CreateToken for %s/%s: %w", pod.Namespace, sa, err)
	}
	return resp.Status.Token, nil
}

// resolveDownwardAPIField returns the string value for a DownwardAPI volume item.
func resolveDownwardAPIField(pod *corev1.Pod, item corev1.DownwardAPIVolumeFile) (string, error) {
	if item.FieldRef == nil {
		return "", fmt.Errorf("only fieldRef downward API supported (no resourceFieldRef)")
	}
	switch item.FieldRef.FieldPath {
	case "metadata.namespace":
		return pod.Namespace, nil
	case "metadata.name":
		return pod.Name, nil
	case "metadata.uid":
		return string(pod.UID), nil
	case "metadata.labels":
		return labelsToString(pod.Labels), nil
	case "metadata.annotations":
		return labelsToString(pod.Annotations), nil
	case "spec.serviceAccountName":
		return pod.Spec.ServiceAccountName, nil
	case "spec.nodeName":
		return pod.Spec.NodeName, nil
	case "status.podIP":
		return pod.Status.PodIP, nil
	case "status.hostIP":
		return pod.Status.HostIP, nil
	default:
		return "", fmt.Errorf("unsupported fieldPath %q", item.FieldRef.FieldPath)
	}
}

func labelsToString(m map[string]string) string {
	result := ""
	for k, v := range m {
		result += k + "=" + v + "\n"
	}
	return result
}


// RefreshConfigMapDirect rewrites a previously-materialised ConfigMap volume in-place
// using the provided ConfigMap object directly (not from the lister cache).
// Files are truncated+written (same inode) so inotify IN_MODIFY propagates
// through the bind mount into the container. New keys are created; stale keys
// are removed.
func RefreshConfigMapDirect(cm *corev1.ConfigMap, vol *corev1.Volume, hostDir string) error {
	keyToPath := make(map[string]string)
	if len(vol.ConfigMap.Items) > 0 {
		for _, item := range vol.ConfigMap.Items {
			keyToPath[item.Key] = item.Path
		}
	} else {
		for k := range cm.Data {
			keyToPath[k] = k
		}
		for k := range cm.BinaryData {
			keyToPath[k] = k
		}
	}

	// Write current keys (truncate existing files to preserve inode).
	written := make(map[string]bool, len(keyToPath))
	for key, path := range keyToPath {
		dest := filepath.Join(hostDir, path)
		written[dest] = true
		if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
			return err
		}
		var data []byte
		if s, ok := cm.Data[key]; ok {
			data = []byte(s)
		} else if b, ok := cm.BinaryData[key]; ok {
			data = b
		}
		if err := truncateWrite(dest, data, 0o644); err != nil {
			return fmt.Errorf("refresh configMap key %s: %w", key, err)
		}
	}

	return removeStaleFiles(hostDir, written)
}

// RefreshSecretDirect rewrites a previously-materialised Secret volume in-place
// using the provided Secret object directly (not from the lister cache).
func RefreshSecretDirect(secret *corev1.Secret, vol *corev1.Volume, hostDir string) error {
	keyToPath := make(map[string]string)
	if len(vol.Secret.Items) > 0 {
		for _, item := range vol.Secret.Items {
			keyToPath[item.Key] = item.Path
		}
	} else {
		for k := range secret.Data {
			keyToPath[k] = k
		}
	}

	written := make(map[string]bool, len(keyToPath))
	for key, path := range keyToPath {
		dest := filepath.Join(hostDir, path)
		written[dest] = true
		if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
			return err
		}
		if err := truncateWrite(dest, secret.Data[key], 0o600); err != nil {
			return fmt.Errorf("refresh secret key %s: %w", key, err)
		}
	}

	return removeStaleFiles(hostDir, written)
}

// truncateWrite opens an existing file with O_TRUNC to preserve the inode
// (so inotify fires IN_MODIFY through bind mounts), or creates it if new.
func truncateWrite(path string, data []byte, perm os.FileMode) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	_, writeErr := f.Write(data)
	closeErr := f.Close()
	if writeErr != nil {
		return writeErr
	}
	return closeErr
}

// removeStaleFiles walks hostDir and removes any files not in the keep set.
func removeStaleFiles(hostDir string, keep map[string]bool) error {
	entries, err := os.ReadDir(hostDir)
	if err != nil {
		return nil // dir may not exist yet
	}
	for _, e := range entries {
		path := filepath.Join(hostDir, e.Name())
		if e.IsDir() {
			if err := removeStaleFiles(path, keep); err != nil {
				return err
			}
			sub, _ := os.ReadDir(path)
			if len(sub) == 0 {
				os.Remove(path)
			}
			continue
		}
		if !keep[path] {
			_ = os.Truncate(path, 0)
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("remove stale key %s: %w", e.Name(), err)
			}
		}
	}
	return nil
}

func ensurePath(path string, t *corev1.HostPathType) error {
	if t == nil || *t == corev1.HostPathUnset || *t == corev1.HostPathDirectory || *t == corev1.HostPathFile {
		// Must already exist — don't create.
		return nil
	}
	switch *t {
	case corev1.HostPathDirectoryOrCreate:
		return os.MkdirAll(path, 0o755)
	case corev1.HostPathFileOrCreate:
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return err
		}
		f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL, 0o644)
		if err != nil && !os.IsExist(err) {
			return err
		}
		if f != nil {
			f.Close()
		}
	}
	return nil
}

// sha256Hash returns a short deterministic hash of the input string.
func sha256Hash(input string) string {
	h := sha256.Sum256([]byte(input))
	return fmt.Sprintf("%x", h)[:16] // Return first 16 hex chars
}

// containsReadOnlyMode checks if the AccessModes list contains a read-only mode.
func containsReadOnlyMode(modes []corev1.PersistentVolumeAccessMode) bool {
	for _, mode := range modes {
		if mode == corev1.ReadOnlyMany {
			return true
		}
	}
	return false
}

// waitForAttachment polls until a VolumeAttachment is marked as attached,
// with a 30-second timeout.
func waitForAttachment(ctx context.Context, kubeClient kubernetes.Interface, vaName string) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for attachment timed out: %w", ctx.Err())
		case <-ticker.C:
			va, err := kubeClient.StorageV1().VolumeAttachments().Get(ctx, vaName, metav1.GetOptions{})
			if err != nil {
				return fmt.Errorf("get VolumeAttachment: %w", err)
			}
			if va.Status.Attached {
				return nil
			}
		}
	}
}
