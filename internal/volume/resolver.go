// Package volume resolves Kubernetes pod volume specs + container volume mounts
// into concrete host→container bind mounts for systemd-nspawn.
//
// Supported volume types:
//   - hostPath  — direct bind mount of a host path
//   - emptyDir  — host-side tmpfs/dir created under the pod's state dir
//   - configMap — files written to a host-side dir, then bind-mounted read-only
//   - secret    — files written to a host-side dir, then bind-mounted read-only
//   - projected — kube-api-access (service account token + CA cert + downward API)
//   - persistentVolumeClaim — resolved to the underlying PV hostPath (local-path and local PV types)
package volume

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	authv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	listersv1 "k8s.io/client-go/listers/core/v1"

	pruntime "github.com/malformed-c/periapsis/internal/runtime"
)

// Resolver resolves pod volume declarations into BindMounts.
type Resolver struct {
	// stateDir is the per-pod directory on the host used for emptyDir and
	// projected volumes: <baseDir>/pawns/<pawn>/pods/<uid>/volumes/
	stateDir string

	// cmLister and secretLister are optional top-level listers.
	// They are scoped to the pod's namespace at resolution time.
	cmLister     listersv1.ConfigMapLister
	secretLister listersv1.SecretLister

	// kubeClient is used for TokenRequest (projected service account volumes).
	// Optional — if nil, projected volumes fall back to an empty token.
	kubeClient kubernetes.Interface
}

// NewResolver creates a Resolver for the given pod UID.
func NewResolver(
	baseDir, pawnName, podUID string,
	cmLister listersv1.ConfigMapLister,
	secretLister listersv1.SecretLister,
	kubeClient kubernetes.Interface,
) *Resolver {
	return &Resolver{
		stateDir:     filepath.Join(baseDir, "pawns", pawnName, "pods", podUID, "volumes"),
		cmLister:     cmLister,
		secretLister: secretLister,
		kubeClient:   kubeClient,
	}
}

// Resolve returns the BindMounts for the given container within a pod.
// It creates any necessary host-side directories/files.
func (r *Resolver) Resolve(ctx context.Context, pod *corev1.Pod, container *corev1.Container) ([]pruntime.BindMount, error) {
	// Build a map from volume name → volume spec for quick lookup.
	volByName := make(map[string]*corev1.Volume, len(pod.Spec.Volumes))
	for i := range pod.Spec.Volumes {
		volByName[pod.Spec.Volumes[i].Name] = &pod.Spec.Volumes[i]
	}

	var mounts []pruntime.BindMount
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

// Cleanup removes host-side state (emptyDir, projected) for a pod.
func (r *Resolver) Cleanup() error {
	return os.RemoveAll(r.stateDir)
}

func (r *Resolver) resolveVolume(
	ctx context.Context,
	pod *corev1.Pod,
	vol *corev1.Volume,
	vm corev1.VolumeMount,
) (pruntime.BindMount, error) {
	namespace := pod.Namespace
	propagation := ""
	if vm.MountPropagation != nil {
		propagation = string(*vm.MountPropagation)
	}

	base := pruntime.BindMount{
		ContainerPath: vm.MountPath,
		ReadOnly:      vm.ReadOnly,
		Propagation:   propagation,
	}

	switch {
	case vol.HostPath != nil:
		hostPath := vol.HostPath.Path
		if err := ensurePath(hostPath, vol.HostPath.Type); err != nil {
			return pruntime.BindMount{}, err
		}
		base.HostPath = hostPath
		return base, nil

	case vol.EmptyDir != nil:
		dir := filepath.Join(r.stateDir, "emptydir", vol.Name)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return pruntime.BindMount{}, fmt.Errorf("create emptyDir %s: %w", dir, err)
		}
		base.HostPath = dir
		return base, nil

	case vol.ConfigMap != nil:
		dir, err := r.writeConfigMap(ctx, namespace, vol)
		if err != nil {
			return pruntime.BindMount{}, err
		}
		base.HostPath = dir
		base.ReadOnly = true // configMaps are always ro
		return base, nil

	case vol.Secret != nil:
		dir, err := r.writeSecret(ctx, namespace, vol)
		if err != nil {
			return pruntime.BindMount{}, err
		}
		base.HostPath = dir
		base.ReadOnly = true
		return base, nil

	case vol.PersistentVolumeClaim != nil:
		hostPath, err := r.resolvePVC(ctx, namespace, vol.PersistentVolumeClaim.ClaimName)
		if err != nil {
			return pruntime.BindMount{}, err
		}
		base.HostPath = hostPath
		if vol.PersistentVolumeClaim.ReadOnly {
			base.ReadOnly = true
		}
		return base, nil

	case vol.Projected != nil:
		dir, err := r.writeProjected(ctx, pod, vol)
		if err != nil {
			return pruntime.BindMount{}, err
		}
		base.HostPath = dir
		base.ReadOnly = true
		return base, nil

	default:
		return pruntime.BindMount{}, fmt.Errorf("unsupported volume type for %q", vol.Name)
	}
}

// resolvePVC looks up a PVC and its bound PV, returning the hostPath of the
// underlying volume. Only hostPath and local PV types are supported — these
// are what local-path-provisioner creates.
func (r *Resolver) resolvePVC(ctx context.Context, namespace, claimName string) (string, error) {
	if r.kubeClient == nil {
		return "", fmt.Errorf("PVC volumes require kubeClient")
	}

	pvc, err := r.kubeClient.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, claimName, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("get PVC %s/%s: %w", namespace, claimName, err)
	}
	if pvc.Status.Phase != corev1.ClaimBound {
		return "", fmt.Errorf("PVC %s/%s is not bound (phase: %s)", namespace, claimName, pvc.Status.Phase)
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

	return "", fmt.Errorf("PV %s has unsupported type (only hostPath and local are supported)", pvName)
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
