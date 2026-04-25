// Copyright (C) 2025-2026 Malformed C. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-only

package node

import (
	"path/filepath"
	"sync"

	"log/slog"

	corev1 "k8s.io/api/core/v1"

	"github.com/malformed-c/periapsis/internal/volume"
)

// volumeMount tracks a single mounted ConfigMap/Secret for a running pod.
type volumeMount struct {
	podUID  string
	hostDir string         // host path where files were written
	vol     *corev1.Volume // original volume spec (for Items filtering)
}

// VolumeTracker maintains the reverse index from "kind:namespace/name" to the
// list of running pods that have that ConfigMap or Secret mounted. It enables
// O(1) live refresh when an informer fires an update event.
type VolumeTracker struct {
	mu           sync.RWMutex
	volRefs      map[string][]volumeMount // "kind:ns/name" -> mounts
	volRefsByPod map[string][]string      // podUID -> keys for cleanup

	baseDir  string
	pawnName string
	logger   *slog.Logger
}

// NewVolumeTracker creates a VolumeTracker.
func NewVolumeTracker(baseDir, pawnName string, logger *slog.Logger) *VolumeTracker {
	return &VolumeTracker{
		volRefs:      make(map[string][]volumeMount),
		volRefsByPod: make(map[string][]string),
		baseDir:      baseDir,
		pawnName:     pawnName,
		logger:       logger,
	}
}

// Track scans a pod's volumes for ConfigMap and Secret types and registers
// them in the reverse index. Safe to call concurrently.
func (vt *VolumeTracker) Track(uid string, pod *corev1.Pod) {
	var keys []string
	var mounts []struct {
		key   string
		mount volumeMount
	}

	for i := range pod.Spec.Volumes {
		vol := &pod.Spec.Volumes[i]
		var kind, name string
		switch {
		case vol.ConfigMap != nil:
			kind = "configmap"
			name = vol.ConfigMap.Name
		case vol.Secret != nil:
			kind = "secret"
			name = vol.Secret.SecretName
		default:
			continue
		}
		key := kind + ":" + pod.Namespace + "/" + name
		hostDir := filepath.Join(vt.baseDir, "pawns", vt.pawnName, "pods", uid, "volumes", kind, vol.Name)
		mounts = append(mounts, struct {
			key   string
			mount volumeMount
		}{key, volumeMount{podUID: uid, hostDir: hostDir, vol: vol}})
		keys = append(keys, key)
	}

	if len(keys) == 0 {
		return
	}

	vt.mu.Lock()
	defer vt.mu.Unlock()
	for _, m := range mounts {
		vt.volRefs[m.key] = append(vt.volRefs[m.key], m.mount)
	}
	vt.volRefsByPod[uid] = keys
}

// Untrack removes all volume reference entries for a pod.
func (vt *VolumeTracker) Untrack(uid string) {
	vt.mu.Lock()
	defer vt.mu.Unlock()
	keys := vt.volRefsByPod[uid]
	for _, key := range keys {
		mounts := vt.volRefs[key]
		filtered := mounts[:0]
		for _, m := range mounts {
			if m.podUID != uid {
				filtered = append(filtered, m)
			}
		}
		if len(filtered) == 0 {
			delete(vt.volRefs, key)
		} else {
			vt.volRefs[key] = filtered
		}
	}
	delete(vt.volRefsByPod, uid)
}

// RefreshConfigMap rewrites ConfigMap volume files for all pods that have it mounted.
func (vt *VolumeTracker) RefreshConfigMap(cm *corev1.ConfigMap) {
	key := "configmap:" + cm.Namespace + "/" + cm.Name
	vt.mu.RLock()
	mounts := make([]volumeMount, len(vt.volRefs[key]))
	copy(mounts, vt.volRefs[key])
	vt.mu.RUnlock()

	if len(mounts) == 0 {
		return
	}

	vt.logger.Info("Refreshing volume", "kind", "configmap", "name", cm.Name, "pods", len(mounts))
	for _, m := range mounts {
		if err := volume.RefreshConfigMapDirect(cm, m.vol, m.hostDir); err != nil {
			vt.logger.Warn("Failed to refresh volume", "kind", "configmap", "name", cm.Name, "pod", m.podUID, "err", err)
		}
	}
}

// RefreshSecret rewrites Secret volume files for all pods that have it mounted.
func (vt *VolumeTracker) RefreshSecret(secret *corev1.Secret) {
	key := "secret:" + secret.Namespace + "/" + secret.Name
	vt.mu.RLock()
	mounts := make([]volumeMount, len(vt.volRefs[key]))
	copy(mounts, vt.volRefs[key])
	vt.mu.RUnlock()

	if len(mounts) == 0 {
		return
	}

	vt.logger.Info("Refreshing volume", "kind", "secret", "name", secret.Name, "pods", len(mounts))
	for _, m := range mounts {
		if err := volume.RefreshSecretDirect(secret, m.vol, m.hostDir); err != nil {
			vt.logger.Warn("Failed to refresh volume", "kind", "secret", "name", secret.Name, "pod", m.podUID, "err", err)
		}
	}
}
