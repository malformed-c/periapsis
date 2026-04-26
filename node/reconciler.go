// Copyright (C) 2025-2026 Malformed C. All rights reserved.
// SPDX-License-Identifier: BUSL-1.1

package node

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/malformed-c/periapsis/internal/image"
	"github.com/malformed-c/periapsis/internal/network"
	perigeos "github.com/malformed-c/periapsis/internal/runtime"
	"github.com/malformed-c/periapsis/internal/volume"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	v1 "k8s.io/client-go/listers/core/v1"
)

const (
	reconcileInterval = 5 * time.Minute
)

// PodTracker is the subset of Gambit that the Reconciler needs to
// avoid tearing down pods that are mid-creation or still registered.
type PodTracker interface {
	IsInFlight(uid string) bool
	HasPod(uid string) bool
	PodUIDs() map[string]string
	EvictGhost(uid string)
	// GetPodCopy returns a DeepCopy of the pod for the given uid, or nil if unknown.
	// Used by the Reconciler to get authoritative namespace/name for CNI teardown
	// rather than relying on systemd unit env vars which may be empty if the unit
	// has already been collected.
	GetPodCopy(uid string) *corev1.Pod
}

// Reconciler performs periodic drift correction between systemd's actual state
// and Kubernetes desired state. Its only job is to remove orphan machines -
// machines running in systemd that have no corresponding pod in Kubernetes.
//
// The Reconciler never creates pods. The VK PodController is the sole authority
// for pod creation. This eliminates the double-create race entirely.
type Reconciler struct {
	tracker       PodTracker
	runtime       perigeos.Runtime
	network       network.NetworkManager
	image         *image.ImageManager
	podLister     v1.PodNamespaceLister
	logger        *slog.Logger
	baseDir       string
	pawnName      string
	hostNodeName  string                       // Real host node name, used for CSI volume cleanup
	syncRequester func(namespace, name string) // forward reconciler callback
}

func NewReconciler(
	g *Gambit,
	rt perigeos.Runtime,
	nm network.NetworkManager,
	im *image.ImageManager,
	podLister v1.PodNamespaceLister,
	logger *slog.Logger,
) *Reconciler {
	return &Reconciler{
		tracker:       g,
		runtime:       rt,
		network:       nm,
		image:         im,
		podLister:     podLister,
		logger:        logger,
		baseDir:       g.Config.BaseDir,
		pawnName:      g.Config.Name,
		hostNodeName:  g.hostNodeName,
		syncRequester: g.RequestSync,
	}
}

// Run starts the reconciliation loop. It blocks until ctx is cancelled.
func (r *Reconciler) Run(ctx context.Context) {
	ticker := time.NewTicker(reconcileInterval)
	defer ticker.Stop()

	r.logger.Info("Reconciler started", "interval", reconcileInterval)

	for {
		select {
		case <-ctx.Done():
			r.logger.Info("Reconciler stopped")
			return
		case <-ticker.C:
			r.cleanOrphans(ctx)
			r.cleanGhosts(ctx)
			r.cleanStaleDirs()
		}
	}
}

// RunOnce runs a single reconciliation pass. Used in tests and on startup.
func (r *Reconciler) RunOnce(ctx context.Context) {
	r.cleanOrphans(ctx)
	r.cleanGhosts(ctx)
	r.cleanStaleDirs()
}

// cleanOrphans finds systemd machines that have no matching pod in Kubernetes
// and tears them down. Machines that are in-flight (being created by Gambit)
// or still visible in the K8s informer cache are skipped.
func (r *Reconciler) cleanOrphans(ctx context.Context) {
	r.logger.Info("Reconciler: scanning for orphan machines")

	machines, err := r.runtime.ListManagedMachines(ctx)
	if err != nil {
		r.logger.Error("Reconciler: failed to list machines", "err", err)
		return
	}

	for _, m := range machines {
		// 1. Skip machines currently being created by Gambit (in-process guard).
		if r.tracker.IsInFlight(m.UID) {
			r.logger.Debug("Reconciler: skipping in-flight machine", "uid", m.UID)
			continue
		}

		// No timestamp-based grace period needed: after a crash,
		// HydrateFromRuntime repopulates g.pods from systemd metadata
		// before the reconciler starts. The inFlight map handles the
		// normal (non-crash) case above.

		// 2. Skip if Gambit's in-memory state still knows about this pod.
		if r.tracker.HasPod(m.UID) {
			continue
		}

		// 3. Check the K8s informer cache. If the pod still exists in K8s
		//    but Gambit lost track of it, request a re-sync so the PodController
		//    re-drives creation (forward reconciler). If K8s doesn't know about
		//    it either, fall through to teardown.
		if r.podLister != nil {
			pods, err := r.podLister.List(labels.Everything())
			if err == nil {
				found := false
				for _, pod := range pods {
					if string(pod.UID) == m.UID {
						found = true
						r.logger.Info("Reconciler: machine exists in K8s but not in Gambit, requesting re-sync",
							"uid", m.UID, "namespace", m.Namespace, "name", m.Name)
						if r.syncRequester != nil {
							r.syncRequester(m.Namespace, m.Name)
						}
						break
					}
				}
				if found {
					continue
				}
			}
		}

		r.logger.Warn("Reconciler: orphan machine found, cleaning up",
			"uid", m.UID,
			"name", m.Name,
			"namespace", m.Namespace,
			"container", m.ContainerName,
		)

		r.teardown(ctx, m)
	}
}

// cleanGhosts removes pods from Gambit's in-memory map that Kubernetes
// no longer knows about. These are pods where the VK PodController never
// delivered a DeletePod call (e.g. informer event dropped under load).
// Also tears down leftover network namespaces via CNI DEL.
func (r *Reconciler) cleanGhosts(ctx context.Context) {
	if r.podLister == nil {
		return
	}

	gambitUIDs := r.tracker.PodUIDs()
	if len(gambitUIDs) == 0 {
		return
	}

	// Build set of UIDs that k8s still wants.
	k8sUIDs := make(map[string]struct{})
	pods, err := r.podLister.List(labels.Everything())
	if err != nil {
		r.logger.Error("Reconciler: failed to list k8s pods for ghost check", "err", err)
		return
	}
	for _, pod := range pods {
		k8sUIDs[string(pod.UID)] = struct{}{}
	}

	var evicted int
	for uid, nsName := range gambitUIDs {
		if r.tracker.IsInFlight(uid) {
			continue
		}
		if _, ok := k8sUIDs[uid]; ok {
			continue
		}
		r.logger.Warn("Reconciler: evicting ghost pod (in gambit but not in k8s)",
			"uid", uid, "name", nsName)

		// Tear down network namespace and CNI state.
		// nsName is "namespace/name" from PodUIDs().
		namespace, name := splitNsName(nsName)
		if err := r.network.Teardown(ctx, uid, namespace, name); err != nil {
			r.logger.Error("Reconciler: ghost network teardown failed",
				"uid", uid, "err", err)
		}

		r.tracker.EvictGhost(uid)
		evicted++
	}

	if evicted > 0 {
		r.logger.Info("Reconciler: evicted ghost pods", "count", evicted)
	}
}

// splitNsName splits "namespace/name" into its parts.
func splitNsName(nsName string) (string, string) {
	if before0, after, ok := strings.Cut(nsName, "/"); ok {
		return before0, after
	}
	return "", nsName
}

// cleanStaleDirs removes pod directories on disk that have no corresponding
// pod in Gambit's in-memory state. These accumulate when perigeos crashes
// after creating the dir but before completing cleanup, or when a pod is
// evicted without full teardown.
func (r *Reconciler) cleanStaleDirs() {
	podsDir := filepath.Join(r.baseDir, "pawns", r.pawnName, "pods")
	entries, err := os.ReadDir(podsDir)
	if err != nil {
		return
	}

	gambitUIDs := r.tracker.PodUIDs()
	var cleaned int
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		uid := e.Name()
		// Normalize: some dirs may have uid-suffix
		if len(uid) > 36 && uid[36] == '-' {
			uid = uid[:36]
		}
		if _, ok := gambitUIDs[uid]; ok {
			continue
		}
		if r.tracker.IsInFlight(uid) {
			continue
		}
		dir := filepath.Join(podsDir, e.Name())
		r.logger.Warn("Reconciler: removing stale pod dir", "uid", uid, "dir", dir)
		if err := os.RemoveAll(dir); err != nil {
			r.logger.Error("Reconciler: failed to remove stale dir", "dir", dir, "err", err)
		}
		cleaned++
	}
	if cleaned > 0 {
		r.logger.Info("Reconciler: cleaned stale pod dirs", "count", cleaned)
	}
}

func (r *Reconciler) teardown(ctx context.Context, m perigeos.PodMetadata) {
	if err := r.runtime.StopMachine(ctx, m.UID, m.ContainerName); err != nil {
		r.logger.Error("Reconciler: failed to stop machine", "uid", m.UID, "container", m.ContainerName, "err", err)
	}

	// Use authoritative namespace/name from PodStore when available.
	// PodMetadata.Namespace and .Name come from readUnitEnv which reads
	// PERIGEOS_META_* from the systemd unit's Environment property via D-Bus.
	// Under load or after the unit is collected, that D-Bus call fails silently
	// and returns empty strings. Cilium's CNI DEL then gets empty namespace/name,
	// returns 404, and the lxc veth + endpoint state leak permanently.
	namespace, name := m.Namespace, m.Name
	if pod := r.tracker.GetPodCopy(m.UID); pod != nil {
		namespace = pod.Namespace
		name = pod.Name
	}
	if namespace == "" || name == "" {
		r.logger.Warn("Reconciler: namespace/name missing for CNI teardown - lxc veth may leak",
			"uid", m.UID, "namespace", namespace, "name", name,
			"metaNamespace", m.Namespace, "metaName", m.Name)
	}

	if err := r.network.Teardown(ctx, m.UID, namespace, name); err != nil {
		r.logger.Error("Reconciler: failed to teardown network", "uid", m.UID, "namespace", namespace, "name", name, "err", err)
	}
	if err := r.image.Unmount(m.UID + "-" + m.ContainerName); err != nil {
		r.logger.Error("Reconciler: failed to unmount", "uid", m.UID, "container", m.ContainerName, "err", err)
	}
	// Clean up volumes and pod workspace directory.
	volResolver := volume.NewResolver(r.baseDir, r.pawnName, m.UID, r.hostNodeName, nil, nil, nil)
	if err := volResolver.Cleanup(); err != nil {
		r.logger.Warn("Reconciler: volume cleanup failed", "uid", m.UID, "err", err)
	}
	podDir := filepath.Join(r.baseDir, "pawns", r.pawnName, "pods", m.UID)
	if err := os.RemoveAll(podDir); err != nil {
		r.logger.Warn("Reconciler: failed to remove pod dir", "uid", m.UID, "err", err)
	}
}
