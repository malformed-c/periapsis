package node

import (
	"context"
	"os"
	"path/filepath"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	listersv1 "k8s.io/client-go/listers/core/v1"
)

func (g *Gambit) HydrateFromRuntime(ctx context.Context) error {
	// --- Primary path: restore from disk ---
	states, err := loadAllPodStates(g.Config.BaseDir, g.Config.Name)
	if err != nil {
		g.Logger.Warn("Failed to load pod states from disk", "err", err)
	}

	// Filter out terminal pods and prepare batch entries.
	var entries []hydratedEntry
	diskUIDs := make(map[string]struct{}, len(states))
	for _, state := range states {
		uid := string(state.Pod.UID)
		// Skip terminal pods - they completed before the restart and should
		// not be resurrected. The PodController will see them as gone.
		if state.Phase == corev1.PodSucceeded || state.Phase == corev1.PodFailed {
			g.Logger.Info("Skipping terminal pod from disk",
				"pod", state.Pod.Name, "phase", state.Phase)
			continue
		}
		entries = append(entries, hydratedEntry{
			uid: uid,
			pod: state.Pod,
			ip:  state.PodIP,
		})
		diskUIDs[uid] = struct{}{}
	}

	// Bulk register all disk-restored pods in a single lock.
	g.store.RegisterHydratedBatch(entries)

	// Initialize probe states for disk-restored pods (must happen outside the lock).
	for _, state := range states {
		if state.Phase == corev1.PodSucceeded || state.Phase == corev1.PodFailed {
			continue
		}
		g.store.InitRestartState(state.Pod)
		// InitRestartState resets restarts - re-apply the persisted counts
		// and backoff durations.
		uid := string(state.Pod.UID)
		if len(state.Restarts) > 0 {
			for cname, count := range state.Restarts {
				g.store.PatchRestartCount(uid, cname, count)
			}
		}
		if len(state.Backoffs) > 0 {
			for cname, backoffSec := range state.Backoffs {
				g.store.PatchBackoff(uid, cname, backoffSec)
			}
		}
	}

	// --- Fallback: pick up any running units not on disk (pre-state-persistence pods) ---
	machines, err := g.Runtime.ListManagedMachines(ctx)
	if err != nil {
		g.Logger.Warn("HydrateFromRuntime: ListManagedMachines failed (non-fatal)", "err", err)
		machines = nil
	}

	for _, m := range machines {
		if m.UID == "" {
			continue
		}
		if _, onDisk := diskUIDs[m.UID]; onDisk {
			continue // already restored from disk
		}
		// Construct a minimal stub pod for fallback registration.
		if m.Name != "" && m.Namespace != "" {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      m.Name,
					Namespace: m.Namespace,
					UID:       types.UID(m.UID),
				},
			}
			g.store.RegisterHydrated(m.UID, pod, m.PodIP)
			g.store.InitRestartState(pod)
		}
	}

	g.Logger.Info("Hydrated in-memory state", "from_disk", len(entries), "from_systemd", len(machines))

	// Sweep disk for orphan overlay dirs that have no corresponding systemd
	// unit. This handles the case where a pod was mid-deletion (systemd unit
	// stopped) when perigeos restarted, leaving the overlay dir behind.
	podsDir := filepath.Join(g.Config.BaseDir, "pawns", g.Config.Name, "pods")
	entries2, err := os.ReadDir(podsDir)
	if err == nil {
		hydratedUIDs := g.store.HydratedUIDs()
		for _, e := range entries2 {
			if !e.IsDir() {
				continue
			}
			name := e.Name()
			uid := name
			if len(name) > 36 && name[36] == '-' {
				uid = name[:36]
			}
			if _, ok := hydratedUIDs[uid]; !ok {
				dirPath := filepath.Join(podsDir, name)
				// Unmount overlayfs if it's an overlay dir (uid-container).
				var cleanErr error
				if len(name) > 36 && name[36] == '-' {
					cleanErr = g.ImageManager.Unmount(name)
				} else {
					cleanErr = os.RemoveAll(dirPath)
				}
				if cleanErr != nil {
					g.Logger.Warn("Failed to clean orphan disk dir at startup", "dir", name, "err", cleanErr)
				} else {
					g.Logger.Info("Cleaned orphan disk dir at startup", "dir", name)
				}
			}
		}
	}

	return nil
}

// PurgeStaleHydrated removes pods that were rehydrated from systemd but never
// confirmed by the PodController (i.e. they don't exist in Kubernetes anymore).
// Call this after the informer caches sync and the PodController has had a
// chance to call CreatePod for all real pods.
func (g *Gambit) PurgeStaleHydrated(podLister listersv1.PodNamespaceLister) {
	hydratedUIDs := g.store.HydratedUIDs()
	g.Logger.Info("PurgeStaleHydrated: checking hydrated pods",
		"pawn", g.Config.Name, "hydrated", len(hydratedUIDs), "total_pods", g.store.PodCount())

	stale := make([]string, 0)
	for uid := range hydratedUIDs {
		// If CreatePod was called for this UID, it's confirmed - skip.
		// (CreatePod replaces the hydration stub with the full pod object,
		// which has Spec.Containers populated.)
		pod := g.store.GetPodCopy(uid)
		if pod != nil && len(pod.Spec.Containers) > 0 {
			continue
		}
		// Check the informer cache - if k8s doesn't know about it, it's stale.
		if podLister != nil {
			pods, err := podLister.List(labels.Everything())
			if err == nil {
				found := false
				for _, p := range pods {
					if string(p.UID) == uid {
						found = true
						break
					}
				}
				if found {
					continue
				}
			}
		}
		stale = append(stale, uid)
	}

	// Purge stale UIDs from the store.
	if len(stale) > 0 {
		g.store.PurgeHydrated(stale)
	}

	for _, uid := range stale {
		g.Logger.Warn("Purging stale hydrated pod", "uid", uid)
		// Clean up pod workspace directory and volumes.
		podDir := filepath.Join(g.Config.BaseDir, "pawns", g.Config.Name, "pods", uid)
		if err := os.RemoveAll(podDir); err != nil {
			g.Logger.Warn("PurgeStaleHydrated: failed to remove pod dir", "uid", uid, "err", err)
		}
		// Clean up any per-container overlay dirs (<uid>-<container>/).
		podsDir := filepath.Join(g.Config.BaseDir, "pawns", g.Config.Name, "pods")
		if entries, err := os.ReadDir(podsDir); err == nil {
			for _, e := range entries {
				if e.IsDir() && strings.HasPrefix(e.Name(), uid+"-") {
					_ = g.ImageManager.Unmount(e.Name())
				}
			}
		}
	}

	if len(stale) > 0 {
		g.Logger.Info("Purged stale hydrated pods", "count", len(stale))
	}
}

// --- Node --------------------------------------------------------------------
