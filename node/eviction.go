// Copyright (C) 2025-2026 Malformed C. All rights reserved.
// SPDX-License-Identifier: BUSL-1.1

package node

import (
	"context"
	"fmt"
	"sort"
	"time"

	pawnstats "github.com/malformed-c/periapsis/internal/stats"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

const (
	// evictionCheckInterval is how often the eviction loop checks pressure.
	evictionCheckInterval = 10 * time.Second

	// evictionMemoryThreshold is the fraction of node memory at which eviction
	// starts. 0.90 = evict when working set exceeds 90% of node capacity.
	evictionMemoryThreshold = 0.90

	// evictionHardThreshold triggers aggressive eviction (evict multiple pods).
	evictionHardThreshold = 0.95

	// evictionMinFreeBytes is the minimum amount of memory to free per eviction
	// round to avoid thrashing (evict enough to get back under soft threshold).
	evictionMinFreeBytes = 128 * 1024 * 1024 // 128 MiB
)

// podEvictionCandidate holds a pod and its eviction priority metadata.
type podEvictionCandidate struct {
	pod          *corev1.Pod
	uid          string
	qosClass     corev1.PodQOSClass
	oomScore     int
	memoryUsage  uint64 // working set bytes from cgroup
	memoryRequest int64 // bytes from pod spec
}

// RunEvictionLoop starts the eviction loop. It periodically checks node memory
// pressure and evicts pods when usage exceeds the configured threshold.
// The loop runs until ctx is cancelled.
func (g *Gambit) RunEvictionLoop(ctx context.Context) {
	ticker := time.NewTicker(evictionCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			g.checkAndEvict(ctx)
		}
	}
}

func (g *Gambit) checkAndEvict(ctx context.Context) {
	nodeMemBytes := g.Config.Memory.Value()
	if nodeMemBytes <= 0 {
		return
	}

	// Read current working set for the whole pawn slice.
	_, workingSet, err := pawnstats.ReadSliceMemory(g.Config.Name)
	if err != nil {
		g.Logger.Debug("eviction: failed to read slice memory", "pawn", g.Config.Name, "err", err)
		return
	}

	usageFraction := float64(workingSet) / float64(nodeMemBytes)
	if usageFraction < evictionMemoryThreshold {
		return
	}

	g.Logger.Warn("eviction: memory pressure detected",
		"pawn", g.Config.Name,
		"usage", resource.NewQuantity(int64(workingSet), resource.BinarySI).String(),
		"capacity", g.Config.Memory.String(),
		"fraction", fmt.Sprintf("%.1f%%", usageFraction*100),
	)

	candidates := g.buildEvictionCandidates()
	if len(candidates) == 0 {
		return
	}

	// Sort: BestEffort first, then Burstable by descending memory usage,
	// then Guaranteed last. Within the same QoS class, highest OOM score
	// (most expendable) evicted first.
	sort.Slice(candidates, func(i, j int) bool {
		ci, cj := candidates[i], candidates[j]
		if ci.qosClass != cj.qosClass {
			return qosOrdinal(ci.qosClass) < qosOrdinal(cj.qosClass)
		}
		if ci.oomScore != cj.oomScore {
			return ci.oomScore > cj.oomScore // higher = evict first
		}
		return ci.memoryUsage > cj.memoryUsage
	})

	// Determine how many pods to evict. Under hard threshold, evict one at a
	// time. At or above hard threshold, evict until we expect to be under soft.
	targetFreeBytes := uint64(float64(nodeMemBytes)*evictionMemoryThreshold) - workingSet
	if usageFraction >= evictionHardThreshold {
		// Aggressive: keep evicting until we expect enough headroom.
		freed := uint64(0)
		for _, c := range candidates {
			if freed > 0 && freed >= uint64(evictionMinFreeBytes) {
				break
			}
			g.evictPod(ctx, c)
			freed += c.memoryUsage
		}
		_ = targetFreeBytes
	} else {
		// Soft threshold: evict just the top candidate.
		g.evictPod(ctx, candidates[0])
	}
}

func (g *Gambit) buildEvictionCandidates() []podEvictionCandidate {
	entries := g.store.Snapshot()
	candidates := make([]podEvictionCandidate, 0, len(entries))

	for _, e := range entries {
		if e.Phase != corev1.PodRunning {
			continue
		}
		if g.store.IsDeleting(e.UID) {
			continue
		}

		pod := e.Pod
		oomScore, qosClass := calculateOOMScore(pod, g.Config.Memory.Value())

		// Read per-pod memory from cgroup. Use the pawn slice path.
		// Sum across all containers.
		var podMemUsage uint64
		for _, c := range pod.Spec.Containers {
			usage, ws, err := pawnstats.ReadContainerMemory(g.Config.Name, e.UID, c.Name)
			if err == nil {
				_ = usage
				podMemUsage += ws
			}
		}

		var memRequest int64
		for _, c := range pod.Spec.Containers {
			if req, ok := c.Resources.Requests[corev1.ResourceMemory]; ok {
				memRequest += req.Value()
			}
		}

		candidates = append(candidates, podEvictionCandidate{
			pod:           pod,
			uid:           e.UID,
			qosClass:      qosClass,
			oomScore:      oomScore,
			memoryUsage:   podMemUsage,
			memoryRequest: memRequest,
		})
	}

	return candidates
}

func (g *Gambit) evictPod(ctx context.Context, c podEvictionCandidate) {
	g.Logger.Warn("eviction: evicting pod",
		"pawn", g.Config.Name,
		"pod", c.pod.Name,
		"namespace", c.pod.Namespace,
		"uid", c.uid,
		"qos", c.qosClass,
		"memoryUsage", resource.NewQuantity(int64(c.memoryUsage), resource.BinarySI).String(),
	)

	g.EventRecorder.Eventf(c.pod, corev1.EventTypeWarning, "Evicted",
		"The pod was evicted due to node memory pressure (usage %.1f%% of capacity)",
		float64(c.memoryUsage)/float64(g.Config.Memory.Value())*100,
	)

	// Mark the pod as evicted in the status before deletion so kubectl shows
	// the reason. This mirrors kubelet's eviction flow.
	evictedPod := c.pod.DeepCopy()
	evictedPod.Status.Phase = corev1.PodFailed
	evictedPod.Status.Reason = "Evicted"
	evictedPod.Status.Message = "The node was under memory pressure."
	g.notifyPodStatus(evictedPod)

	evictCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	if err := g.DeletePod(evictCtx, c.pod); err != nil {
		g.Logger.Error("eviction: DeletePod failed",
			"pawn", g.Config.Name,
			"pod", c.pod.Name,
			"uid", c.uid,
			"err", err,
		)
	}
}

// qosOrdinal maps QoS class to sort order (lower = evicted first).
func qosOrdinal(q corev1.PodQOSClass) int {
	switch q {
	case corev1.PodQOSBestEffort:
		return 0
	case corev1.PodQOSBurstable:
		return 1
	case corev1.PodQOSGuaranteed:
		return 2
	default:
		return 1
	}
}
