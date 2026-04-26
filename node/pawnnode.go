// Copyright (C) 2025-2026 Malformed C. All rights reserved.
// SPDX-License-Identifier: BUSL-1.1

package node

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/malformed-c/periapsis/internal/config"
	"github.com/malformed-c/periapsis/internal/image"
	pawnstats "github.com/malformed-c/periapsis/internal/stats"
	"github.com/malformed-c/periapsis/internal/version"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	diskPressureThresholdPercent  = 85.0
	inodePressureThresholdPercent = 95.0
	memoryPressureThreshold       = 95.0
	pidPressureThreshold          = 98.0
	nodeStatusUpdateInterval      = 30 * time.Second
)

// PawnNode owns the NodeProvider implementation and node lifecycle.
type PawnNode struct {
	cfg          config.PawnConfig
	store        *PodStore
	imageManager *image.ImageManager
	logger       *slog.Logger

	shuttingDown atomic.Bool
	shutdownCh   chan struct{}
	startTime    time.Time

	// deletePod is set after construction to break the circular dependency with Gambit.
	deletePod func(ctx context.Context, pod *corev1.Pod) error
}

// NewPawnNode creates a new PawnNode.
func NewPawnNode(cfg config.PawnConfig, store *PodStore, im *image.ImageManager, logger *slog.Logger) *PawnNode {
	return &PawnNode{
		cfg:          cfg,
		store:        store,
		imageManager: im,
		logger:       logger,
		shutdownCh:   make(chan struct{}),
		startTime:    time.Now(),
	}
}

// SetDeletePod sets the delete callback to break circular dependency with Gambit.
func (pn *PawnNode) SetDeletePod(fn func(ctx context.Context, pod *corev1.Pod) error) {
	pn.deletePod = fn
}

// IsShuttingDown returns whether the pawn is in shutdown mode.
func (pn *PawnNode) IsShuttingDown() bool {
	return pn.shuttingDown.Load()
}

// StartTime returns the pawn's start time.
func (pn *PawnNode) StartTime() time.Time {
	return pn.startTime
}

// NodeIP returns the pawn's node IP.
func (pn *PawnNode) NodeIP() string {
	return resolveNodeIP(pn.cfg)
}

// BuildNode constructs a Kubernetes Node object for this pawn.
func (pn *PawnNode) BuildNode() *corev1.Node {
	hostName, _ := os.Hostname()
	pawnName := pn.cfg.Name
	outBoundIP := net.ParseIP(resolveNodeIP(pn.cfg))
	swapCapacity := int64(1)

	var stat syscall.Statfs_t
	workspacePath := pn.imageManager.GetLayerCachePath()
	if err := syscall.Statfs(workspacePath, &stat); err != nil {
		pn.logger.Error("Could not stat workspace filesystem", "path", workspacePath, "err", err)
	}

	totalBytes := stat.Blocks * uint64(stat.Bsize)
	ephemeralStorage := *resource.NewQuantity(int64(totalBytes), resource.BinarySI)

	// All nodes carry the host topology label so the constellation agent
	// can discover all nodes sharing a physical host via label selector.
	// Primary nodes get the primary role; regular pawns get the pawn role.
	labels := make(map[string]string, len(pn.cfg.Labels)+7)
	maps.Copy(labels, pn.cfg.Labels)
	labels["periapsis.io/host"] = hostName
	labels["kubernetes.io/hostname"] = pawnName
	labels["kubernetes.io/os"] = "linux"
	labels["kubernetes.io/arch"] = runtime.GOARCH
	labels["beta.kubernetes.io/os"] = "linux"
	labels["beta.kubernetes.io/arch"] = runtime.GOARCH
	labels["topology.kubernetes.io/zone"] = hostName
	if pn.cfg.IsPrimary {
		labels["periapsis.io/primary"] = "true"
		labels["node-role.kubernetes.io/primary"] = ""
	} else {
		labels["node-role.kubernetes.io/pawn"] = ""
	}

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   pawnName,
			Labels: labels,
		},
		Spec: corev1.NodeSpec{
			Unschedulable: false,
			// Pawns carry their configured taints (typically
			// node.periapsis.io/type=pawn:NoSchedule). DaemonSets schedule
			// on the primary node instead.
			Taints:     pn.cfg.Taints,
			ProviderID: fmt.Sprintf("perigeos://%s/%s", hostName, pawnName),
		},
		Status: corev1.NodeStatus{
			NodeInfo: corev1.NodeSystemInfo{
				ContainerRuntimeVersion: systemdVersion(),
				KubeletVersion:          "perigeos://" + version.Version,
				KernelVersion:           kernelVersion(),
				OSImage:                 osImage(),
				OperatingSystem:         "linux",
				Architecture:            runtime.GOARCH,
				Swap: &corev1.NodeSwapStatus{
					Capacity: &swapCapacity,
				},
			},
			Capacity: corev1.ResourceList{
				corev1.ResourceCPU:              pn.cfg.CPU,
				corev1.ResourceMemory:           pn.cfg.Memory,
				corev1.ResourcePods:             resource.MustParse("256"),
				corev1.ResourceStorage:          ephemeralStorage,
				corev1.ResourceEphemeralStorage: ephemeralStorage,
			},
			Conditions: pn.nodeConditions(),
			Addresses: []corev1.NodeAddress{
				{Type: corev1.NodeHostName, Address: pawnName},
				{Type: corev1.NodeInternalIP, Address: outBoundIP.String()},
			},
			DaemonEndpoints: corev1.NodeDaemonEndpoints{
				KubeletEndpoint: corev1.DaemonEndpoint{
					Port: int32(pn.cfg.Port),
				},
			},
		},
	}
	// Compute Allocatable = Capacity - sum(running pod requests).
	// This lets the k8s scheduler see real available resources and avoid
	// overcommitting this node.
	node.Status.Allocatable = pn.store.ComputeAllocatable(node.Status.Capacity)

	// Report volumes in use so the attach/detach controller doesn't get confused.
	node.Status.VolumesAttached, node.Status.VolumesInUse = pn.collectVolumeStatus()

	return node
}

// collectVolumeStatus scans running pods for PVC-backed volumes and returns
// VolumesAttached and VolumesInUse slices for the node status. Since periapsis
// only supports hostPath and local PVs (no real CSI attach), we report all
// bound PVCs as attached to prevent the attach/detach controller from
// interfering with pod lifecycle.
func (pn *PawnNode) collectVolumeStatus() ([]corev1.AttachedVolume, []corev1.UniqueVolumeName) {
	pods := pn.store.GetPods()
	seen := make(map[corev1.UniqueVolumeName]bool)

	for _, pod := range pods {
		// Skip terminal pods; their volumes are no longer in use.
		if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}

		// Iterate pod volumes looking for PVC references.
		for _, vol := range pod.Spec.Volumes {
			if vol.PersistentVolumeClaim == nil {
				continue
			}

			// Use a synthetic volume name - we don't do real attach/detach.
			// Format: kubernetes.io/no-attacher/namespace-claimname
			name := corev1.UniqueVolumeName(
				"kubernetes.io/no-attacher/" + pod.Namespace + "-" + vol.PersistentVolumeClaim.ClaimName,
			)
			seen[name] = true
		}
	}

	// Build result slices from the deduplicated set.
	attached := make([]corev1.AttachedVolume, 0, len(seen))
	inUse := make([]corev1.UniqueVolumeName, 0, len(seen))
	for name := range seen {
		attached = append(attached, corev1.AttachedVolume{Name: name, DevicePath: ""})
		inUse = append(inUse, name)
	}

	return attached, inUse
}

// Shutdown marks the pawn as shutting down.
func (pn *PawnNode) Shutdown() {
	if pn.shuttingDown.CompareAndSwap(false, true) {
		close(pn.shutdownCh)

		pn.logger.Info("Shutdown initiated", "pawn", pn.cfg.Name)
	}
}

// DrainPods deletes all running pods on this pawn.
func (pn *PawnNode) DrainPods(ctx context.Context) {
	pods := pn.store.GetPods()

	for _, pod := range pods {
		pn.logger.Info("Draining pod", "pawn", pn.cfg.Name, "pod", pod.Name)

		if err := pn.deletePod(ctx, pod); err != nil {
			pn.logger.Error("Failed to drain pod", "pod", pod.Name, "err", err)
		}
	}
}

// Ping returns an error if the pawn is shutting down.
func (pn *PawnNode) Ping(context.Context) error {
	if pn.shuttingDown.Load() {
		return fmt.Errorf("pawn %s is shutting down", pn.cfg.Name)
	}

	return nil
}

// NotifyNodeStatus sends node status updates at regular intervals.
func (pn *PawnNode) NotifyNodeStatus(ctx context.Context, cb func(*corev1.Node)) {
	pn.logger.Info("Starting pawn status notifier", "pawn", pn.cfg.Name)

	go func() {
		pn.logger.Info("Sending initial pawn registration", "pawn", pn.cfg.Name)

		cb(pn.BuildNode())

		ticker := time.NewTicker(nodeStatusUpdateInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				pn.logger.Info("Stopping pawn status notifier")

				return

			case <-pn.shutdownCh:
				pn.logger.Info("Shutdown signal received, marking node NotReady+Unschedulable", "pawn", pn.cfg.Name)

				node := pn.BuildNode()
				node.Spec.Unschedulable = true
				for i := range node.Status.Conditions {
					if node.Status.Conditions[i].Type == corev1.NodeReady {
						node.Status.Conditions[i].Status = corev1.ConditionFalse
						node.Status.Conditions[i].Reason = "Shutdown"
						node.Status.Conditions[i].Message = "perigeos shutting down"
					}
				}

				cb(node)
				return

			case <-ticker.C:
				pn.logger.Info("Updating node status", "pawn", pn.cfg.Name)

				cb(pn.BuildNode())
			}
		}
	}()
}

// nodeConditions returns the set of node conditions for this pawn.
func (pn *PawnNode) nodeConditions() []corev1.NodeCondition {
	now := metav1.Now()

	return []corev1.NodeCondition{
		{
			Type:               corev1.NodeReady,
			Status:             corev1.ConditionTrue,
			LastHeartbeatTime:  now,
			LastTransitionTime: now,
			Reason:             "PawnReady",
			Message:            "Pawn is ready",
		},
		pn.getMemoryPressureCondition(now),
		pn.getDiskPressureCondition(now, pn.imageManager.GetLayerCachePath()),
		pn.getPIDPressureCondition(now),
		{
			Type:               corev1.NodeNetworkUnavailable,
			Status:             corev1.ConditionFalse,
			LastHeartbeatTime:  now,
			LastTransitionTime: now,
			Reason:             "RouteCreated",
			Message:            "RouteController created a route",
		},
	}
}

// getMemoryPressureCondition checks memory usage and returns a NodeMemoryPressure condition.
func (pn *PawnNode) getMemoryPressureCondition(now metav1.Time) corev1.NodeCondition {
	cond := corev1.NodeCondition{
		Type:               corev1.NodeMemoryPressure,
		LastHeartbeatTime:  now,
		LastTransitionTime: now,
	}

	file, err := os.Open("/proc/meminfo")
	if err != nil {
		cond.Status = corev1.ConditionUnknown
		cond.Reason = "ProcMeminfoUnavailable"
		cond.Message = "Could not read /proc/meminfo"

		return cond
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var memTotal, memAvailable uint64
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 2 {
			continue
		}

		val, _ := strconv.ParseUint(fields[1], 10, 64)
		switch fields[0] {
		case "MemTotal:":
			memTotal = val

		case "MemAvailable:":
			memAvailable = val
		}
	}

	if memTotal == 0 {
		cond.Status = corev1.ConditionUnknown
		cond.Reason = "MeminfoParsingFailed"
		cond.Message = "Could not parse MemTotal from /proc/meminfo"

		return cond
	}

	usagePercent := (float64(memTotal-memAvailable) / float64(memTotal)) * 100.0
	if usagePercent > memoryPressureThreshold {
		cond.Status = corev1.ConditionTrue
		cond.Reason = "PawnHasHighMemoryUsage"
		cond.Message = fmt.Sprintf("Memory usage %.2f%% exceeds threshold %.2f%%", usagePercent, memoryPressureThreshold)

	} else {
		cond.Status = corev1.ConditionFalse
		cond.Reason = "PawnHasSufficientMemory"
		cond.Message = "Pawn has sufficient memory available"
	}

	return cond
}

// getDiskPressureCondition checks disk usage and returns a NodeDiskPressure condition.
func (pn *PawnNode) getDiskPressureCondition(now metav1.Time, path string) corev1.NodeCondition {
	cond := corev1.NodeCondition{
		Type:               corev1.NodeDiskPressure,
		LastHeartbeatTime:  now,
		LastTransitionTime: now,
	}

	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		cond.Status = corev1.ConditionUnknown
		cond.Reason = "StatfsFailed"
		cond.Message = fmt.Sprintf("Could not stat filesystem at %s: %v", path, err)

		return cond
	}

	if stat.Files > 0 {
		inodeUsage := (float64(stat.Files-stat.Ffree) / float64(stat.Files)) * 100.0
		if inodeUsage > inodePressureThresholdPercent {
			cond.Status = corev1.ConditionTrue
			cond.Reason = "PawnHasHighInodeUsage"
			cond.Message = fmt.Sprintf("Inode usage %.2f%% exceeds threshold", inodeUsage)

			return cond
		}

		blockUsage := (float64(stat.Blocks-stat.Bfree) / float64(stat.Blocks)) * 100.0
		if blockUsage > diskPressureThresholdPercent {
			cond.Status = corev1.ConditionTrue
			cond.Reason = "PawnHasHighDiskUsage"
			cond.Message = fmt.Sprintf("Disk usage %.2f%% exceeds threshold", blockUsage)

			return cond
		}
	}

	cond.Status = corev1.ConditionFalse
	cond.Reason = "PawnHasNoDiskPressure"
	cond.Message = "Pawn has no disk pressure"

	return cond
}

// getPIDPressureCondition checks PID usage and returns a NodePIDPressure condition.
func (pn *PawnNode) getPIDPressureCondition(now metav1.Time) corev1.NodeCondition {
	cond := corev1.NodeCondition{
		Type:               corev1.NodePIDPressure,
		LastHeartbeatTime:  now,
		LastTransitionTime: now,
	}

	pidMaxBytes, err := os.ReadFile("/proc/sys/kernel/pid_max")
	if err != nil {
		cond.Status = corev1.ConditionUnknown
		cond.Reason = "PidMaxUnavailable"
		cond.Message = "Could not read /proc/sys/kernel/pid_max"

		return cond
	}
	pidMax, _ := strconv.ParseFloat(strings.TrimSpace(string(pidMaxBytes)), 64)

	procFiles, err := os.ReadDir("/proc")
	if err != nil {
		cond.Status = corev1.ConditionUnknown
		cond.Reason = "ProcUnavailable"
		cond.Message = "Could not read /proc"

		return cond
	}

	pidCount := 0
	for _, f := range procFiles {
		if _, err := strconv.Atoi(f.Name()); err == nil {
			pidCount++
		}
	}

	pidUsage := (float64(pidCount) / pidMax) * 100.0
	if pidUsage > pidPressureThreshold {
		cond.Status = corev1.ConditionTrue
		cond.Reason = "PawnHasHighPIDUsage"
		cond.Message = fmt.Sprintf("PID usage %.2f%% exceeds threshold", pidUsage)

	} else {
		cond.Status = corev1.ConditionFalse
		cond.Reason = "PawnHasSufficientPID"
		cond.Message = "Pawn has sufficient PIDs available"
	}

	return cond
}

// GetStatsSummary gathers statistics for the pawn and its pods.
func (pn *PawnNode) GetStatsSummary(_ context.Context) (*pawnstats.Summary, error) {
	now := metav1.Now()
	pawnName := pn.cfg.Name

	// Node-level stats from the pawn's cgroup slice.
	nodeStats := pawnstats.NodeStats{
		NodeName:  pawnName,
		StartTime: now,
	}
	if cpuNs, err := pawnstats.ReadSliceCPU(pawnName); err == nil {
		nodeStats.CPU = &pawnstats.CPUStats{
			Time:                 now,
			UsageCoreNanoSeconds: &cpuNs,
		}
	}
	if usage, ws, err := pawnstats.ReadSliceMemory(pawnName); err == nil {
		nodeStats.Memory = &pawnstats.MemoryStats{
			Time:            now,
			UsageBytes:      &usage,
			WorkingSetBytes: &ws,
		}
	}

	pods := pn.store.GetPods()

	podStats := make([]pawnstats.PodStats, 0, len(pods))
	for _, pod := range pods {
		ps := pawnstats.PodStats{
			PodRef: pawnstats.PodReference{
				Name:      pod.Name,
				Namespace: pod.Namespace,
				UID:       string(pod.UID),
			},
			StartTime: pod.CreationTimestamp,
		}

		var podCPUNs, podMemUsage, podMemWS uint64
		for _, c := range pod.Spec.Containers {
			cs := pawnstats.ContainerStats{
				Name:      c.Name,
				StartTime: now,
			}
			if cpuNs, err := pawnstats.ReadContainerCPU(pawnName, string(pod.UID), c.Name); err == nil {
				cs.CPU = &pawnstats.CPUStats{
					Time:                 now,
					UsageCoreNanoSeconds: &cpuNs,
				}
				podCPUNs += cpuNs
			}
			if usage, ws, err := pawnstats.ReadContainerMemory(pawnName, string(pod.UID), c.Name); err == nil {
				cs.Memory = &pawnstats.MemoryStats{
					Time:            now,
					UsageBytes:      &usage,
					WorkingSetBytes: &ws,
				}
				podMemUsage += usage
				podMemWS += ws
			}
			ps.Containers = append(ps.Containers, cs)
		}

		if podCPUNs > 0 {
			ps.CPU = &pawnstats.CPUStats{Time: now, UsageCoreNanoSeconds: &podCPUNs}
		}
		if podMemUsage > 0 {
			ps.Memory = &pawnstats.MemoryStats{Time: now, UsageBytes: &podMemUsage, WorkingSetBytes: &podMemWS}
		}
		podStats = append(podStats, ps)
	}

	return &pawnstats.Summary{
		Node: nodeStats,
		Pods: podStats,
	}, nil
}

// Package-level helper functions

// systemdVersion returns the systemd version string.
func systemdVersion() string {
	out, err := exec.Command("systemctl", "--version").Output()
	if err != nil {
		return "systemd://"
	}

	// First line: "systemd 259 (259.3-1-arch)" - extract the parenthesized version.
	line := strings.SplitN(string(out), "\n", 2)[0]
	if start := strings.IndexByte(line, '('); start >= 0 {
		if end := strings.IndexByte(line[start:], ')'); end >= 0 {
			return "systemd://" + line[start+1:start+end]
		}
	}

	// Fallback: use the bare version number.
	fields := strings.Fields(line)
	if len(fields) >= 2 {
		return "systemd://" + fields[1]
	}

	return "systemd://"
}

// kernelVersion returns the kernel version string.
func kernelVersion() string {
	var buf syscall.Utsname
	if err := syscall.Uname(&buf); err != nil {
		return ""
	}

	b := make([]byte, 0, len(buf.Release))
	for _, c := range buf.Release {
		if c == 0 {
			break
		}

		b = append(b, byte(c))
	}

	return string(b)
}

// osImage returns the OS image string from /etc/os-release.
func osImage() string {
	f, err := os.Open("/etc/os-release")
	if err != nil {
		return ""
	}
	defer f.Close()

	s := bufio.NewScanner(f)
	for s.Scan() {
		if after, ok := strings.CutPrefix(s.Text(), "PRETTY_NAME="); ok {
			return strings.Trim(after, "\"")
		}
	}

	return ""
}
