// Copyright (C) 2025-2026 Malformed C. All rights reserved.
// SPDX-License-Identifier: BUSL-1.1

package node

import (
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/malformed-c/periapsis/errdefs"
	"github.com/malformed-c/periapsis/internal/psi"
	perigeos "github.com/malformed-c/periapsis/internal/runtime"
	"github.com/malformed-c/periapsis/internal/types"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// completedEntry records a recently-deleted pod for log retrieval.
// Entries expire after completedTTL to prevent unbounded growth in high-churn clusters.
const completedTTL = 24 * time.Hour

type completedEntry struct {
	uid       string
	deletedAt time.Time
}

// containerMachineState is the last-known OS-level state for a single
// container. Written by BatchWatcher on every poll/event cycle; read by
// buildPodStatus to derive corev1.ContainerState without a D-Bus call.
type containerMachineState struct {
	State    perigeos.MachineState
	ExitCode int32
}

// podState acts as the localized "Actor" state for a single pod.
// By giving each pod its own mutex, we eliminate global lock contention
// for highly concurrent events like container probes or restarts across 3000+ pods.
type podState struct {
	mu sync.RWMutex

	pod      *corev1.Pod
	ip       string
	phase    corev1.PodPhase
	hydrated bool
	inFlight *creationHandle
	deleting bool
	restarts map[string]*containerRestartState
	probes   map[string]*ContainerProbeState

	// seenRunning tracks containers that have been observed in Running state at
	// least once. Owned here so that BatchWatcher doesn't need a parallel map
	// with its own GC loop - cleanup is automatic when the pod is unregistered.
	seenRunning map[string]bool

	// machineStates holds the last-known MachineState and exit code per
	// container, written by BatchWatcher on every D-Bus event and poll cycle.
	// buildPodStatus reads from here instead of carrying a stateLookup callback.
	machineStates map[string]containerMachineState // key: containerName
}

// PodStore is the single source of truth for node-level pod state.
// It implements PodTracker so it can be passed directly to the Reconciler.
type PodStore struct {
	// registryMu protects the maps that hold the global pod topologies.
	// It is ONLY locked when pods are added or removed, never during localized mutations.
	registryMu  sync.RWMutex
	pods        map[string]*podState
	nameIndex   map[string]string         // "namespace/name" -> UID
	completed   map[string]completedEntry // "namespace/name" -> entry (log fallback)
	completedMu sync.Mutex                // separate lock - never held with registryMu

	// atomic global counters for instant 0-lock queries
	usedCPU       atomic.Int64 // in millicores
	usedMem       atomic.Int64 // in bytes
	deletingCount atomic.Int32 // number of pods currently in the delete path

	// roSnap is an asynchronously updated lock-free read replica.
	roSnap    atomic.Pointer[[]PodSnapshot]
	triggerCh chan struct{} // triggers the background aggregator
	stopCh    chan struct{} // closed by Close() to stop the aggregator

	probeRunner *ProbeRunner
	createSem   chan struct{}
	logger      *slog.Logger
}

// Verify PodStore satisfies PodTracker.
var _ PodTracker = (*PodStore)(nil)

// NewPodStore creates a PodStore, starting the background snapshot aggregator.
func NewPodStore(rt perigeos.Runtime, createConcurrency int, logger *slog.Logger) *PodStore {
	store := &PodStore{
		pods:        make(map[string]*podState),
		nameIndex:   make(map[string]string),
		completed:   make(map[string]completedEntry),
		triggerCh:   make(chan struct{}, 1),
		stopCh:      make(chan struct{}),
		probeRunner: NewProbeRunner(rt, logger),
		createSem:   make(chan struct{}, createConcurrency),
		logger:      logger,
	}

	// Start the background snapshot aggregator
	go store.runSnapshotAggregator()

	return store
}

// --- Internal Helpers ---

func podResources(pod *corev1.Pod) (cpuMillis, memBytes int64) {
	if pod == nil {
		return
	}

	for _, c := range pod.Spec.Containers {
		if req, ok := c.Resources.Requests[corev1.ResourceCPU]; ok {
			cpuMillis += req.MilliValue()
		}
		if req, ok := c.Resources.Requests[corev1.ResourceMemory]; ok {
			memBytes += req.Value()
		}
	}

	return
}

// triggerSnapshot signals the background aggregator to rebuild the lock-free read view.
func (s *PodStore) triggerSnapshot() {
	select {
	case s.triggerCh <- struct{}{}:
	default:
	}
}

// runSnapshotAggregator is the central event loop that rebuilds the global snapshot.
// It completely decouples O(N) read-replica building from hot O(1) write paths.
// Exits when stopCh is closed (via Close()).
func (s *PodStore) runSnapshotAggregator() {
	for {
		select {
		case <-s.stopCh:
			return

		case _, ok := <-s.triggerCh:
			if !ok {
				return
			}
		}

		s.registryMu.RLock()
		snaps := make([]PodSnapshot, 0, len(s.pods))
		for uid, ps := range s.pods {
			ps.mu.RLock()
			if !ps.hydrated {
				snaps = append(snaps, PodSnapshot{
					Name:       ps.pod.Name,
					Namespace:  ps.pod.Namespace,
					UID:        uid,
					IP:         ps.ip,
					Phase:      ps.phase,
					Containers: len(ps.pod.Spec.Containers),
				})
			}
			ps.mu.RUnlock()
		}
		s.registryMu.RUnlock()

		s.roSnap.Store(&snaps)
	}
}

func (s *PodStore) getPodState(uid string) *podState {
	s.registryMu.RLock()
	defer s.registryMu.RUnlock()

	return s.pods[uid]
}

// --- PodTracker interface ---

func (s *PodStore) IsInFlight(uid string) bool {
	if ps := s.getPodState(uid); ps != nil {
		ps.mu.RLock()
		defer ps.mu.RUnlock()

		return ps.inFlight != nil
	}

	return false
}

func (s *PodStore) HasPod(uid string) bool {
	return s.getPodState(uid) != nil
}

func (s *PodStore) PodUIDs() map[string]string {
	s.registryMu.RLock()
	defer s.registryMu.RUnlock()

	uids := make(map[string]string, len(s.pods))
	for uid, ps := range s.pods {
		ps.mu.RLock()
		uids[uid] = ps.pod.Namespace + "/" + ps.pod.Name
		ps.mu.RUnlock()
	}

	return uids
}

func (s *PodStore) EvictGhost(uid string) {
	s.registryMu.Lock()
	ps, ok := s.pods[uid]
	if ok {
		delete(s.pods, uid)
		if ps.pod != nil {
			delete(s.nameIndex, ps.pod.Namespace+"/"+ps.pod.Name)
		}
	}
	s.registryMu.Unlock()

	if ok {
		ps.mu.RLock()
		cpu, mem := podResources(ps.pod)
		ps.mu.RUnlock()

		s.usedCPU.Add(-cpu)
		s.usedMem.Add(-mem)
		s.triggerSnapshot()
	}
}

// --- Accessors ---

func (s *PodStore) PodCount() int {
	s.registryMu.RLock()
	defer s.registryMu.RUnlock()

	return len(s.pods)
}

func (s *PodStore) PodIP(uid string) string {
	if ps := s.getPodState(uid); ps != nil {
		ps.mu.RLock()
		defer ps.mu.RUnlock()

		return ps.ip
	}

	return ""
}

func (s *PodStore) PodPhase(uid string) corev1.PodPhase {
	if ps := s.getPodState(uid); ps != nil {
		ps.mu.RLock()
		defer ps.mu.RUnlock()

		return ps.phase
	}

	return ""
}

func (s *PodStore) DeletionsInProgress() bool {
	return s.deletingCount.Load() > 0
}

func (s *PodStore) IsDeleting(uid string) bool {
	if ps := s.getPodState(uid); ps != nil {
		ps.mu.RLock()
		defer ps.mu.RUnlock()

		return ps.deleting
	}

	return false
}

func (s *PodStore) IsContainerReady(uid, containerName string) bool {
	if ps := s.getPodState(uid); ps != nil {
		ps.mu.RLock()
		defer ps.mu.RUnlock()
		if probe, ok := ps.probes[containerName]; ok {
			return probe.Ready
		}
	}

	return true
}

// MarkContainerSeenRunning records that a container has been observed in Running
// state at least once. Safe to call before InitRestartState (the map is lazily
// initialised). Idempotent.
func (s *PodStore) MarkContainerSeenRunning(uid, containerName string) {
	ps := s.getPodState(uid)
	if ps == nil {
		s.logger.Error("MarkContainerSeenRunning: pod not registered", "uid", uid, "container", containerName)

		return
	}

	ps.mu.Lock()
	if ps.seenRunning == nil {
		ps.seenRunning = make(map[string]bool)
	}
	ps.seenRunning[containerName] = true
	ps.mu.Unlock()
}

// IsContainerSeenRunning reports whether a container has ever been observed in
// Running state. Returns false for unknown pods or containers.
func (s *PodStore) IsContainerSeenRunning(uid, containerName string) bool {
	if ps := s.getPodState(uid); ps != nil {
		ps.mu.RLock()
		defer ps.mu.RUnlock()
		return ps.seenRunning[containerName]
	}
	return false
}

// --- Composite State Mutations ---

func (s *PodStore) RegisterPending(uid string, pod *corev1.Pod, handle *creationHandle) {
	s.registryMu.Lock()
	if ps, ok := s.pods[uid]; ok {
		ps.mu.Lock()
		ps.pod = pod
		ps.phase = corev1.PodPending
		ps.inFlight = handle
		ps.mu.Unlock()
		s.registryMu.Unlock()

		return
	}

	ps := &podState{
		pod:      pod,
		phase:    corev1.PodPending,
		inFlight: handle,
	}

	s.pods[uid] = ps
	s.nameIndex[pod.Namespace+"/"+pod.Name] = uid
	s.registryMu.Unlock()

	cpu, mem := podResources(pod)
	s.usedCPU.Add(cpu)
	s.usedMem.Add(mem)

	s.triggerSnapshot()
}

func (s *PodStore) AlreadyRunning(uid string, pod *corev1.Pod) (exists bool, wasStub bool) {
	ps := s.getPodState(uid)
	if ps == nil {
		return false, false
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()

	wasStub = len(ps.pod.Spec.Containers) == 0
	if wasStub {
		// Replace the empty stub with real pod and register its resources.
		// DeepCopy for the same reason as PromoteRunning: prevent the caller
		// from aliasing the store's pod pointer and mutating it concurrently.
		cpu, mem := podResources(pod)
		s.usedCPU.Add(cpu)
		s.usedMem.Add(mem)
		ps.pod = pod.DeepCopy()
	}
	ps.hydrated = false
	s.triggerSnapshot()

	return true, wasStub
}

func (s *PodStore) AlreadyInFlight(uid string) bool {
	return s.IsInFlight(uid)
}

func (s *PodStore) PromoteRunning(uid string, pod *corev1.Pod, ip string) {
	if ps := s.getPodState(uid); ps != nil {
		ps.mu.Lock()
		// DeepCopy breaks the aliasing between the store's pod and the
		// lifecycle goroutine's local pod variable. Without this, lifecycle.go
		// continues mutating pod.Status (env resolution, PodIP, etc.) on the
		// same pointer that Snapshot returns to BatchWatcher - a data race on
		// pod.Status fields read by buildPodStatus concurrently.
		ps.pod = pod.DeepCopy()
		ps.ip = ip
		ps.phase = corev1.PodRunning
		ps.inFlight = nil
		ps.mu.Unlock()
		slog.Debug("PromoteRunning",
			"pod", pod.Name, "uid", uid, "ip", ip,
			"containers", len(pod.Spec.Containers)+len(pod.Spec.InitContainers),
			"caller", callerSite())
		s.triggerSnapshot()
	}
}

func (s *PodStore) MarkDeleting(uid string) {
	if ps := s.getPodState(uid); ps != nil {
		ps.mu.Lock()
		if !ps.deleting {
			ps.deleting = true
			s.deletingCount.Add(1)
		}
		ps.mu.Unlock()
	}
}

func (s *PodStore) SetPodIP(uid, ip string) {
	if ps := s.getPodState(uid); ps != nil {
		ps.mu.Lock()
		ps.ip = ip
		ps.mu.Unlock()
		s.triggerSnapshot()
	}
}

func (s *PodStore) Unregister(uid, namespace, name string) {
	s.registryMu.Lock()
	ps, ok := s.pods[uid]
	if ok {
		delete(s.pods, uid)
		delete(s.nameIndex, namespace+"/"+name)
	}

	s.completedMu.Lock()
	s.completed[namespace+"/"+name] = completedEntry{uid: uid, deletedAt: time.Now()}
	s.expireCompleted()
	s.completedMu.Unlock()
	s.registryMu.Unlock()

	if ok {
		ps.mu.RLock()
		cpu, mem := podResources(ps.pod)
		wasDeleting := ps.deleting
		ps.mu.RUnlock()

		s.usedCPU.Add(-cpu)
		s.usedMem.Add(-mem)
		if wasDeleting {
			s.deletingCount.Add(-1)
		}

		s.triggerSnapshot()
	}
}

func (s *PodStore) setPhase(ps *podState, phase corev1.PodPhase) {
	ps.phase = phase

	if ps.pod != nil {
		ps.pod.Status.Phase = phase
	}
}

func (s *PodStore) SetPhase(uid string, phase corev1.PodPhase) {
	if ps := s.getPodState(uid); ps != nil {
		ps.mu.Lock()
		s.setPhase(ps, phase)
		ps.mu.Unlock()

		s.triggerSnapshot()
	}
}

func (s *PodStore) SetPodStatus(uid string, status corev1.PodStatus) {
	if ps := s.getPodState(uid); ps != nil {
		ps.mu.Lock()

		s.setPhase(ps, status.Phase)

		if ps.pod != nil {
			status.DeepCopyInto(&ps.pod.Status)
		}

		ps.mu.Unlock()

		s.triggerSnapshot()
	}
}

// SetContainerMachineState records the OS-level state for a container as
// observed by BatchWatcher (from ListManagedMachines or a D-Bus event).
// buildPodStatus reads from here to build corev1.ContainerState without
// requiring a D-Bus call per container.
func (s *PodStore) SetContainerMachineState(uid, containerName string, state perigeos.MachineState, exitCode int32) {
	ps := s.getPodState(uid)
	if ps == nil {
		s.logger.Error("SetContainerMachineState: pod not registered", "uid", uid, "container", containerName)

		return
	}

	ps.mu.Lock()
	if ps.machineStates == nil {
		ps.machineStates = make(map[string]containerMachineState)
	}
	ps.machineStates[containerName] = containerMachineState{State: state, ExitCode: exitCode}
	ps.mu.Unlock()
}

// ContainerMachineState returns the last-known OS-level state and exit code
// for a container. Returns (StateUnknown, 0) if no record exists.
func (s *PodStore) ContainerMachineState(uid, containerName string) (perigeos.MachineState, int32) {
	if ps := s.getPodState(uid); ps != nil {
		ps.mu.RLock()
		ms, ok := ps.machineStates[containerName]
		ps.mu.RUnlock()
		if ok {
			return ms.State, ms.ExitCode
		}
	}
	return perigeos.StateUnknown, 0
}

func (s *PodStore) MarkFailed(uid string, pod *corev1.Pod, reason, message string) *corev1.Pod {
	failedPod := pod.DeepCopy()
	failedPod.Status.Phase = corev1.PodFailed
	failedPod.Status.Reason = reason
	failedPod.Status.Message = message

	if ps := s.getPodState(uid); ps != nil {
		ps.mu.Lock()
		ps.pod = failedPod
		ps.phase = corev1.PodFailed
		ps.mu.Unlock()
		s.triggerSnapshot()
	}

	return failedPod
}

func (s *PodStore) CancelInFlight(uid string) {
	if ps := s.getPodState(uid); ps != nil {
		ps.mu.RLock()
		handle := ps.inFlight
		ps.mu.RUnlock()

		if handle != nil {
			handle.cancel()
			<-handle.done
		}
	}
}

// --- Restart & Probe State ---

func (s *PodStore) InitRestartState(pod *corev1.Pod) {
	uid := string(pod.UID)
	ps := s.getPodState(uid)
	if ps == nil {
		return
	}

	allContainers := make([]corev1.Container, 0, len(pod.Spec.InitContainers)+len(pod.Spec.Containers))
	allContainers = append(allContainers, pod.Spec.InitContainers...)
	allContainers = append(allContainers, pod.Spec.Containers...)

	ps.mu.Lock()
	defer ps.mu.Unlock()

	if ps.restarts == nil {
		ps.restarts = make(map[string]*containerRestartState, len(allContainers))
	}
	if ps.probes == nil {
		ps.probes = make(map[string]*ContainerProbeState, len(allContainers))
	}

	for _, c := range allContainers {
		if _, ok := ps.restarts[c.Name]; !ok {
			ps.restarts[c.Name] = &containerRestartState{
				backoff:     restartBackoffInit,
				lastStarted: time.Now(),
			}
		}
		if _, ok := ps.probes[c.Name]; !ok {
			ps.probes[c.Name] = &ContainerProbeState{
				StartedAt:     time.Now(),
				Ready:         c.ReadinessProbe == nil,
				LastProbeTime: make(map[string]time.Time),
			}
		}
	}

	slog.Info("InitRestartState",
		"pod", pod.Name, "uid", uid,
		"containers", len(allContainers),
		"caller", callerSite())
}

// InitRestartStateFrom is the flat-payload variant of InitRestartState.
// It accepts the same data as ContainerInitPayload from the types package,
// so Syzygy can call it without holding a *corev1.Pod pointer.
// This is the target call site for the Syzygy-wired path.
func (s *PodStore) InitRestartStateFrom(uid, namespace, name string, containers []types.ContainerInitPayload) {
	ps := s.getPodState(uid)
	if ps == nil {
		return
	}

	rs := make(map[string]*containerRestartState, len(containers))
	probes := make(map[string]*ContainerProbeState, len(containers))

	for _, c := range containers {
		rs[c.Name] = &containerRestartState{
			backoff:     restartBackoffInit,
			lastStarted: time.Now(),
		}
		probes[c.Name] = &ContainerProbeState{
			StartedAt:     time.Now(),
			Ready:         !c.HasReadinessProbe,
			LastProbeTime: make(map[string]time.Time),
		}
	}

	ps.mu.Lock()
	ps.restarts = rs
	ps.probes = probes
	ps.mu.Unlock()

	slog.Info("InitRestartStateFrom",
		"pod", name, "uid", uid,
		"containers", len(containers),
		"caller", callerSite())

	for _, c := range containers {
		slog.Debug("InitRestartStateFrom: container",
			"pod", name, "container", c.Name,
			"hasReadinessProbe", c.HasReadinessProbe,
			"initialReady", !c.HasReadinessProbe)
	}
}

func (s *PodStore) RestartCounts(uid string) map[string]int32 {
	if ps := s.getPodState(uid); ps != nil {
		ps.mu.RLock()
		defer ps.mu.RUnlock()

		if len(ps.restarts) == 0 {
			return nil
		}

		counts := make(map[string]int32, len(ps.restarts))
		for c, r := range ps.restarts {
			counts[c] = r.count
		}

		return counts
	}

	return nil
}

// RestartBackoffs returns the per-container CrashLoopBackOff durations in seconds.
// Used to persist backoff state to disk so it survives perigeos restarts.
func (s *PodStore) RestartBackoffs(uid string) map[string]float64 {
	if ps := s.getPodState(uid); ps != nil {
		ps.mu.RLock()
		defer ps.mu.RUnlock()

		if len(ps.restarts) == 0 {
			return nil
		}

		backoffs := make(map[string]float64, len(ps.restarts))
		for c, r := range ps.restarts {
			backoffs[c] = r.backoff.Seconds()
		}

		return backoffs
	}

	return nil
}

func (s *PodStore) RestartState(uid, containerName string) *containerRestartState {
	if ps := s.getPodState(uid); ps != nil {
		ps.mu.RLock()
		defer ps.mu.RUnlock()

		return ps.restarts[containerName]
	}

	return nil
}

// ContainerStartedAt returns the time the container last entered Running.
// Returns a zero metav1.Time if unknown.
func (s *PodStore) ContainerStartedAt(uid, containerName string) metav1.Time {
	if ps := s.getPodState(uid); ps != nil {
		ps.mu.RLock()
		defer ps.mu.RUnlock()
		if rs := ps.restarts[containerName]; rs != nil && !rs.lastStarted.IsZero() {
			return metav1.NewTime(rs.lastStarted)
		}
	}

	return metav1.Time{}
}

func (s *PodStore) ProbeState(uid, containerName string) *ContainerProbeState {
	if ps := s.getPodState(uid); ps != nil {
		ps.mu.RLock()
		defer ps.mu.RUnlock()

		return ps.probes[containerName]
	}

	return nil
}

func (s *PodStore) UpdateProbeState(uid, containerName string, fn func(*ContainerProbeState)) {
	if ps := s.getPodState(uid); ps != nil {
		ps.mu.Lock()
		if state, ok := ps.probes[containerName]; ok {
			fn(state)
		}
		ps.mu.Unlock()
	}
}

func (s *PodStore) ResetProbeState(uid, containerName string) {
	if ps := s.getPodState(uid); ps != nil {
		ps.mu.Lock()
		if ps.probes != nil {
			ps.probes[containerName] = &ContainerProbeState{
				StartedAt:     time.Now(),
				LastProbeTime: make(map[string]time.Time),
			}
		}
		ps.mu.Unlock()
	}
}

func (s *PodStore) IncrementRestart(uid, containerName string) {
	if ps := s.getPodState(uid); ps != nil {
		ps.mu.Lock()
		if rs, ok := ps.restarts[containerName]; ok {
			rs.count++
			rs.lastStarted = time.Now()
		}
		ps.mu.Unlock()
	}
}

// --- Hydration ---

func (s *PodStore) RegisterHydrated(uid string, pod *corev1.Pod, ip string) {
	ps := &podState{
		pod:      pod,
		ip:       ip,
		phase:    corev1.PodRunning,
		hydrated: true,
	}

	s.registryMu.Lock()
	s.pods[uid] = ps
	s.nameIndex[pod.Namespace+"/"+pod.Name] = uid
	s.registryMu.Unlock()

	cpu, mem := podResources(pod)
	s.usedCPU.Add(cpu)
	s.usedMem.Add(mem)

	s.triggerSnapshot()
}

type hydratedEntry struct {
	uid string
	pod *corev1.Pod
	ip  string
}

func (s *PodStore) RegisterHydratedBatch(entries []hydratedEntry) {
	var totalCPU, totalMem int64

	s.registryMu.Lock()
	for _, e := range entries {
		s.pods[e.uid] = &podState{
			pod:      e.pod,
			ip:       e.ip,
			phase:    corev1.PodRunning,
			hydrated: true,
		}
		s.nameIndex[e.pod.Namespace+"/"+e.pod.Name] = e.uid

		cpu, mem := podResources(e.pod)
		totalCPU += cpu
		totalMem += mem
	}
	s.registryMu.Unlock()

	s.usedCPU.Add(totalCPU)
	s.usedMem.Add(totalMem)

	s.triggerSnapshot()
}

func (s *PodStore) PatchRestartCount(uid, containerName string, count int32) {
	if ps := s.getPodState(uid); ps != nil {
		ps.mu.Lock()
		if rs, ok := ps.restarts[containerName]; ok {
			rs.count = count
		}
		ps.mu.Unlock()
	}
}

// PatchBackoff restores a CrashLoopBackOff duration for a container from disk.
// Called during HydrateFromRuntime to re-apply persisted backoff values.
func (s *PodStore) PatchBackoff(uid, containerName string, backoffSec float64) {
	if ps := s.getPodState(uid); ps != nil {
		ps.mu.Lock()
		if rs, ok := ps.restarts[containerName]; ok {
			rs.backoff = time.Duration(backoffSec * float64(time.Second))
		}
		ps.mu.Unlock()
	}
}

func (s *PodStore) HydratedUIDs() map[string]bool {
	s.registryMu.RLock()
	defer s.registryMu.RUnlock()

	out := make(map[string]bool)
	for uid, ps := range s.pods {
		ps.mu.RLock()
		if ps.hydrated {
			out[uid] = true
		}
		ps.mu.RUnlock()
	}

	return out
}

func (s *PodStore) ClearHydrated(uid string) {
	if ps := s.getPodState(uid); ps != nil {
		ps.mu.Lock()
		ps.hydrated = false
		ps.mu.Unlock()
		s.triggerSnapshot()
	}
}

func (s *PodStore) PurgeHydrated(staleUIDs []string) {
	var subCPU, subMem int64

	s.registryMu.Lock()
	for _, uid := range staleUIDs {
		if ps, ok := s.pods[uid]; ok {
			ps.mu.RLock()
			cpu, mem := podResources(ps.pod)
			ps.mu.RUnlock()

			subCPU += cpu
			subMem += mem

			if ps.pod != nil {
				delete(s.nameIndex, ps.pod.Namespace+"/"+ps.pod.Name)
			}
			delete(s.pods, uid)
		}
	}
	s.registryMu.Unlock()

	s.usedCPU.Add(-subCPU)
	s.usedMem.Add(-subMem)

	s.triggerSnapshot()
}

// --- Pod Queries ---

func (s *PodStore) GetPod(namespace, name string) (*corev1.Pod, error) {
	s.registryMu.RLock()
	uid, ok := s.nameIndex[namespace+"/"+name]
	s.registryMu.RUnlock()

	if !ok {
		return nil, errdefs.NotFoundf("pod %s/%s not found", namespace, name)
	}

	if ps := s.getPodState(uid); ps != nil {
		ps.mu.RLock()
		defer ps.mu.RUnlock()

		return ps.pod, nil
	}

	return nil, errdefs.NotFoundf("pod %s/%s not found", namespace, name)
}

func (s *PodStore) GetPods() []*corev1.Pod {
	s.registryMu.RLock()
	defer s.registryMu.RUnlock()

	list := make([]*corev1.Pod, 0, len(s.pods))
	for _, ps := range s.pods {
		ps.mu.RLock()
		list = append(list, ps.pod)
		ps.mu.RUnlock()
	}

	return list
}

func (s *PodStore) GetPodCopy(uid string) *corev1.Pod {
	if ps := s.getPodState(uid); ps != nil {
		ps.mu.RLock()
		defer ps.mu.RUnlock()

		if ps.pod != nil {
			return ps.pod.DeepCopy()
		}
	}

	return nil
}

func (s *PodStore) FindPodUID(namespace, podName string) (string, error) {
	s.registryMu.RLock()
	uid, ok := s.nameIndex[namespace+"/"+podName]
	s.registryMu.RUnlock()

	if ok {
		return uid, nil
	}

	return "", fmt.Errorf("pod %s/%s not found", namespace, podName)
}

func (s *PodStore) CompletedPodUID(namespace, name string) string {
	s.completedMu.Lock()
	defer s.completedMu.Unlock()

	return s.completed[namespace+"/"+name].uid
}

func (s *PodStore) LoadSnapshot() []PodSnapshot {
	if p := s.roSnap.Load(); p != nil {
		return *p
	}

	return nil
}

func (s *PodStore) SnapshotPods() []PodSnapshot {
	return s.LoadSnapshot()
}

type PodEntry struct {
	UID   string
	Pod   *corev1.Pod
	Phase corev1.PodPhase
	PodIP string
}

func (s *PodStore) Snapshot() []PodEntry {
	s.registryMu.RLock()
	defer s.registryMu.RUnlock()

	entries := make([]PodEntry, 0, len(s.pods))
	for uid, ps := range s.pods {
		ps.mu.RLock()
		entries = append(entries, PodEntry{
			UID:   uid,
			Pod:   ps.pod,
			Phase: ps.phase,
			PodIP: ps.ip,
		})
		ps.mu.RUnlock()
	}

	return entries
}

func (s *PodStore) ActiveUIDs() map[string]bool {
	s.registryMu.RLock()
	defer s.registryMu.RUnlock()

	uids := make(map[string]bool, len(s.pods))
	for uid := range s.pods {
		uids[uid] = true
	}

	return uids
}

// --- Resource Admission ---

// PSI thresholds for pod admission. When host-wide pressure exceeds these
// values (avg10, percentage of wall time), new pods are rejected until
// pressure subsides. This prevents piling more work onto a stalled host.
const (
	psiCPUThreshold    = 70.0 // "some" - any task waiting for CPU
	psiMemoryThreshold = 15.0 // "full" - all tasks stalled on memory
)

func (s *PodStore) AdmitPod(pod *corev1.Pod, nodeCPU, nodeMem resource.Quantity) string {
	// Host-wide PSI check - reject early if the machine is under pressure.
	if hp, err := psi.Read(); err == nil {
		if hp.CPU.Avg10 > psiCPUThreshold {
			return fmt.Sprintf("Host CPU pressure too high: avg10=%.1f%% (threshold %.0f%%)",
				hp.CPU.Avg10, psiCPUThreshold)
		}
		if hp.Memory.Avg10 > psiMemoryThreshold {
			return fmt.Sprintf("Host memory pressure too high: avg10=%.1f%% (threshold %.0f%%)",
				hp.Memory.Avg10, psiMemoryThreshold)
		}
	}

	podCPU, podMem := podResources(pod)
	if podCPU == 0 && podMem == 0 {
		return ""
	}

	// 0-lock atomic read for admission calculations!
	usedCPU := s.usedCPU.Load()
	usedMem := s.usedMem.Load()

	cpuCap := nodeCPU.MilliValue()
	memCap := nodeMem.Value()

	if cpuCap > 0 && usedCPU+podCPU > cpuCap {
		return fmt.Sprintf("Insufficient cpu: requested %dm, used %dm, capacity %dm",
			podCPU, usedCPU, cpuCap)
	}
	if memCap > 0 && usedMem+podMem > memCap {
		return fmt.Sprintf("Insufficient memory: requested %d, used %d, capacity %d",
			podMem, usedMem, memCap)
	}

	return ""
}

func (s *PodStore) ComputeAllocatable(capacity corev1.ResourceList) corev1.ResourceList {
	usedCPU := s.usedCPU.Load()
	usedMem := s.usedMem.Load()

	alloc := make(corev1.ResourceList, len(capacity))
	for k, v := range capacity {
		alloc[k] = v.DeepCopy()
	}

	if usedCPU > 0 {
		if cap := capacity[corev1.ResourceCPU]; cap.MilliValue() > usedCPU {
			alloc[corev1.ResourceCPU] = *resource.NewMilliQuantity(cap.MilliValue()-usedCPU, resource.DecimalSI)

		} else {
			alloc[corev1.ResourceCPU] = *resource.NewMilliQuantity(0, resource.DecimalSI)
		}
	}
	if usedMem > 0 {
		if cap := capacity[corev1.ResourceMemory]; cap.Value() > usedMem {
			alloc[corev1.ResourceMemory] = *resource.NewQuantity(cap.Value()-usedMem, resource.BinarySI)

		} else {
			alloc[corev1.ResourceMemory] = *resource.NewQuantity(0, resource.BinarySI)
		}
	}

	return alloc
}

// --- Utilities ---

func (s *PodStore) CreateSem() chan struct{} {
	return s.createSem
}

// expireCompleted removes completed entries older than completedTTL.
// Must be called with completedMu held.
func (s *PodStore) expireCompleted() {
	cutoff := time.Now().Add(-completedTTL)
	for k, e := range s.completed {
		if e.deletedAt.Before(cutoff) {
			delete(s.completed, k)
		}
	}
}

// Close shuts down the background snapshot aggregator. Must be called when the
// PodStore is no longer needed (e.g. in tests, or on pawn shutdown).
func (s *PodStore) Close() {
	close(s.stopCh)
}

func (s *PodStore) ProbeRunner() *ProbeRunner {
	return s.probeRunner
}

func (s *PodStore) BumpBackoff(uid, containerName string) (count int32, backoff time.Duration) {
	if ps := s.getPodState(uid); ps != nil {
		ps.mu.Lock()
		defer ps.mu.Unlock()

		if rs, ok := ps.restarts[containerName]; ok {
			rs.count++
			backoff = rs.backoff
			rs.backoff *= 2
			if rs.backoff > 5*time.Minute {
				rs.backoff = 5 * time.Minute
			}

			return rs.count, backoff
		}
	}

	return 0, 0
}

func (s *PodStore) MarkRestarted(uid, containerName string) {
	if ps := s.getPodState(uid); ps != nil {
		ps.mu.Lock()
		if rs, ok := ps.restarts[containerName]; ok {
			rs.lastStarted = time.Now()
		}
		ps.mu.Unlock()
	}
}

// ResetBackoff resets the CrashLoopBackOff duration for a container that has
// been running stably for longer than restartBackoffReset. This is the
// funnel for backoff resets - callers MUST use this method instead of
// directly mutating the containerRestartState struct, because RestartState()
// returns a pointer under only an RLock. Direct mutation outside the write
// lock is a data race with BumpBackoff().
func (s *PodStore) ResetBackoff(uid, containerName string) {
	if ps := s.getPodState(uid); ps != nil {
		ps.mu.Lock()
		if rs, ok := ps.restarts[containerName]; ok {
			rs.backoff = restartBackoffInit
		}
		ps.mu.Unlock()
	}
}

// callerSite returns a short "file:line" for the caller's caller (skip=2).
func callerSite() string {
	if _, file, line, ok := runtime.Caller(2); ok {
		for i := len(file) - 1; i >= 0; i-- {
			if file[i] == '/' {
				file = file[i+1:]

				break
			}
		}

		return fmt.Sprintf("%s:%d", file, line)
	}

	return "unknown"
}
