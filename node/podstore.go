package node

import (
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/malformed-c/periapsis/errdefs"
	perigeos "github.com/malformed-c/periapsis/internal/runtime"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// PodStore holds all pod state maps and the mutex that protects them.
// It is the single source of truth for which pods exist, their IPs, phases,
// restart state, probe state, and in-flight/deleting flags.
//
// PodStore implements PodTracker so it can be passed directly to the Reconciler.
type PodStore struct {
	mu            sync.RWMutex
	pods          map[string]*corev1.Pod                       // UID → Pod
	podIPs        map[string]string                            // UID → IP
	podPhases     map[string]corev1.PodPhase                   // UID → last known phase
	hydratedUIDs  map[string]bool                              // UIDs from HydrateFromRuntime
	inFlight      map[string]*podSaga                          // UID → active creation saga
	deleting      map[string]bool                              // UIDs with DeletePod in progress
	completedPods map[string]string                            // "namespace/name" → UID (log fallback)
	restarts      map[string]map[string]*containerRestartState // UID → container → restart state
	probeStates   map[string]map[string]*ContainerProbeState   // UID → container → probe state
	probeRunner   *ProbeRunner
	createSem     chan struct{} // limits concurrent pod creation sagas

	// roSnap is a read-only mirror of pod state, committed atomically on
	// every mutation. Readers (control server, CLI) load it lock-free.
	roSnap atomic.Pointer[[]PodSnapshot]

	logger *slog.Logger
}

// Verify PodStore satisfies PodTracker.
var _ PodTracker = (*PodStore)(nil)

// NewPodStore creates a PodStore with initialized maps.
func NewPodStore(rt perigeos.Runtime, createConcurrency int, logger *slog.Logger) *PodStore {
	return &PodStore{
		pods:          make(map[string]*corev1.Pod),
		podIPs:        make(map[string]string),
		podPhases:     make(map[string]corev1.PodPhase),
		hydratedUIDs:  make(map[string]bool),
		inFlight:      make(map[string]*podSaga),
		deleting:      make(map[string]bool),
		completedPods: make(map[string]string),
		restarts:      make(map[string]map[string]*containerRestartState),
		probeStates:   make(map[string]map[string]*ContainerProbeState),
		probeRunner:   NewProbeRunner(rt, logger),
		createSem:     make(chan struct{}, createConcurrency),
		logger:        logger,
	}
}

// ─── PodTracker interface ───────────────────────────────────────────────────

// IsInFlight reports whether a pod creation goroutine is currently active for uid.
func (s *PodStore) IsInFlight(uid string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.inFlight[uid] != nil
}

// HasPod reports whether the store knows about a pod.
func (s *PodStore) HasPod(uid string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.pods[uid]
	return ok
}

// PodUIDs returns a snapshot of all pod UIDs and their namespace/name.
func (s *PodStore) PodUIDs() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	uids := make(map[string]string, len(s.pods))
	for uid, pod := range s.pods {
		uids[uid] = pod.Namespace + "/" + pod.Name
	}
	return uids
}

// EvictGhost removes a pod from in-memory state without running full teardown.
// Used by the Reconciler for ghost pods.
func (s *PodStore) EvictGhost(uid string) {
	s.mu.Lock()
	delete(s.pods, uid)
	delete(s.podIPs, uid)
	delete(s.podPhases, uid)
	s.commitSnapshot()
	s.mu.Unlock()
}

// ─── Accessors ──────────────────────────────────────────────────────────────

// PodCount returns the number of known pods.
func (s *PodStore) PodCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.pods)
}

// PodIP returns the IP allocated to a pod, or "" if unknown.
func (s *PodStore) PodIP(uid string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.podIPs[uid]
}

// PodPhase returns the last known phase for a pod, or "" if unknown.
func (s *PodStore) PodPhase(uid string) corev1.PodPhase {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.podPhases[uid]
}

// DeletionsInProgress returns true if any pods are currently being deleted.
func (s *PodStore) DeletionsInProgress() bool {
	s.mu.RLock()
	n := len(s.deleting)
	s.mu.RUnlock()
	return n > 0
}

// IsDeleting reports whether a pod is in the delete path.
func (s *PodStore) IsDeleting(uid string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.deleting[uid]
}

// IsContainerReady returns whether a container should be reported as Ready.
func (s *PodStore) IsContainerReady(uid, containerName string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if ps, ok := s.probeStates[uid][containerName]; ok {
		return ps.Ready
	}
	return true // no probe state → default ready
}

// ─── Composite State Mutations ──────────────────────────────────────────────

// RegisterPending records a pod as Pending and sets up its in-flight saga.
// Called at the start of CreatePod.
func (s *PodStore) RegisterPending(uid string, pod *corev1.Pod, saga *podSaga) {
	s.mu.Lock()
	s.inFlight[uid] = saga
	s.pods[uid] = pod
	s.podPhases[uid] = corev1.PodPending
	s.commitSnapshot()
	s.mu.Unlock()
}

// AlreadyRunning checks if a pod is already registered. If it's a hydration
// stub (no containers), it replaces the entry with the full pod. Returns true
// if the pod already exists (caller should skip creation).
func (s *PodStore) AlreadyRunning(uid string, pod *corev1.Pod) (exists bool, wasStub bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	existing, ok := s.pods[uid]
	if !ok {
		return false, false
	}
	if len(existing.Spec.Containers) == 0 {
		s.pods[uid] = pod
		delete(s.hydratedUIDs, uid)
		s.commitSnapshot()
		return true, true
	}
	delete(s.hydratedUIDs, uid)
	s.commitSnapshot()
	return true, false
}

// AlreadyInFlight reports whether a creation saga is already running for uid.
func (s *PodStore) AlreadyInFlight(uid string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.inFlight[uid] != nil
}

// PromoteRunning transitions a pod from Pending to Running after successful creation.
// Clears the in-flight saga. Called at the end of createPodSync.
func (s *PodStore) PromoteRunning(uid string, pod *corev1.Pod, ip string) {
	s.mu.Lock()
	s.pods[uid] = pod
	s.podIPs[uid] = ip
	s.podPhases[uid] = corev1.PodRunning
	delete(s.inFlight, uid)
	s.commitSnapshot()
	s.mu.Unlock()
}

// MarkDeleting flags a pod as being deleted so the batch watcher skips restarts.
func (s *PodStore) MarkDeleting(uid string) {
	s.mu.Lock()
	s.deleting[uid] = true
	s.mu.Unlock()
}

// Unregister removes a pod from all state maps and records its UID in
// completedPods for post-deletion log retrieval.
func (s *PodStore) Unregister(uid, namespace, name string) {
	s.mu.Lock()
	s.completedPods[namespace+"/"+name] = uid
	delete(s.pods, uid)
	delete(s.podIPs, uid)
	delete(s.podPhases, uid)
	delete(s.restarts, uid)
	delete(s.probeStates, uid)
	delete(s.deleting, uid)
	delete(s.inFlight, uid)
	s.commitSnapshot()
	s.mu.Unlock()
}

// SetPhase updates the phase for a pod. Used by BatchWatcher for terminal transitions.
func (s *PodStore) SetPhase(uid string, phase corev1.PodPhase) {
	s.mu.Lock()
	s.podPhases[uid] = phase
	s.commitSnapshot()
	s.mu.Unlock()
}

// MarkFailed records a pod as Failed in the internal maps.
// Returns the updated pod (deep copy with failure status set).
func (s *PodStore) MarkFailed(uid string, pod *corev1.Pod, reason, message string) *corev1.Pod {
	failedPod := pod.DeepCopy()
	failedPod.Status.Phase = corev1.PodFailed
	failedPod.Status.Reason = reason
	failedPod.Status.Message = message
	s.mu.Lock()
	s.pods[uid] = failedPod
	s.podPhases[uid] = corev1.PodFailed
	s.commitSnapshot()
	s.mu.Unlock()
	return failedPod
}

// CancelInFlight cancels the in-flight saga for uid (if any) and waits for
// its compensations to finish. Returns immediately if nothing is in flight.
func (s *PodStore) CancelInFlight(uid string) {
	s.mu.RLock()
	saga := s.inFlight[uid]
	s.mu.RUnlock()
	if saga == nil {
		return
	}
	saga.cancel()
	<-saga.done
}

// ─── Restart & Probe State ──────────────────────────────────────────────────

// InitRestartState initializes CrashLoopBackOff and probe tracking for a
// newly created pod. Must NOT be called while holding the store lock.
func (s *PodStore) InitRestartState(pod *corev1.Pod) {
	uid := string(pod.UID)
	s.mu.Lock()
	rs := make(map[string]*containerRestartState, len(pod.Spec.Containers))
	ps := make(map[string]*ContainerProbeState, len(pod.Spec.Containers))
	for _, c := range pod.Spec.Containers {
		rs[c.Name] = &containerRestartState{
			backoff:     restartBackoffInit,
			lastStarted: time.Now(),
		}
		ps[c.Name] = &ContainerProbeState{
			StartedAt:     time.Now(),
			Ready:         c.ReadinessProbe == nil,
			LastProbeTime: make(map[string]time.Time),
		}
	}
	s.restarts[uid] = rs
	s.probeStates[uid] = ps
	s.mu.Unlock()
}

// RestartCounts returns a snapshot of restart counts for a pod.
func (s *PodStore) RestartCounts(uid string) map[string]int32 {
	s.mu.RLock()
	restartMap := s.restarts[uid]
	s.mu.RUnlock()
	if len(restartMap) == 0 {
		return nil
	}
	counts := make(map[string]int32, len(restartMap))
	for c, r := range restartMap {
		counts[c] = r.count
	}
	return counts
}

// RestartState returns the restart state for a container (count, backoff).
// Returns nil if no state exists.
func (s *PodStore) RestartState(uid, containerName string) *containerRestartState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if m, ok := s.restarts[uid]; ok {
		return m[containerName]
	}
	return nil
}

// ProbeState returns the probe state for a container (nil if none).
func (s *PodStore) ProbeState(uid, containerName string) *ContainerProbeState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if m, ok := s.probeStates[uid]; ok {
		return m[containerName]
	}
	return nil
}

// UpdateProbeState applies fn to the probe state under the write lock.
// If no probe state exists, fn is not called.
func (s *PodStore) UpdateProbeState(uid, containerName string, fn func(*ContainerProbeState)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if m, ok := s.probeStates[uid]; ok {
		if ps, ok := m[containerName]; ok {
			fn(ps)
		}
	}
}

// ResetProbeState replaces the probe state for a container with a fresh one.
// Used by liveness probe failure to reset startup state after restart.
func (s *PodStore) ResetProbeState(uid, containerName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if m, ok := s.probeStates[uid]; ok {
		m[containerName] = &ContainerProbeState{
			StartedAt:     time.Now(),
			LastProbeTime: make(map[string]time.Time),
		}
	}
}

// IncrementRestart bumps the restart count and updates lastStarted for a container.
func (s *PodStore) IncrementRestart(uid, containerName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if m, ok := s.restarts[uid]; ok {
		if rs, ok := m[containerName]; ok {
			rs.count++
			rs.lastStarted = time.Now()
		}
	}
}

// ─── Hydration ──────────────────────────────────────────────────────────────

// RegisterHydrated bulk-registers a pod from disk state during startup.
func (s *PodStore) RegisterHydrated(uid string, pod *corev1.Pod, ip string) {
	s.mu.Lock()
	s.pods[uid] = pod
	if ip != "" {
		s.podIPs[uid] = ip
	}
	s.podPhases[uid] = corev1.PodRunning
	s.hydratedUIDs[uid] = true
	s.commitSnapshot()
	s.mu.Unlock()
}

// RegisterHydratedBatch registers multiple pods from disk state in a single lock.
func (s *PodStore) RegisterHydratedBatch(entries []hydratedEntry) {
	s.mu.Lock()
	for _, e := range entries {
		s.pods[e.uid] = e.pod
		if e.ip != "" {
			s.podIPs[e.uid] = e.ip
		}
		s.podPhases[e.uid] = corev1.PodRunning
		s.hydratedUIDs[e.uid] = true
	}
	s.commitSnapshot()
	s.mu.Unlock()
}

// hydratedEntry is a batch element for RegisterHydratedBatch.
type hydratedEntry struct {
	uid string
	pod *corev1.Pod
	ip  string
}

// PatchRestartCount sets the restart count for a specific container.
// Used after hydration to restore persisted counts.
func (s *PodStore) PatchRestartCount(uid, containerName string, count int32) {
	s.mu.Lock()
	if rs, ok := s.restarts[uid][containerName]; ok {
		rs.count = count
	}
	s.mu.Unlock()
}

// HydratedUIDs returns a copy of the hydrated UID set.
func (s *PodStore) HydratedUIDs() map[string]bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]bool, len(s.hydratedUIDs))
	for uid := range s.hydratedUIDs {
		out[uid] = true
	}
	return out
}

// ClearHydrated removes a UID from the hydrated set (k8s confirmed this pod).
func (s *PodStore) ClearHydrated(uid string) {
	s.mu.Lock()
	delete(s.hydratedUIDs, uid)
	s.commitSnapshot()
	s.mu.Unlock()
}

// PurgeHydrated removes pods from state that are in the hydrated set.
// Returns the purged UIDs. Used by PurgeStaleHydrated.
func (s *PodStore) PurgeHydrated(staleUIDs []string) {
	s.mu.Lock()
	for _, uid := range staleUIDs {
		delete(s.pods, uid)
		delete(s.podIPs, uid)
		delete(s.podPhases, uid)
		delete(s.hydratedUIDs, uid)
		delete(s.restarts, uid)
		delete(s.probeStates, uid)
	}
	s.commitSnapshot()
	s.mu.Unlock()
}

// ─── Pod Queries ────────────────────────────────────────────────────────────

// GetPod returns a pod by namespace and name.
func (s *PodStore) GetPod(namespace, name string) (*corev1.Pod, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, pod := range s.pods {
		if pod.Namespace == namespace && pod.Name == name {
			return pod, nil
		}
	}
	return nil, errdefs.NotFoundf("pod %s/%s not found", namespace, name)
}

// GetPods returns all known pods.
func (s *PodStore) GetPods() []*corev1.Pod {
	s.mu.RLock()
	defer s.mu.RUnlock()
	list := make([]*corev1.Pod, 0, len(s.pods))
	for _, pod := range s.pods {
		list = append(list, pod)
	}
	return list
}

// GetPodCopy returns a deep copy of a pod by UID (nil if not found).
func (s *PodStore) GetPodCopy(uid string) *corev1.Pod {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if p, ok := s.pods[uid]; ok {
		return p.DeepCopy()
	}
	return nil
}

// FindPodUID looks up a pod UID by namespace and name.
func (s *PodStore) FindPodUID(namespace, podName string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, pod := range s.pods {
		if pod.Namespace == namespace && pod.Name == podName {
			return string(pod.UID), nil
		}
	}
	return "", fmt.Errorf("pod %s/%s not found", namespace, podName)
}

// CompletedPodUID returns the UID of a recently-deleted pod for log retrieval.
func (s *PodStore) CompletedPodUID(namespace, name string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.completedPods[namespace+"/"+name]
}

// commitSnapshot rebuilds the read-only snapshot from current state.
// Must be called while holding s.mu (write lock).
func (s *PodStore) commitSnapshot() {
	snaps := make([]PodSnapshot, 0, len(s.pods))
	for uid, pod := range s.pods {
		if s.hydratedUIDs[uid] {
			continue
		}
		snaps = append(snaps, PodSnapshot{
			Name:       pod.Name,
			Namespace:  pod.Namespace,
			UID:        uid,
			IP:         s.podIPs[uid],
			Phase:      s.podPhases[uid],
			Containers: len(pod.Spec.Containers),
		})
	}
	s.roSnap.Store(&snaps)
}

// LoadSnapshot returns the current read-only pod snapshot. Lock-free.
func (s *PodStore) LoadSnapshot() []PodSnapshot {
	if p := s.roSnap.Load(); p != nil {
		return *p
	}
	return nil
}

// SnapshotPods returns a lightweight snapshot of all pods in a single lock.
// Returns nil if the lock cannot be acquired immediately.
func (s *PodStore) SnapshotPods() []PodSnapshot {
	return s.LoadSnapshot()
}

// Snapshot returns all active pods with phase and IP for batch processing.
type PodEntry struct {
	UID   string
	Pod   *corev1.Pod
	Phase corev1.PodPhase
	PodIP string
}

// Snapshot returns all pods with their phase and IP in a single lock acquisition.
func (s *PodStore) Snapshot() []PodEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entries := make([]PodEntry, 0, len(s.pods))
	for uid, pod := range s.pods {
		entries = append(entries, PodEntry{
			UID:   uid,
			Pod:   pod,
			Phase: s.podPhases[uid],
			PodIP: s.podIPs[uid],
		})
	}
	return entries
}

// ActiveUIDs returns the set of known pod UIDs for stale unit cleanup.
func (s *PodStore) ActiveUIDs() map[string]bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	uids := make(map[string]bool, len(s.pods))
	for uid := range s.pods {
		uids[uid] = true
	}
	return uids
}

// ─── Resource Admission ─────────────────────────────────────────────────────

// AdmitPod checks if the pod's resource requests fit within remaining node capacity.
// Returns an empty string if admitted, or a reason string if rejected.
func (s *PodStore) AdmitPod(pod *corev1.Pod, nodeCPU, nodeMem resource.Quantity) string {
	var podCPU, podMem int64
	for _, c := range pod.Spec.Containers {
		if req, ok := c.Resources.Requests[corev1.ResourceCPU]; ok {
			podCPU += req.MilliValue()
		}
		if req, ok := c.Resources.Requests[corev1.ResourceMemory]; ok {
			podMem += req.Value()
		}
	}
	if podCPU == 0 && podMem == 0 {
		return ""
	}

	s.mu.RLock()
	var usedCPU, usedMem int64
	for _, p := range s.pods {
		for _, c := range p.Spec.Containers {
			if req, ok := c.Resources.Requests[corev1.ResourceCPU]; ok {
				usedCPU += req.MilliValue()
			}
			if req, ok := c.Resources.Requests[corev1.ResourceMemory]; ok {
				usedMem += req.Value()
			}
		}
	}
	s.mu.RUnlock()

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

// ComputeAllocatable returns Capacity minus the sum of resource requests from
// all currently tracked pods.
func (s *PodStore) ComputeAllocatable(capacity corev1.ResourceList) corev1.ResourceList {
	var usedCPU, usedMem int64
	s.mu.RLock()
	for _, p := range s.pods {
		for _, c := range p.Spec.Containers {
			if req, ok := c.Resources.Requests[corev1.ResourceCPU]; ok {
				usedCPU += req.MilliValue()
			}
			if req, ok := c.Resources.Requests[corev1.ResourceMemory]; ok {
				usedMem += req.Value()
			}
		}
	}
	s.mu.RUnlock()

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

// CreateSem returns the channel that limits concurrent pod creation sagas.
func (s *PodStore) CreateSem() chan struct{} {
	return s.createSem
}

// ProbeRunner returns the ProbeRunner instance for running container probes.
func (s *PodStore) ProbeRunner() *ProbeRunner {
	return s.probeRunner
}

// BumpBackoff increments restart count, applies backoff doubling, and returns
// the current count and backoff duration. Returns (0, 0) if no state exists.
func (s *PodStore) BumpBackoff(uid, containerName string) (count int32, backoff time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if m, ok := s.restarts[uid]; ok {
		if rs, ok := m[containerName]; ok {
			rs.count++
			backoff = rs.backoff
			rs.backoff *= 2
			if rs.backoff > 5*time.Minute { // restartBackoffMax
				rs.backoff = 5 * time.Minute
			}
			return rs.count, backoff
		}
	}
	return 0, 0
}

// MarkRestarted sets the lastStarted time to now for a container.
// Used after a successful container restart.
func (s *PodStore) MarkRestarted(uid, containerName string) {
	s.mu.Lock()
	if m, ok := s.restarts[uid]; ok {
		if rs, ok := m[containerName]; ok {
			rs.lastStarted = time.Now()
		}
	}
	s.mu.Unlock()
}
