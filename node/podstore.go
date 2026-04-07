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

// podState acts as the localized "Actor" state for a single pod.
// By giving each pod its own mutex, we eliminate global lock contention
// for highly concurrent events like container probes or restarts across 3000+ pods.
type podState struct {
	mu sync.RWMutex

	pod      *corev1.Pod
	ip       string
	phase    corev1.PodPhase
	hydrated bool
	inFlight *podSaga
	deleting bool
	restarts map[string]*containerRestartState
	probes   map[string]*ContainerProbeState
}

// PodStore is the single source of truth for node-level pod state.
// It implements PodTracker so it can be passed directly to the Reconciler.
type PodStore struct {
	// registryMu protects the maps that hold the global pod topologies.
	// It is ONLY locked when pods are added or removed, never during localized mutations.
	registryMu sync.RWMutex
	pods       map[string]*podState
	nameIndex  map[string]string // "namespace/name" → UID
	completed  map[string]string // "namespace/name" → UID (log fallback)

	// atomic global counters for instant 0-lock Admittance queries
	usedCPU atomic.Int64 // in millicores
	usedMem atomic.Int64 // in bytes

	// roSnap is an asynchronously updated lock-free read replica.
	roSnap    atomic.Pointer[[]PodSnapshot]
	triggerCh chan struct{} // triggers the background aggregator

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
		completed:   make(map[string]string),
		triggerCh:   make(chan struct{}, 1),
		probeRunner: NewProbeRunner(rt, logger),
		createSem:   make(chan struct{}, createConcurrency),
		logger:      logger,
	}

	// Start the background snapshot aggregator
	go store.runSnapshotAggregator()

	return store
}

// ─── Internal Helpers ───────────────────────────────────────────────────────

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
func (s *PodStore) runSnapshotAggregator() {
	for range s.triggerCh {
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

// ─── PodTracker interface ───────────────────────────────────────────────────

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

// ─── Accessors ──────────────────────────────────────────────────────────────

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
	s.registryMu.RLock()
	defer s.registryMu.RUnlock()

	for _, ps := range s.pods {
		ps.mu.RLock()
		deleting := ps.deleting
		ps.mu.RUnlock()
		if deleting {
			return true
		}
	}
	return false
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

// ─── Composite State Mutations ──────────────────────────────────────────────

func (s *PodStore) RegisterPending(uid string, pod *corev1.Pod, saga *podSaga) {
	ps := &podState{
		pod:      pod,
		phase:    corev1.PodPending,
		inFlight: saga,
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
		cpu, mem := podResources(pod)
		s.usedCPU.Add(cpu)
		s.usedMem.Add(mem)
		ps.pod = pod
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
		ps.pod = pod
		ps.ip = ip
		ps.phase = corev1.PodRunning
		ps.inFlight = nil
		ps.mu.Unlock()
		s.triggerSnapshot()
	}
}

func (s *PodStore) MarkDeleting(uid string) {
	if ps := s.getPodState(uid); ps != nil {
		ps.mu.Lock()
		ps.deleting = true
		ps.mu.Unlock()
	}
}

func (s *PodStore) Unregister(uid, namespace, name string) {
	s.registryMu.Lock()
	ps, ok := s.pods[uid]
	if ok {
		delete(s.pods, uid)
		delete(s.nameIndex, namespace+"/"+name)
	}
	s.completed[namespace+"/"+name] = uid
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

func (s *PodStore) SetPhase(uid string, phase corev1.PodPhase) {
	if ps := s.getPodState(uid); ps != nil {
		ps.mu.Lock()
		ps.phase = phase
		ps.mu.Unlock()
		s.triggerSnapshot()
	}
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
		saga := ps.inFlight
		ps.mu.RUnlock()

		if saga != nil {
			saga.cancel()
			<-saga.done
		}
	}
}

// ─── Restart & Probe State ──────────────────────────────────────────────────

func (s *PodStore) InitRestartState(pod *corev1.Pod) {
	uid := string(pod.UID)
	ps := s.getPodState(uid)
	if ps == nil {
		return
	}

	rs := make(map[string]*containerRestartState, len(pod.Spec.Containers))
	probes := make(map[string]*ContainerProbeState, len(pod.Spec.Containers))

	for _, c := range pod.Spec.Containers {
		rs[c.Name] = &containerRestartState{
			backoff:     restartBackoffInit,
			lastStarted: time.Now(),
		}
		probes[c.Name] = &ContainerProbeState{
			StartedAt:     time.Now(),
			Ready:         c.ReadinessProbe == nil,
			LastProbeTime: make(map[string]time.Time),
		}
	}

	ps.mu.Lock()
	ps.restarts = rs
	ps.probes = probes
	ps.mu.Unlock()
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

func (s *PodStore) RestartState(uid, containerName string) *containerRestartState {
	if ps := s.getPodState(uid); ps != nil {
		ps.mu.RLock()
		defer ps.mu.RUnlock()
		return ps.restarts[containerName]
	}
	return nil
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

// ─── Hydration ──────────────────────────────────────────────────────────────

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

// ─── Pod Queries ────────────────────────────────────────────────────────────

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
	s.registryMu.RLock()
	defer s.registryMu.RUnlock()
	return s.completed[namespace+"/"+name]
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

// ─── Resource Admission ─────────────────────────────────────────────────────

func (s *PodStore) AdmitPod(pod *corev1.Pod, nodeCPU, nodeMem resource.Quantity) string {
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

// ─── Utilities ──────────────────────────────────────────────────────────────

func (s *PodStore) CreateSem() chan struct{} {
	return s.createSem
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
