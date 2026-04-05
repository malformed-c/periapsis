package node

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/malformed-c/periapsis/internal/config"
	"github.com/malformed-c/periapsis/internal/downward"
	"github.com/malformed-c/periapsis/internal/image"
	"github.com/malformed-c/periapsis/internal/network"
	"github.com/malformed-c/periapsis/internal/pki"
	perigeos "github.com/malformed-c/periapsis/internal/runtime"
	pawnstats "github.com/malformed-c/periapsis/internal/stats"
	"github.com/malformed-c/periapsis/internal/volume"
	"github.com/malformed-c/periapsis/node/api"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	listersv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"

	"github.com/malformed-c/periapsis/internal/manager"
	"github.com/malformed-c/periapsis/internal/podutils"
)

const (
	// machineStartTimeout is how long CreatePod waits for the machine to become
	// active in systemd before giving up. Keeps inFlight alive for this window.
	machineStartTimeout  = 60 * time.Second
	initContainerTimeout = 10 * time.Minute

	// createBackoffInit is the initial delay between create retries.
	// Doubled each attempt, capped at createBackoffMax.
	createBackoffInit = 2 * time.Second
	createBackoffMax  = 5 * time.Minute

	// restartBackoffInit is the initial CrashLoopBackOff delay for
	// container restarts. Doubled each attempt, capped at restartBackoffMax.
	// Reset after a container runs successfully for restartBackoffReset.
	restartBackoffInit  = 10 * time.Second
	restartBackoffMax   = 5 * time.Minute
	restartBackoffReset = 10 * time.Minute

	// containerWatchPoll is how often the watcher checks container states.
	containerWatchPoll = 2 * time.Second

	// DefaultCreateConcurrency limits how many pod creation sagas run in
	// parallel per pawn. Extra CreatePod calls queue in inFlight (returning
	// Pending to k8s) until a slot opens. Prevents burst load from
	// overwhelming the Cilium agent's endpoint queue.
	DefaultCreateConcurrency = 5
)

// Gambit is the periapsis provider. It implements the PodProvider interface
// and orchestrates image pulling, overlayfs mounting, network setup,
// and systemd-nspawn machine management for each pod.
type Gambit struct {
	Config         config.PawnConfig
	ImageManager   *image.ImageManager
	NetworkManager network.NetworkManager
	Runtime        perigeos.Runtime
	Logger         *slog.Logger
	Tidal          *downward.Tidal
	EventRecorder  record.EventRecorder

	// Listers for env population and volume resolution.
	// Set via GambitDeps during construction.
	cmLister     listersv1.ConfigMapLister
	secretLister listersv1.SecretLister
	svcLister    listersv1.ServiceLister
	kubeClient   kubernetes.Interface
	clusterDNS   string // ClusterIP of kube-dns, written to container resolv.conf

	volumes *VolumeTracker // live ConfigMap/Secret volume refresh

	store        *PodStore    // pod state maps and mutex
	batchWatcher *BatchWatcher // single watcher per pawn (replaces per-pod watchers)

	// podNotify is the callback registered by NotifyPods. When set, Gambit
	// pushes pod status changes to the PodController instead of relying on
	// the syncProviderWrapper's 5-second polling loop.
	podNotify func(*corev1.Pod)

	// syncRequester is the forward reconciler callback. When set, Gambit
	// (via BatchWatcher or Reconciler) can request the PodController re-sync
	// a pod from the K8s side. The sync handler's podsEffectivelyEqual
	// check prevents infinite loops.
	syncRequester func(namespace, name string)

	node *PawnNode
}

// podSaga tracks an in-flight pod creation or a running watcher.
// The cancel func signals the goroutine to stop; done is closed when it exits.
type podSaga struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// containerRestartState tracks CrashLoopBackOff state for a single container.
type containerRestartState struct {
	count       int32
	backoff     time.Duration
	lastStarted time.Time // when the container last entered Running; used to reset backoff
}

// parseUnitName extracts (uid, containerName) from a systemd unit name.
// Format: perigeos-<pawn>-pod-<uid>-<containerName>.service
// Returns ("", "") if the unit name doesn't match this pawn's prefix.
// ParseUnitName extracts the pod UID and container name from a systemd unit name.
// Format: perigeos-<pawn>-pod-<uid>-<containerName>.service
func ParseUnitName(pawnName, unitName string) (uid, containerName string) {
	prefix := "perigeos-" + pawnName + "-pod-"
	suffix := ".service"
	if !strings.HasPrefix(unitName, prefix) || !strings.HasSuffix(unitName, suffix) {
		return "", ""
	}
	// Strip prefix and suffix to get "<uid>-<containerName>"
	inner := unitName[len(prefix) : len(unitName)-len(suffix)]
	// UIDs are standard 36-char UUIDs (8-4-4-4-12 with hyphens).
	if len(inner) < 38 { // 36 (UUID) + 1 (hyphen) + at least 1 char
		return "", ""
	}
	return inner[:36], inner[37:]
}

// resolveNodeIP returns the IP that this pawn advertises to the apiserver.
// cfg.NodeIP takes precedence; if unset, the default outbound IP is used.
// Override via node_ip in perigeos.toml when the control plane cannot reach
// the default outbound IP (e.g. control plane running inside a container).
func resolveNodeIP(cfg config.PawnConfig) string {
	if cfg.NodeIP != "" {
		return cfg.NodeIP
	}
	if ip := pki.GetOutboundIP(); ip != nil {
		return ip.String()
	}
	return "127.0.0.1"
}

func createConcurrency(cfg config.PawnConfig) int {
	if cfg.CreateConcurrency > 0 {
		return cfg.CreateConcurrency
	}
	return DefaultCreateConcurrency
}

// GambitDeps holds all dependencies for Gambit. Passed to NewGambit.
type GambitDeps struct {
	Config         config.PawnConfig
	Store          *PodStore
	Volumes        *VolumeTracker
	Node           *PawnNode
	ImageManager   *image.ImageManager
	NetworkManager network.NetworkManager
	Runtime        perigeos.Runtime
	Logger         *slog.Logger
	EventRecorder  record.EventRecorder

	// K8s listers for env population and volume resolution.
	CMLister     listersv1.ConfigMapLister
	SecretLister listersv1.SecretLister
	SvcLister    listersv1.ServiceLister
	KubeClient   kubernetes.Interface
	ClusterDNS   string

	// API server address injected into pods (optional).
	APIServerHost string
	APIServerPort string

	// Informers for live ConfigMap/Secret volume refresh (optional).
	CMInformer     cache.SharedIndexInformer
	SecretInformer cache.SharedIndexInformer
}

func NewGambit(deps GambitDeps) *Gambit {
	nodeIP := resolveNodeIP(deps.Config)
	g := &Gambit{
		Config:         deps.Config,
		ImageManager:   deps.ImageManager,
		NetworkManager: deps.NetworkManager,
		Runtime:        deps.Runtime,
		Logger:         deps.Logger,
		Tidal:          downward.NewTidal(deps.Config.Name, nodeIP),
		EventRecorder:  deps.EventRecorder,
		store:          deps.Store,
		volumes:        deps.Volumes,
		node:           deps.Node,
		cmLister:       deps.CMLister,
		secretLister:   deps.SecretLister,
		svcLister:      deps.SvcLister,
		kubeClient:     deps.KubeClient,
		clusterDNS:     deps.ClusterDNS,
	}
	if deps.APIServerHost != "" {
		g.Tidal.SetAPIServer(deps.APIServerHost, deps.APIServerPort)
	}
	if deps.CMInformer != nil {
		deps.CMInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
			UpdateFunc: func(_, obj interface{}) {
				cm := obj.(*corev1.ConfigMap)
				g.volumes.RefreshConfigMap(cm)
			},
		})
	}
	if deps.SecretInformer != nil {
		deps.SecretInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
			UpdateFunc: func(_, obj interface{}) {
				s := obj.(*corev1.Secret)
				g.volumes.RefreshSecret(s)
			},
		})
	}
	return g
}

// SetSyncRequester registers the forward reconciler callback. The provider
// calls fn(namespace, name) to request the PodController re-sync a pod.
func (g *Gambit) SetSyncRequester(fn func(namespace, name string)) {
	g.syncRequester = fn
}

// RequestSync asks the PodController to re-evaluate a pod from the K8s side.
// Safe to call from any goroutine. No-op if SetSyncRequester was never called.
func (g *Gambit) RequestSync(namespace, name string) {
	if g.syncRequester != nil {
		g.syncRequester(namespace, name)
	}
}

// IsInFlight reports whether a pod creation goroutine is currently active for uid.
// Used by the Reconciler to avoid killing machines mid-creation.
func (g *Gambit) IsInFlight(uid string) bool {
	return g.store.IsInFlight(uid)
}

// cancelInFlight cancels the in-flight saga for uid (if any) and waits for
// its compensations to finish. Returns immediately if nothing is in flight.
// Called by DeletePod so the delete path never races with a create in progress.
func (g *Gambit) cancelInFlight(uid string) {
	g.store.CancelInFlight(uid)
}

// StopBatchWatcher stops the batch watcher. Called during graceful shutdown.
func (g *Gambit) StopBatchWatcher() {
	if g.batchWatcher != nil {
		g.batchWatcher.Stop()
	}
}

// PodCount returns the number of pods known to this pawn.
func (g *Gambit) PodCount() int {
	return g.store.PodCount()
}

// PodIP returns the IP allocated to a pod, or "" if unknown.
func (g *Gambit) PodIP(uid string) string {
	return g.store.PodIP(uid)
}

// PodPhase returns the last known phase for a pod, or "" if unknown.
func (g *Gambit) PodPhase(uid string) corev1.PodPhase {
	return g.store.PodPhase(uid)
}

// NodeIP returns the IP this pawn advertises to the apiserver.
func (g *Gambit) NodeIP() string {
	return resolveNodeIP(g.Config)
}

// PodUIDs returns a snapshot of all pod UIDs known to this gambit.
func (g *Gambit) PodUIDs() map[string]string {
	return g.store.PodUIDs()
}

// HasPod reports whether Gambit's in-memory state knows about a pod.
func (g *Gambit) HasPod(uid string) bool {
	return g.store.HasPod(uid)
}

// EvictGhost removes a pod from Gambit's in-memory state without running
// the full DeletePod teardown (no machine stop, no CNI DEL).
// Used by the Reconciler for ghost pods — pods in gambit's map that have
// no systemd unit and are no longer desired by Kubernetes.
func (g *Gambit) EvictGhost(uid string) {
	g.store.EvictGhost(uid)
}

// HydrateFromRuntime re-populates in-memory state from persisted pod-state.json
// files written at pod creation time. Falls back to reading live systemd units
// for any pods not found on disk (e.g. pods created before this version).
// Called once at startup before controllers run.
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
		// Skip terminal pods — they completed before the restart and should
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
		// InitRestartState resets restarts — re-apply the persisted counts.
		if len(state.Restarts) > 0 {
			uid := string(state.Pod.UID)
			for cname, count := range state.Restarts {
				g.store.PatchRestartCount(uid, cname, count)
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
		// If CreatePod was called for this UID, it's confirmed — skip.
		// (CreatePod replaces the hydration stub with the full pod object,
		// which has Spec.Containers populated.)
		pod := g.store.GetPodCopy(uid)
		if pod != nil && len(pod.Spec.Containers) > 0 {
			continue
		}
		// Check the informer cache — if k8s doesn't know about it, it's stale.
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

// ─── Node ────────────────────────────────────────────────────────────────────

func (g *Gambit) BuildNode() *corev1.Node {
	return g.node.BuildNode()
}

// Shutdown signals this pawn to begin graceful shutdown. Ping will return an
// error, causing VK to stop renewing the lease. The node becomes NotReady
// after ~40s, and the scheduler stops placing pods on it.
func (g *Gambit) Shutdown() {
	g.node.Shutdown()
}

// DeletionsInProgress returns true if any pods are currently being deleted.
func (g *Gambit) DeletionsInProgress() bool {
	return g.store.DeletionsInProgress()
}

// DrainPods actively stops all running pods on this pawn. Unlike the passive
// drain (waiting for apiserver DeletePod calls), this directly stops containers
// and cleans up resources. Call after Shutdown().
func (g *Gambit) DrainPods(ctx context.Context) {
	g.node.DrainPods(ctx)
}

// NotifyPods registers a callback for asynchronous pod status updates.
// When set, the BatchWatcher pushes status changes directly to the
// PodController instead of relying on the 5-second polling loop.
func (g *Gambit) NotifyPods(_ context.Context, cb func(*corev1.Pod)) {
	g.podNotify = cb
}

func (g *Gambit) Ping(ctx context.Context) error {
	return g.node.Ping(ctx)
}

func (g *Gambit) NotifyNodeStatus(ctx context.Context, cb func(*corev1.Node)) {
	g.node.NotifyNodeStatus(ctx, cb)
}

// notifyPodStatus pushes an updated pod to the PodController if a callback
// is registered, and persists the pod state to disk so it survives restarts.
// This is the single funnel for all status changes — persist here rather
// than scattering writePodState across every call site.
func (g *Gambit) notifyPodStatus(pod *corev1.Pod) {
	var caller string
	if _, file, line, ok := runtime.Caller(1); ok {
		caller = fmt.Sprintf("%s:%d", filepath.Base(file), line)
	}
	g.Logger.Debug("notifyPodStatus", "pod", pod.Name, "phase", pod.Status.Phase, "caller", caller)

	if g.podNotify != nil {
		g.podNotify(pod)
	}

	// Persist state to disk. Terminal pods (Succeeded/Failed) are persisted
	// so HydrateFromRuntime knows not to resurrect them.
	uid := string(pod.UID)
	podIP := g.store.PodIP(uid)
	counts := g.store.RestartCounts(uid)
	if err := writePodState(g.Config.BaseDir, g.Config.Name, &PersistedPodState{
		Pod:      pod,
		PodIP:    podIP,
		Phase:    pod.Status.Phase,
		Restarts: counts,
	}); err != nil {
		g.Logger.Warn("Failed to persist pod state", "pod", pod.Name, "err", err)
	}
}

// NotifyPodStatus is the exported wrapper for notifyPodStatus, used as a BatchWatcher callback.
func (g *Gambit) NotifyPodStatus(pod *corev1.Pod) {
	g.notifyPodStatus(pod)
}

// RestartContainerCB is the exported wrapper for restartContainer callback.
func (g *Gambit) RestartContainerCB(ctx context.Context, uid string, pod *corev1.Pod, containerName string) {
	g.restartContainer(ctx, uid, pod, containerName)
}

// BuildPodStatusCB is the exported wrapper for buildPodStatus callback.
func (g *Gambit) BuildPodStatusCB(pod *corev1.Pod, stateLookup func(string, string) perigeos.MachineState) *corev1.PodStatus {
	return g.buildPodStatus(pod, stateLookup)
}

// ─── Pod Lifecycle ───────────────────────────────────────────────────────────

// CreatePod dispatches pod creation to a goroutine and returns immediately.
// The saga is cancelled and its compensations run if DeletePod arrives
// before creation completes, preventing dangling machines and netns leaks.
func (g *Gambit) CreatePod(ctx context.Context, pod *corev1.Pod) error {
	g.Logger.Info("CreatePod", "pawn", g.Config.Name, "namespace", pod.Namespace, "pod", pod.Name)

	if len(pod.Spec.Containers) == 0 {
		return nil
	}

	// Pod admission: reject if the pod's resource requests exceed available
	// node capacity. Prevents overcommit that cascading OOM kills.
	if reason := g.admitPod(pod); reason != "" {
		g.Logger.Warn("Pod admission rejected", "pod", pod.Name, "reason", reason)
		if g.EventRecorder != nil {
			g.EventRecorder.Eventf(pod, corev1.EventTypeWarning, "FailedAdmission", reason)
		}
		return fmt.Errorf("pod admission: %s", reason)
	}

	uid := string(pod.UID)

	// Check if already in-flight
	if g.store.AlreadyInFlight(uid) {
		g.Logger.Info("CreatePod: already in-flight, skipping", "pod", pod.Name)
		return nil
	}

	// After a restart HydrateFromRuntime re-populates store from running
	// systemd units. When the informer reconnects it calls CreatePod for
	// every pod it knows about — skip pods that are already running so we
	// don't tear down and recreate a healthy machine.
	if exists, wasStub := g.store.AlreadyRunning(uid, pod); exists {
		if wasStub {
			// Also initialize probe state — it was never set because
			// createPodSync was skipped for already-running pods.
			g.store.InitRestartState(pod)
		}
		g.Logger.Info("CreatePod: already running (hydrated from runtime), skipping", "pod", pod.Name)
		return nil
	}

	sagaCtx, cancel := context.WithCancel(context.Background())
	saga := &podSaga{cancel: cancel, done: make(chan struct{})}
	// Register immediately as Pending so GetPodStatus never returns NotFound
	// while the pod is queued waiting for a createSem slot. Without this,
	// VK interprets NotFound as the pod not existing and may issue DeletePod,
	// cancelling the saga and causing a create/cancel loop.
	g.store.RegisterPending(uid, pod, saga)

	go func() {
		defer close(saga.done)
		defer cancel()

		// Wait for a creation slot. If the saga is cancelled (DeletePod
		// arrived while we were queued) bail out without starting work.
		createSem := g.store.CreateSem()
		select {
		case createSem <- struct{}{}:
		case <-sagaCtx.Done():
			return
		}
		defer func() { <-createSem }()

		neverRestart := pod.Spec.RestartPolicy == corev1.RestartPolicyNever
		backoff := createBackoffInit
		for attempt := 1; ; attempt++ {
			err := g.createPodSync(sagaCtx, pod)
			if err == nil {
				// Creation succeeded — init restart state for the batch watcher.
				g.initRestartState(pod)
				return
			}
			g.Logger.Warn("CreatePod attempt failed",
				"pod", pod.Name, "attempt", attempt, "err", err)
			g.EventRecorder.Eventf(pod, corev1.EventTypeWarning, "FailedCreate",
				"creation attempt %d failed: %v", attempt, err)

			// restartPolicy: Never → don't retry, mark Failed immediately.
			if neverRestart {
				g.Logger.Error("CreatePod failed (restartPolicy=Never)", "pod", pod.Name, "err", err)
				g.markPodFailed(uid, pod, err)
				return
			}

			// Pod stays in Pending phase during retries — k8s sees it as
			// "still being created" and won't schedule an overshoot replacement.
			select {
			case <-time.After(backoff):
			case <-sagaCtx.Done():
				return
			}
			backoff *= 2
			if backoff > createBackoffMax {
				backoff = createBackoffMax
			}
		}
	}()

	return nil
}

// extractResourceLimits reads memory and CPU limits from a container spec.
func extractResourceLimits(c *corev1.Container) (memBytes uint64, cpuMillis int64) {
	if c.Resources.Limits != nil {
		if mem, ok := c.Resources.Limits[corev1.ResourceMemory]; ok {
			memBytes = uint64(mem.Value())
		}
		if cpu, ok := c.Resources.Limits[corev1.ResourceCPU]; ok {
			cpuMillis = cpu.MilliValue()
		}
	}
	return
}

// isPrivileged reports whether the container requests privileged mode.
func isPrivileged(c *corev1.Container) bool {
	return c.SecurityContext != nil &&
		c.SecurityContext.Privileged != nil &&
		*c.SecurityContext.Privileged
}

// createPodSync runs the full pod creation sequence within a saga.
// Each completed step registers a compensation; on any failure (including
// context cancellation from a concurrent DeletePod) compensations run in
// reverse, ensuring no partial state is left on disk or in systemd.
func (g *Gambit) createPodSync(ctx context.Context, pod *corev1.Pod) error {
	uid := string(pod.UID)
	g.setKind(pod)

	// Pre-flight: verify machined can accept new registrations.
	// Failing fast here avoids a 60s waitForContainer timeout when
	// nspawn exits immediately with "Failed to pin client process: Too many open files".
	if err := g.Runtime.CheckMachined(ctx); err != nil {
		g.EventRecorder.Eventf(pod, corev1.EventTypeWarning, "FailedPreFlight", "machined check failed: %v", err)
		return fmt.Errorf("machined pre-flight: %w", err)
	}

	// saga accumulates compensations for completed steps.
	// compensate() runs them in reverse order (LIFO).
	var compensations []func()
	compensate := func() {
		for i := len(compensations) - 1; i >= 0; i-- {
			compensations[i]()
		}
	}

	// Step 1: network setup.
	// HostNetwork pods share the host network namespace — skip CNI and use
	// /proc/1/ns/net directly so the container joins the host netns.
	var netPath, podIP string
	if pod.Spec.HostNetwork {
		netPath = "/proc/1/ns/net"
		podIP = resolveNodeIP(g.Config)
	} else {
		var err error
		netPath, podIP, err = g.NetworkManager.Setup(ctx, uid, pod.Namespace, pod.Name, pod.Spec.NodeName)
		if err != nil {
			g.EventRecorder.Eventf(pod, corev1.EventTypeWarning, "FailedNetwork", "CNI setup failed: %v", err)
			return fmt.Errorf("network setup: %w", err)
		}
		compensations = append(compensations, func() {
			_ = g.NetworkManager.Teardown(context.Background(), uid, pod.Namespace, pod.Name)
		})
	}

	// Register pod workspace cleanup as early compensation (runs last in LIFO).
	compensations = append(compensations, func() {
		volResolver := volume.NewResolver(g.Config.BaseDir, g.Config.Name, uid, nil, nil, nil)
		_ = volResolver.Cleanup()
		podDir := filepath.Join(g.Config.BaseDir, "pawns", g.Config.Name, "pods", uid)
		_ = os.RemoveAll(podDir)
	})

	// Populate environment variables now that podIP is known.
	// This resolves ConfigMap/Secret envFrom, FieldRef (including status.podIP),
	// service env vars, and $(var) expansion — all with the correct podIP.
	pod.Status.PodIP = podIP
	pod.Status.PodIPs = []corev1.PodIP{{IP: podIP}}
	pod.Status.HostIP = resolveNodeIP(g.Config)
	rm, _ := manager.NewResourceManager(nil, g.secretLister, g.cmLister, g.svcLister)
	if err := podutils.PopulateEnvironmentVariables(ctx, pod, rm, g.EventRecorder); err != nil {
		g.EventRecorder.Eventf(pod, corev1.EventTypeWarning, "FailedPopulateEnv", "environment variable resolution failed: %v", err)
		compensate()
		return fmt.Errorf("env population: %w", err)
	}

	// Pod-level image pull cache to avoid re-pulling the same image for multiple containers.
	type pullCacheEntry struct {
		layers []string
		cached bool
	}
	pullCache := make(map[string]pullCacheEntry)

	// Run init containers sequentially; each must exit 0 before the next starts.
	for i := range pod.Spec.InitContainers {
		ic := &pod.Spec.InitContainers[i]
		g.Logger.Info("Starting init container", "pod", pod.Name, "container", ic.Name)

		// Check pod-level cache first before pulling from registry.
		var layers []string
		var cached bool
		if entry, hit := pullCache[ic.Image]; hit {
			// Cache hit: reuse layers from previous pull in this pod.
			layers = entry.layers
			cached = true
		} else {
			// Cache miss: pull normally and store result.
			g.EventRecorder.Eventf(pod, corev1.EventTypeNormal, "Pulling", "Pulling image %s for init container %s", ic.Image, ic.Name)
			var pullCached bool
			var err error
			layers, pullCached, err = g.ImageManager.PullWithOptions(ic.Image, string(ic.ImagePullPolicy),
				image.PullOptions{
					Progress: pullProgressFunc(g, pod, ic.Image, ic.Name),
					Event:    podEventFn(g, pod),
				})
			if err != nil {
				g.EventRecorder.Eventf(pod, corev1.EventTypeWarning, "FailedPull", "Pull %s: %v", ic.Name, err)
				compensate()
				return fmt.Errorf("pull init container %s: %w", ic.Name, err)
			}
			// Cache the successful pull result.
			pullCache[ic.Image] = pullCacheEntry{layers: layers, cached: pullCached}
		}
		if cached {
			g.EventRecorder.Eventf(pod, corev1.EventTypeNormal, "Cached", "Image %s already present (pod cache) for init container %s", ic.Image, ic.Name)
		} else {
			g.EventRecorder.Eventf(pod, corev1.EventTypeNormal, "Pulled", "Pulled image %s for init container %s", ic.Image, ic.Name)
		}

		rootfs, err := g.ImageManager.Mount(uid+"-"+ic.Name, layers)
		if err != nil {
			g.EventRecorder.Eventf(pod, corev1.EventTypeWarning, "FailedMount", "Mount %s: %v", ic.Name, err)
			compensate()
			return fmt.Errorf("mount init container %s: %w", ic.Name, err)
		}
		g.EventRecorder.Eventf(pod, corev1.EventTypeNormal, "Created", "Created init container %s", ic.Name)
		// Init containers are ephemeral: unmount immediately after they exit.
		// No compensation registered — we unmount inline below.

		resolvedEnv := g.Tidal.ResolveEnv(pod, ic, podIP)
		volResolver := volume.NewResolver(g.Config.BaseDir, g.Config.Name, uid, g.cmLister, g.secretLister, g.kubeClient)
		bindMounts, err := volResolver.Resolve(ctx, pod, ic)
		if err != nil {
			g.EventRecorder.Eventf(pod, corev1.EventTypeWarning, "FailedMount",
				"Volume resolution failed for init container %s: %v", ic.Name, err)
			_ = g.ImageManager.Unmount(uid + "-" + ic.Name)
			compensate()
			return fmt.Errorf("volume resolution for init container %s: %w", ic.Name, err)
		}
		if g.clusterDNS != "" {
			if err := writeResolvConf(rootfs, g.clusterDNS); err != nil {
				g.Logger.Warn("Failed to write resolv.conf for init container", "container", ic.Name, "err", err)
			}
		}
		icMemLimit, icCPULimit := extractResourceLimits(ic)
		icEP, icCmd := g.ImageManager.ImageEntrypoint(ic.Image)
		cfg := perigeos.PodConfig{
			Name:             pod.Name,
			Namespace:        pod.Namespace,
			UID:              uid,
			ContainerName:    ic.Name,
			Container:        ic,
			PawnName:         g.Config.Name,
			RootFS:           rootfs,
			BindMounts:       bindMounts,
			NetNSPath:        netPath,
			HostNetwork:      pod.Spec.HostNetwork,
			HostPID:          pod.Spec.HostPID,
			Privileged:       isPrivileged(ic),
			Environment:      resolvedEnv,
			PodIP:            podIP,
			MemoryLimitBytes: icMemLimit,
			CPULimitMillis:   icCPULimit,
			ImageEntrypoint:  icEP,
			ImageCmd:         icCmd,
		}

		if err := g.Runtime.RunMachine(ctx, uid, cfg); err != nil {
			_ = g.ImageManager.Unmount(uid + "-" + ic.Name)
			compensate()
			return fmt.Errorf("start init container %s: %w", ic.Name, err)
		}

		state, err := g.Runtime.WaitForMachineExit(ctx, uid, ic.Name, initContainerTimeout)

		if err != nil {
			// Context cancel or timeout — stop the machine before cleanup.
			_ = g.Runtime.StopMachine(context.Background(), uid, ic.Name)
			_ = g.ImageManager.Unmount(uid + "-" + ic.Name)
			g.EventRecorder.Eventf(pod, corev1.EventTypeWarning, "FailedInit", "Init container %s: %v", ic.Name, err)
			compensate()
			return fmt.Errorf("init container %s: %w", ic.Name, err)
		}
		_ = g.ImageManager.Unmount(uid + "-" + ic.Name)
		if state == perigeos.StateFailed {
			g.EventRecorder.Eventf(pod, corev1.EventTypeWarning, "FailedInit", "Init container %s exited with error", ic.Name)
			compensate()
			return fmt.Errorf("init container %s failed", ic.Name)
		}

		g.Logger.Info("Init container completed", "pod", pod.Name, "container", ic.Name)
	}

	// Step 2+: pull and mount each app container, registering compensations as we go.
	type startResult struct {
		name string
		err  error
	}
	startResults := make(chan startResult, len(pod.Spec.Containers))

	for i := range pod.Spec.Containers {
		c := &pod.Spec.Containers[i]

		// Check pod-level cache first before pulling from registry.
		var layers []string
		var cached bool
		if entry, hit := pullCache[c.Image]; hit {
			// Cache hit: reuse layers from previous pull in this pod.
			layers = entry.layers
			cached = true
		} else {
			// Cache miss: pull normally and store result.
			g.EventRecorder.Eventf(pod, corev1.EventTypeNormal, "Pulling", "Pulling image %s for container %s", c.Image, c.Name)
			var pullCached bool
			var err error
			layers, pullCached, err = g.ImageManager.PullWithOptions(c.Image, string(c.ImagePullPolicy),
				image.PullOptions{
					Progress: pullProgressFunc(g, pod, c.Image, c.Name),
					Event:    podEventFn(g, pod),
				})
			if err != nil {
				g.EventRecorder.Eventf(pod, corev1.EventTypeWarning, "FailedPull", "Pull %s: %v", c.Name, err)
				compensate()
				return fmt.Errorf("pull container %s: %w", c.Name, err)
			}
			// Cache the successful pull result.
			pullCache[c.Image] = pullCacheEntry{layers: layers, cached: pullCached}
		}
		if cached {
			g.EventRecorder.Eventf(pod, corev1.EventTypeNormal, "Cached", "Image %s already present (pod cache) for container %s", c.Image, c.Name)
		} else {
			g.EventRecorder.Eventf(pod, corev1.EventTypeNormal, "Pulled", "Pulled image %s for container %s", c.Image, c.Name)
		}

		rootfs, err := g.ImageManager.Mount(uid+"-"+c.Name, layers)
		if err != nil {
			g.EventRecorder.Eventf(pod, corev1.EventTypeWarning, "FailedMount", "Mount %s: %v", c.Name, err)
			compensate()
			return fmt.Errorf("mount container %s: %w", c.Name, err)
		}
		g.EventRecorder.Eventf(pod, corev1.EventTypeNormal, "Created", "Created container %s", c.Name)
		cname := c.Name // capture for compensation closure
		compensations = append(compensations, func() {
			_ = g.ImageManager.Unmount(uid + "-" + cname)
			_ = g.Runtime.StopMachine(context.Background(), uid, cname)
		})

		resolvedEnv := g.Tidal.ResolveEnv(pod, c, podIP)
		volResolver := volume.NewResolver(g.Config.BaseDir, g.Config.Name, uid, g.cmLister, g.secretLister, g.kubeClient)
		bindMounts, err := volResolver.Resolve(ctx, pod, c)
		if err != nil {
			g.EventRecorder.Eventf(pod, corev1.EventTypeWarning, "FailedMount",
				"Volume resolution failed for container %s: %v", c.Name, err)
			compensate()
			return fmt.Errorf("volume resolution for container %s: %w", c.Name, err)
		}
		if g.clusterDNS != "" {
			if err := writeResolvConf(rootfs, g.clusterDNS); err != nil {
				g.Logger.Warn("Failed to write resolv.conf", "container", c.Name, "err", err)
			}
		}
		cMemLimit, cCPULimit := extractResourceLimits(c)
		cEP, cCmd := g.ImageManager.ImageEntrypoint(c.Image)
		cfg := perigeos.PodConfig{
			Name:                          pod.Name,
			Namespace:                     pod.Namespace,
			UID:                           uid,
			ContainerName:                 c.Name,
			Container:                     c,
			PawnName:                      g.Config.Name,
			RootFS:                        rootfs,
			BindMounts:                    bindMounts,
			NetNSPath:                     netPath,
			HostNetwork:                   pod.Spec.HostNetwork,
			HostPID:                       pod.Spec.HostPID,
			Privileged:                    isPrivileged(c),
			Environment:                   resolvedEnv,
			PodIP:                         podIP,
			MemoryLimitBytes:              cMemLimit,
			CPULimitMillis:                cCPULimit,
			ImageEntrypoint:               cEP,
			ImageCmd:                      cCmd,
			TerminationGracePeriodSeconds: podTerminationGracePeriod(pod),
		}

		go func(cfg perigeos.PodConfig, cn string) {
			if err := g.Runtime.RunMachine(ctx, uid, cfg); err != nil {
				startResults <- startResult{cn, err}
				return
			}
			if err := g.waitForContainer(ctx, uid, cn, machineStartTimeout); err != nil {
				startResults <- startResult{cn, err}
				return
			}
			startResults <- startResult{cn, nil}
		}(cfg, c.Name)
	}

	// Collect start results; run compensations if any container failed.
	var startErr error
	for range pod.Spec.Containers {
		r := <-startResults
		if r.err != nil && startErr == nil {
			startErr = fmt.Errorf("container %s: %w", r.name, r.err)
		}
	}

	if startErr != nil {
		compensate()
		g.EventRecorder.Eventf(pod, corev1.EventTypeWarning, "FailedStart", "Pod failed to start: %v", startErr)
		return startErr
	}

	// Promote pod from Pending to Running and record IP.
	g.store.PromoteRunning(uid, pod, podIP)

	// Index volume-mounted ConfigMaps/Secrets for live refresh.
	g.volumes.Track(uid, pod)

	g.Logger.Info("Pod started successfully", "pod", pod.Name, "ip", podIP,
		"containers", len(pod.Spec.Containers))
	g.EventRecorder.Eventf(pod, corev1.EventTypeNormal, "Started", "Started pod %s", pod.Name)

	// Push Running status to PodController immediately. We construct the
	// status directly rather than calling GetPodStatus, because the
	// BatchWatcher cache may contain stale/intermediate states from
	// systemd's unit startup sequence (brief inactive→active transition).
	{
		updated := pod.DeepCopy()
		updated.Status.Phase = corev1.PodRunning
		updated.Status.HostIP = resolveNodeIP(g.Config)
		updated.Status.PodIP = podIP
		now := metav1.NewTime(time.Now())
		updated.Status.StartTime = &now
		allReady := true
		for _, c := range pod.Spec.Containers {
			ready := g.isContainerReady(uid, c.Name)
			if !ready {
				allReady = false
			}
			updated.Status.ContainerStatuses = append(updated.Status.ContainerStatuses, corev1.ContainerStatus{
				Name:  c.Name,
				Image: c.Image,
				Ready: ready,
				State: corev1.ContainerState{
					Running: &corev1.ContainerStateRunning{StartedAt: now},
				},
			})
		}
		readyCondition := corev1.ConditionFalse
		if allReady {
			readyCondition = corev1.ConditionTrue
		}
		updated.Status.Conditions = []corev1.PodCondition{{
			Type:   corev1.PodReady,
			Status: readyCondition,
		}}
		g.notifyPodStatus(updated)
	}

	// Mark all containers as seen-running in the BatchWatcher so it knows
	// they actually started (even if the D-Bus "running" event arrives
	// after the unit already exited for fast-exit containers).
	if g.batchWatcher != nil {
		for _, c := range pod.Spec.Containers {
			g.batchWatcher.MarkRunning(uid, c.Name)
		}
	}

	// Don't Poke here — the BatchWatcher will detect the exit via D-Bus
	// events (SubState=failed for non-zero exits, immediate) or the next
	// ticker poll (2s, for clean exits). Poking immediately would race
	// with systemd's ExecMainStatus update, causing exit-1 containers
	// to be misclassified as Succeeded.

	return nil
}

// waitForContainer polls MachineStatus until the container is Running or timeout.
func (g *Gambit) waitForContainer(ctx context.Context, uid, containerName string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		state, err := g.Runtime.MachineStatus(ctx, uid, containerName)
		if err == nil {
			switch state {
			case perigeos.StateRunning:
				return nil
			case perigeos.StateExited, perigeos.StateFailed:
				// Container already ran and exited (fast-exit containers like
				// certgen finish before the first poll). This is success from
				// the perspective of "the machine started" — the BatchWatcher
				// will handle terminal phase transitions and exit codes.
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	return fmt.Errorf("container %s/%s did not become running within %s", uid, containerName, timeout)
}

// waitForMachine is kept for HydrateFromRuntime compatibility.
func (g *Gambit) waitForMachine(ctx context.Context, uid string, timeout time.Duration) error {
	// For single-container pods this is equivalent to waitForContainer.
	// We don't know the container name here, so this is a no-op placeholder
	// called only from HydrateFromRuntime which doesn't need it.
	return nil
}

// markPodFailed records a pod as Failed in the internal maps and pushes
// the terminal status to the PodController.
func (g *Gambit) markPodFailed(uid string, pod *corev1.Pod, err error) {
	g.EventRecorder.Eventf(pod, corev1.EventTypeWarning, "CreateFailed", "pod creation failed: %v", err)
	failedPod := g.store.MarkFailed(uid, pod, "CreateFailed", err.Error())
	g.notifyPodStatus(failedPod)
}

// initRestartState initializes CrashLoopBackOff tracking for a newly created pod.
// Called after successful pod creation so the batch watcher can manage restarts.
func (g *Gambit) initRestartState(pod *corev1.Pod) {
	g.store.InitRestartState(pod)
}

// isContainerReady returns whether a container should be reported as Ready.
// If no readiness probe is defined, defaults to true (set in initRestartState).
func (g *Gambit) isContainerReady(uid, containerName string) bool {
	return g.store.IsContainerReady(uid, containerName)
}

// restartContainer implements a single container restart with CrashLoopBackOff.
func (g *Gambit) restartContainer(ctx context.Context, uid string, pod *corev1.Pod, containerName string) {
	// Don't start new machines during graceful shutdown.
	if g.node.IsShuttingDown() {
		return
	}

	count, backoff := g.store.BumpBackoff(uid, containerName)
	if count == 0 {
		// No restart state found
		return
	}

	g.Logger.Info("Restarting container (CrashLoopBackOff)",
		"pod", pod.Name, "container", containerName,
		"restartCount", count, "backoff", backoff)
	g.EventRecorder.Eventf(pod, corev1.EventTypeWarning, "BackOff",
		"Back-off restarting container %s (count: %d)", containerName, count)

	select {
	case <-time.After(backoff):
	case <-ctx.Done():
		return
	}

	// Stop the old machine (may already be stopped).
	g.EventRecorder.Eventf(pod, corev1.EventTypeNormal, "Killing", "Restarting container %s", containerName)
	_ = g.Runtime.StopMachine(ctx, uid, containerName)

	// Find the container spec to rebuild the config.
	var container *corev1.Container
	for i := range pod.Spec.Containers {
		if pod.Spec.Containers[i].Name == containerName {
			container = &pod.Spec.Containers[i]
			break
		}
	}
	if container == nil {
		return
	}

	// Unmount any leftover overlay from the previous run before re-mounting.
	// Without this, overlays stack on each restart (each Mount adds a new one).
	_ = g.ImageManager.Unmount(uid + "-" + containerName)

	layers, _, err := g.ImageManager.PullWithOptions(container.Image, string(container.ImagePullPolicy),
		image.PullOptions{Event: podEventFn(g, pod)})
	if err != nil {
		g.Logger.Error("Restart: image pull failed", "container", containerName, "err", err)
		return
	}
	rootfs, err := g.ImageManager.Mount(uid+"-"+containerName, layers)
	if err != nil {
		g.Logger.Error("Restart: mount failed", "container", containerName, "err", err)
		return
	}

	podIP := g.store.PodIP(uid)

	var netPath string
	if pod.Spec.HostNetwork {
		netPath = "/proc/1/ns/net"
	} else {
		netPath = filepath.Join("/var/run/netns", uid)
	}

	resolvedEnv := g.Tidal.ResolveEnv(pod, container, podIP)
	volResolver := volume.NewResolver(g.Config.BaseDir, g.Config.Name, uid, g.cmLister, g.secretLister, g.kubeClient)
	bindMounts, err := volResolver.Resolve(ctx, pod, container)
	if err != nil {
		g.Logger.Error("Restart: volume resolution failed", "container", containerName, "err", err)
		_ = g.ImageManager.Unmount(uid + "-" + containerName)
		return
	}

	if g.clusterDNS != "" {
		if err := writeResolvConf(rootfs, g.clusterDNS); err != nil {
			g.Logger.Warn("Restart: failed to write resolv.conf", "container", containerName, "err", err)
		}
	}

	rMemLimit, rCPULimit := extractResourceLimits(container)
	rEP, rCmd := g.ImageManager.ImageEntrypoint(container.Image)
	cfg := perigeos.PodConfig{
		Name:                          pod.Name,
		Namespace:                     pod.Namespace,
		UID:                           uid,
		ContainerName:                 containerName,
		Container:                     container,
		PawnName:                      g.Config.Name,
		RootFS:                        rootfs,
		BindMounts:                    bindMounts,
		NetNSPath:                     netPath,
		HostNetwork:                   pod.Spec.HostNetwork,
		HostPID:                       pod.Spec.HostPID,
		Privileged:                    isPrivileged(container),
		Environment:                   resolvedEnv,
		PodIP:                         podIP,
		MemoryLimitBytes:              rMemLimit,
		CPULimitMillis:                rCPULimit,
		ImageEntrypoint:               rEP,
		ImageCmd:                      rCmd,
		TerminationGracePeriodSeconds: podTerminationGracePeriod(pod),
	}

	// Pre-flight: verify machined health before starting.
	if err := g.Runtime.CheckMachined(ctx); err != nil {
		g.Logger.Error("Restart: machined unhealthy, skipping", "container", containerName, "err", err)
		_ = g.ImageManager.Unmount(uid + "-" + containerName)
		return
	}

	if err := g.Runtime.RunMachine(ctx, uid, cfg); err != nil {
		g.Logger.Error("Restart: RunMachine failed", "container", containerName, "err", err)
		return
	}
	if err := g.waitForContainer(ctx, uid, containerName, machineStartTimeout); err != nil {
		g.Logger.Error("Restart: container did not start", "container", containerName, "err", err)
		return
	}

	g.store.MarkRestarted(uid, containerName)

	g.Logger.Info("Container restarted successfully", "pod", pod.Name, "container", containerName)

	// Push updated status immediately so restartCount is visible in k8s
	// without waiting for the next batch watcher cycle.
	restartedPod := g.store.GetPodCopy(uid)
	if restartedPod != nil {
		status := g.buildPodStatus(restartedPod, func(u, cn string) perigeos.MachineState {
			state, err := g.Runtime.MachineStatus(ctx, u, cn)
			if err != nil {
				return perigeos.StateUnknown
			}
			return state
		})
		updated := restartedPod.DeepCopy()
		status.DeepCopyInto(&updated.Status)
		g.notifyPodStatus(updated)
	}
}

func (g *Gambit) UpdatePod(_ context.Context, pod *corev1.Pod) error {
	g.Logger.Info("UpdatePod", "pawn", g.Config.Name, "pod", pod.Name)
	return nil
}

func (g *Gambit) DeletePod(ctx context.Context, pod *corev1.Pod) error {
	var caller string
	if _, file, line, ok := runtime.Caller(1); ok {
		caller = fmt.Sprintf("%s:%d", filepath.Base(file), line)
	}
	g.Logger.Info("DeletePod", "pawn", g.Config.Name, "namespace", pod.Namespace, "name", pod.Name, "caller", caller)
	uid := string(pod.UID)
	g.setKind(pod)

	// Mark pod as deleting so the batch watcher won't restart its containers.
	g.store.MarkDeleting(uid)

	// If a CreatePod saga is running, cancel it and wait for its compensations
	// to finish before proceeding. This closes the race where DeletePod arrives
	// while the CNI is still allocating an IP or a machine is starting.
	g.cancelInFlight(uid)

	// Enforce terminationGracePeriodSeconds. PreStop hooks + container stop
	// share this budget. If PreStop consumes part of it, the remaining time
	// is available for SIGTERM before systemd sends SIGKILL.
	gracePeriod := podTerminationGracePeriod(pod)
	if gracePeriod > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(gracePeriod)*time.Second)
		defer cancel()
	}

	// Run PreStop lifecycle hooks before stopping containers.
	g.runPreStopHooks(ctx, pod, uid)

	// Stop all app containers. systemd sends SIGTERM → waits TimeoutStopSec
	// (set from terminationGracePeriodSeconds at unit creation) → SIGKILL.
	for _, c := range pod.Spec.Containers {
		if err := g.Runtime.StopMachine(ctx, uid, c.Name); err != nil {
			g.Logger.Error("Failed to stop container", "container", c.Name, "err", err)
		}
		if err := g.ImageManager.Unmount(uid + "-" + c.Name); err != nil {
			g.Logger.Error("Failed to unmount container overlay", "container", c.Name, "err", err)
		}
	}
	// Stop any init containers that may still be running (edge case).
	for _, c := range pod.Spec.InitContainers {
		if err := g.Runtime.StopMachine(ctx, uid, c.Name); err != nil {
			g.Logger.Error("Failed to stop init container", "container", c.Name, "err", err)
		}
		if err := g.ImageManager.Unmount(uid + "-" + c.Name); err != nil {
			g.Logger.Error("Failed to unmount init container overlay", "container", c.Name, "err", err)
		}
	}

	// HostNetwork pods share the host netns — no CNI netns was created.
	if !pod.Spec.HostNetwork {
		if err := g.NetworkManager.Teardown(ctx, uid, pod.Namespace, pod.Name); err != nil {
			g.Logger.Error("Failed to teardown network", "err", err)
		}
	}

	// Clean up any host-side volume state (emptyDir, configMap projections).
	volResolver := volume.NewResolver(g.Config.BaseDir, g.Config.Name, uid, nil, nil, nil)
	if err := volResolver.Cleanup(); err != nil {
		g.Logger.Warn("Volume cleanup failed (non-fatal)", "uid", uid, "err", err)
	}

	// Clean up pod workspace directory.
	podDir := filepath.Join(g.Config.BaseDir, "pawns", g.Config.Name, "pods", uid)
	if err := os.RemoveAll(podDir); err != nil {
		g.Logger.Warn("Failed to remove pod directory", "uid", uid, "err", err)
	}

	g.volumes.Untrack(uid)
	g.store.Unregister(uid, pod.Namespace, pod.Name)

	if err := deletePodState(g.Config.BaseDir, g.Config.Name, uid); err != nil {
		g.Logger.Warn("Failed to delete pod state", "pod", pod.Name, "err", err)
	}

	return nil
}

// admitPod checks if the pod's resource requests fit within remaining node capacity.
// Returns an empty string if admitted, or a reason string if rejected.
func (g *Gambit) admitPod(pod *corev1.Pod) string {
	return g.store.AdmitPod(pod, g.Config.CPU, g.Config.Memory)
}

// computeAllocatable returns Capacity minus the sum of resource requests from
// all currently tracked pods. This gives the k8s scheduler visibility into
// real available resources so it can avoid overcommitting the node.
func (g *Gambit) computeAllocatable(capacity corev1.ResourceList) corev1.ResourceList {
	return g.store.ComputeAllocatable(capacity)
}

// pullProgressFunc returns a callback that emits Pulling events at 10% steps.
// podEventFn returns an image.PullEventFn that forwards layer pull events as
// pod events on the given pod.
func podEventFn(g *Gambit, pod *corev1.Pod) image.PullEventFn {
	return func(eventType, reason, message string) {
		g.EventRecorder.Eventf(pod, eventType, reason, "%s", message)
	}
}

func pullProgressFunc(g *Gambit, pod *corev1.Pod, imageName, containerName string) image.PullProgress {
	var lastPct int
	return func(done, total int) {
		if total == 0 {
			return
		}
		pct := done * 100 / total
		// Emit at every 10% boundary crossed, and always at 100%.
		step := pct / 10 * 10
		if step > lastPct || pct == 100 {
			g.EventRecorder.Eventf(pod, corev1.EventTypeNormal, "Pulling",
				"Pulling image %s: %d%% (%d/%d layers) for %s", imageName, pct, done, total, containerName)
			lastPct = step
		}
	}
}

// podTerminationGracePeriod returns the pod's termination grace period in seconds.
// Defaults to 30 (Kubernetes default) if not set.
func podTerminationGracePeriod(pod *corev1.Pod) int64 {
	if pod.Spec.TerminationGracePeriodSeconds != nil {
		return *pod.Spec.TerminationGracePeriodSeconds
	}
	return 30
}

// runPreStopHooks executes PreStop lifecycle hooks for all running containers.
// Hooks run within the parent context's deadline (shared with the stop budget).
// Errors are logged but do not prevent container shutdown.
func (g *Gambit) runPreStopHooks(ctx context.Context, pod *corev1.Pod, uid string) {
	for i := range pod.Spec.Containers {
		c := &pod.Spec.Containers[i]
		if c.Lifecycle == nil || c.Lifecycle.PreStop == nil {
			continue
		}

		hook := c.Lifecycle.PreStop
		g.Logger.Info("Running PreStop hook", "container", c.Name, "pod", pod.Name)

		switch {
		case hook.Exec != nil:
			if err := g.Runtime.RunInContainer(ctx, uid, c.Name, hook.Exec.Command, &noopAttachIO{}); err != nil {
				g.Logger.Warn("PreStop exec hook failed", "container", c.Name, "err", err)
				if g.EventRecorder != nil {
					g.EventRecorder.Eventf(pod, corev1.EventTypeWarning, "FailedPreStopHook",
						"PreStop exec hook failed for container %s: %v", c.Name, err)
				}
			}

		case hook.HTTPGet != nil:
			g.runHTTPPreStopHook(ctx, pod, c.Name, uid, hook.HTTPGet)
		}
	}
}

// runHTTPPreStopHook executes an HTTP GET PreStop hook against the container.
func (g *Gambit) runHTTPPreStopHook(ctx context.Context, pod *corev1.Pod, containerName, uid string, httpGet *corev1.HTTPGetAction) {
	podIP := g.store.PodIP(uid)

	host := httpGet.Host
	if host == "" {
		host = podIP
	}
	if host == "" {
		g.Logger.Warn("PreStop HTTP hook: no host/podIP available", "container", containerName)
		return
	}

	port := httpGet.Port.String()
	scheme := string(httpGet.Scheme)
	if scheme == "" {
		scheme = "http"
	}

	url := fmt.Sprintf("%s://%s:%s%s", strings.ToLower(scheme), host, port, httpGet.Path)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		g.Logger.Warn("PreStop HTTP hook: bad request", "container", containerName, "err", err)
		return
	}
	for _, h := range httpGet.HTTPHeaders {
		req.Header.Set(h.Name, h.Value)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		g.Logger.Warn("PreStop HTTP hook failed", "container", containerName, "url", url, "err", err)
		if g.EventRecorder != nil {
			g.EventRecorder.Eventf(pod, corev1.EventTypeWarning, "FailedPreStopHook",
				"PreStop HTTP hook failed for container %s: %v", containerName, err)
		}
		return
	}
	resp.Body.Close()
	g.Logger.Info("PreStop HTTP hook completed", "container", containerName, "status", resp.StatusCode)
}

// ─── Pod Queries ─────────────────────────────────────────────────────────────

func (g *Gambit) GetPod(_ context.Context, namespace, name string) (*corev1.Pod, error) {
	return g.store.GetPod(namespace, name)
}

func (g *Gambit) GetPodStatus(ctx context.Context, namespace, name string) (*corev1.PodStatus, error) {
	targetPod, err := g.store.GetPod(namespace, name)
	if err != nil {
		return nil, err
	}

	uid := string(targetPod.UID)

	// Pod is queued, waiting for a createSem slot — no machine exists yet.
	// Return Pending so VK doesn't interpret NotFound as a missing pod.
	phase := g.store.PodPhase(uid)
	if phase == corev1.PodPending {
		return &corev1.PodStatus{Phase: corev1.PodPending}, nil
	}

	// If the pod is in a terminal phase (set by BatchWatcher), return
	// the stored status directly. The systemd unit may already be cleaned
	// up (ResetUnit), so querying the stateCache would give stale results.
	if phase == corev1.PodSucceeded || phase == corev1.PodFailed {
		return &targetPod.Status, nil
	}

	// If the pod was marked as failed during creation (e.g. CNI error,
	// image pull failure), return the stored failure status directly.
	// No machine was started, so querying systemd would give misleading
	// "Completed" / "Unknown" states.
	if targetPod.Status.Phase == corev1.PodFailed {
		return &targetPod.Status, nil
	}

	// Use the BatchWatcher's cached stateMap if available, otherwise fall
	// back to per-container D-Bus queries.
	var stateLookup func(uid, containerName string) perigeos.MachineState
	if g.batchWatcher != nil {
		stateLookup = g.batchWatcher.ContainerState
	} else {
		stateLookup = func(uid, containerName string) perigeos.MachineState {
			state, err := g.Runtime.MachineStatus(ctx, uid, containerName)
			if err != nil {
				return perigeos.StateUnknown
			}
			return state
		}
	}

	return g.buildPodStatus(targetPod, stateLookup), nil
}

// buildPodStatus constructs a PodStatus from the pod spec and a state lookup
// function. Used by both GetPodStatus (on-demand) and the BatchWatcher
// coalescer (push on change). The stateLookup func returns the current
// container state given (uid, containerName).
func (g *Gambit) buildPodStatus(pod *corev1.Pod, stateLookup func(uid, containerName string) perigeos.MachineState) *corev1.PodStatus {
	uid := string(pod.UID)

	containerStatuses := make([]corev1.ContainerStatus, 0, len(pod.Spec.Containers))
	podPhase := corev1.PodRunning
	allReady := true

	podRestarts := g.store.RestartCounts(uid)

	policy := pod.Spec.RestartPolicy
	if policy == "" {
		policy = corev1.RestartPolicyAlways
	}

	for _, c := range pod.Spec.Containers {
		state := stateLookup(uid, c.Name)

		restartCount := podRestarts[c.Name]

		cs := corev1.ContainerStatus{
			Name:         c.Name,
			Image:        c.Image,
			Ready:        false,
			RestartCount: restartCount,
		}

		switch state {
		case perigeos.StateRunning:
			cs.Ready = g.isContainerReady(uid, c.Name)
			cs.State = corev1.ContainerState{
				Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(g.node.StartTime())},
			}
		case perigeos.StateCreating, perigeos.StateUnknown:
			podPhase = corev1.PodPending
			cs.State = corev1.ContainerState{
				Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"},
			}
		case perigeos.StateFailed:
			if policy == corev1.RestartPolicyAlways || policy == corev1.RestartPolicyOnFailure {
				podPhase = corev1.PodRunning
				cs.State = corev1.ContainerState{
					Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"},
				}
			} else {
				podPhase = corev1.PodFailed
				cs.State = corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, Reason: "Error"},
				}
			}
		case perigeos.StateExited:
			if policy == corev1.RestartPolicyAlways {
				podPhase = corev1.PodRunning
				cs.State = corev1.ContainerState{
					Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"},
				}
			} else {
				if podPhase == corev1.PodRunning {
					podPhase = corev1.PodSucceeded
				}
				cs.State = corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{ExitCode: 0, Reason: "Completed"},
				}
			}
		}

		if !cs.Ready {
			allReady = false
		}
		containerStatuses = append(containerStatuses, cs)
	}

	readyCondition := corev1.ConditionFalse
	if allReady {
		readyCondition = corev1.ConditionTrue
	}

	ip := g.store.PodIP(uid)

	return &corev1.PodStatus{
		Phase:     podPhase,
		HostIP:    resolveNodeIP(g.Config),
		PodIP:     ip,
		StartTime: pod.Status.StartTime,
		Conditions: []corev1.PodCondition{{
			Type:   corev1.PodReady,
			Status: readyCondition,
		}},
		ContainerStatuses: containerStatuses,
	}
}

func (g *Gambit) GetPods(_ context.Context) ([]*corev1.Pod, error) {
	g.Logger.Debug("GetPods", "pawn", g.Config.Name)
	return g.store.GetPods(), nil
}

// PodSnapshot holds pod info captured under a single lock acquisition.
type PodSnapshot struct {
	Name       string
	Namespace  string
	UID        string
	IP         string
	Phase      corev1.PodPhase
	Containers int
}

// SnapshotPods returns a lightweight snapshot of all pods in a single lock.
// Designed for the control API to avoid per-pod lock acquisitions.
// Returns nil if the lock cannot be acquired immediately (heavy write churn).
func (g *Gambit) SnapshotPods() []PodSnapshot {
	return g.store.SnapshotPods()
}

// ─── Logs & Exec ─────────────────────────────────────────────────────────────

var _ api.ContainerLogsHandlerFunc = (*Gambit)(nil).GetContainerLogs

func (g *Gambit) GetContainerLogs(
	ctx context.Context,
	namespace, podName, containerName string,
	opts api.ContainerLogOpts,
) (io.ReadCloser, error) {
	g.Logger.Info("GetContainerLogs", "pawn", g.Config.Name, "namespace", namespace, "pod", podName, "container", containerName)

	// Try to find the pod by namespace and name.
	uid, err := g.store.FindPodUID(namespace, podName)
	if err != nil {
		// Fall back to completed pods — journal entries survive after DeletePod
		// removes the pod from the store.
		uid = g.store.CompletedPodUID(namespace, podName)
		if uid == "" {
			return nil, fmt.Errorf("pod %s/%s not found", namespace, podName)
		}
	}

	return g.Runtime.GetLogStream(ctx, uid, containerName, opts)
}

func (g *Gambit) AttachToContainer(
	ctx context.Context,
	namespace, podName, containerName string,
	attach api.AttachIO,
) error {
	uid, err := g.findPodUID(namespace, podName)
	if err != nil {
		return err
	}
	return g.Runtime.AttachToContainer(ctx, uid, containerName, attach)
}

func (g *Gambit) RunInContainer(
	ctx context.Context,
	namespace, podName, containerName string,
	cmd []string,
	attach api.AttachIO,
) error {
	uid, err := g.findPodUID(namespace, podName)
	if err != nil {
		return err
	}

	return g.Runtime.RunInContainer(ctx, uid, containerName, cmd, attach)
}

func (g *Gambit) findPodUID(namespace, podName string) (string, error) {
	return g.store.FindPodUID(namespace, podName)
}

// ─── Node Conditions ─────────────────────────────────────────────────────────

// setKind restores Pod TypeMeta stripped by client-go informers.
// Required for the EventRecorder to construct object references correctly.
func (g *Gambit) setKind(pod *corev1.Pod) {
	pod.Kind = "Pod"
	pod.APIVersion = "v1"
}

// GetStatsSummary returns kubelet-compatible resource usage for this pawn node.
// Called by the /stats/summary HTTP endpoint consumed by metrics-server.
func (g *Gambit) GetStatsSummary(ctx context.Context) (*pawnstats.Summary, error) {
	return g.node.GetStatsSummary(ctx)
}

// writeResolvConf writes a cluster-aware /etc/resolv.conf into the container
// rootfs before systemd-nspawn starts, so pods can resolve Kubernetes service
// names via the cluster DNS (coredns).
// /etc/resolv.conf in many images is a symlink to systemd-resolved's stub;
// we remove the symlink first so we write a real file, not the host's stub.
func writeResolvConf(rootfs, dnsIP string) error {
	content := "nameserver " + dnsIP + "\n" +
		"search default.svc.cluster.local svc.cluster.local cluster.local\n" +
		"options ndots:5\n"
	path := filepath.Join(rootfs, "etc", "resolv.conf")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	// Remove symlink (or existing file) so WriteFile creates a real file.
	_ = os.Remove(path)
	return os.WriteFile(path, []byte(content), 0o644)
}
