package node

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/malformed-c/periapsis/internal/config"
	"github.com/malformed-c/periapsis/internal/downward"
	"github.com/malformed-c/periapsis/internal/image"
	"github.com/malformed-c/periapsis/internal/network"
	"github.com/malformed-c/periapsis/internal/pki"
	pruntime "github.com/malformed-c/periapsis/internal/runtime"
	pawstats "github.com/malformed-c/periapsis/internal/stats"
	"github.com/malformed-c/periapsis/internal/version"
	"github.com/malformed-c/periapsis/internal/volume"
	"github.com/malformed-c/periapsis/errdefs"
	"github.com/malformed-c/periapsis/node/api"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
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
	diskPressureThresholdPercent  = 85.0
	inodePressureThresholdPercent = 95.0
	memoryPressureThreshold       = 95.0
	pidPressureThreshold          = 98.0
	nodeStatusUpdateInterval      = 30 * time.Second

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

// volumeMount tracks a single mounted ConfigMap/Secret for a running pod.
type volumeMount struct {
	podUID  string
	hostDir string         // host path where files were written
	vol     *corev1.Volume // original volume spec (for Items filtering)
}

// Gambit is the periapsis provider. It implements the PodProvider interface
// and orchestrates image pulling, overlayfs mounting, network setup,
// and systemd-nspawn machine management for each pod.
type Gambit struct {
	Config         config.PawnConfig
	ImageManager   *image.ImageManager
	NetworkManager network.NetworkManager
	Runtime        pruntime.Runtime
	Logger         *slog.Logger
	Tidal          *downward.Tidal
	EventRecorder  record.EventRecorder

	// Listers for env population and volume resolution.
	// Set via SetListers after construction.
	cmLister     listersv1.ConfigMapLister
	secretLister listersv1.SecretLister
	svcLister    listersv1.ServiceLister
	kubeClient   kubernetes.Interface
	clusterDNS   string // ClusterIP of kube-dns, written to container resolv.conf

	// volRefs indexes mounted ConfigMaps/Secrets by "kind:namespace/name"
	// for O(1) lookup when an informer fires an update event.
	volRefs    map[string][]volumeMount
	// volRefsByPod maps podUID → list of volRef keys for cleanup on delete.
	volRefsByPod map[string][]string

	mu           sync.RWMutex
	pods         map[string]*corev1.Pod                       // UID → Pod
	podIPs       map[string]string                            // UID → IP
	podPhases    map[string]corev1.PodPhase                   // UID → last known phase
	hydratedUIDs map[string]bool                              // UIDs from HydrateFromRuntime, cleared after purge
	inFlight     map[string]*podSaga                          // UID → active creation saga (nil = not creating)
	batchWatcher *BatchWatcher                                // single watcher per pawn (replaces per-pod watchers)
	restarts     map[string]map[string]*containerRestartState // UID → container → restart state
	probeStates    map[string]map[string]*ContainerProbeState // UID → container → probe state
	probeRunner    *ProbeRunner
	completedPods  map[string]string                         // "namespace/name" → UID for recently-deleted pods (log fallback)
	createSem    chan struct{}                                 // limits concurrent pod creation sagas

	// podNotify is the callback registered by NotifyPods. When set, Gambit
	// pushes pod status changes to the PodController instead of relying on
	// the syncProviderWrapper's 5-second polling loop.
	podNotify func(*corev1.Pod)

	// syncRequester is the forward reconciler callback. When set, Gambit
	// (via BatchWatcher or Reconciler) can request the PodController re-sync
	// a pod from the K8s side. The sync handler's podsEffectivelyEqual
	// check prevents infinite loops.
	syncRequester func(namespace, name string)

	shuttingDown atomic.Bool
	shutdownCh   chan struct{} // closed when Shutdown() is called

	startTime time.Time
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

func NewGambit(
	cfg config.PawnConfig,
	im *image.ImageManager,
	nm network.NetworkManager,
	rt pruntime.Runtime,
	logger *slog.Logger,
	eRec record.EventRecorder,
) *Gambit {
	nodeIP := resolveNodeIP(cfg)

	return &Gambit{
		Config:         cfg,
		ImageManager:   im,
		NetworkManager: nm,
		Runtime:        rt,
		Logger:         logger,
		Tidal:          downward.NewTidal(cfg.Name, nodeIP),
		EventRecorder:  eRec,

		pods:         make(map[string]*corev1.Pod),
		podIPs:       make(map[string]string),
		podPhases:    make(map[string]corev1.PodPhase),
		hydratedUIDs: make(map[string]bool),
		inFlight:     make(map[string]*podSaga),
		restarts:     make(map[string]map[string]*containerRestartState),
		probeStates:   make(map[string]map[string]*ContainerProbeState),
		probeRunner:   NewProbeRunner(rt, logger, eRec),
		completedPods: make(map[string]string),
		volRefs:      make(map[string][]volumeMount),
		volRefsByPod: make(map[string][]string),
		createSem:    make(chan struct{}, createConcurrency(cfg)),
		shutdownCh:   make(chan struct{}),
		startTime:    time.Now(),
	}
}

// SetListers provides the listers needed for env population and volume resolution.
// Called after construction once informers are synced.
func (g *Gambit) SetListers(cmLister listersv1.ConfigMapLister, secretLister listersv1.SecretLister, svcLister listersv1.ServiceLister) {
	g.cmLister = cmLister
	g.secretLister = secretLister
	g.svcLister = svcLister
}

// SetInformers registers event handlers on ConfigMap and Secret informers
// to refresh volume-mounted files in running pods when the underlying
// objects are updated. Uses in-place file writes so inotify fires.
func (g *Gambit) SetInformers(cmInformer, secretInformer cache.SharedIndexInformer) {
	cmInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(_, obj interface{}) {
			cm := obj.(*corev1.ConfigMap)
			g.refreshConfigMapVolumes(cm)
		},
	})
	secretInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(_, obj interface{}) {
			s := obj.(*corev1.Secret)
			g.refreshSecretVolumes(s)
		},
	})
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

// refreshConfigMapVolumes rewrites ConfigMap volume files using the updated object directly.
func (g *Gambit) refreshConfigMapVolumes(cm *corev1.ConfigMap) {
	key := "configmap:" + cm.Namespace + "/" + cm.Name
	g.mu.RLock()
	mounts := make([]volumeMount, len(g.volRefs[key]))
	copy(mounts, g.volRefs[key])
	g.mu.RUnlock()

	if len(mounts) == 0 {
		return
	}

	g.Logger.Info("Refreshing volume", "kind", "configmap", "name", cm.Name, "pods", len(mounts))
	for _, m := range mounts {
		err := volume.RefreshConfigMapDirect(cm, m.vol, m.hostDir)
		if err != nil {
			g.Logger.Warn("Failed to refresh volume", "kind", "configmap", "name", cm.Name, "pod", m.podUID, "err", err)
		}
	}
}

// refreshSecretVolumes rewrites Secret volume files using the updated object directly.
func (g *Gambit) refreshSecretVolumes(secret *corev1.Secret) {
	key := "secret:" + secret.Namespace + "/" + secret.Name
	g.mu.RLock()
	mounts := make([]volumeMount, len(g.volRefs[key]))
	copy(mounts, g.volRefs[key])
	g.mu.RUnlock()

	if len(mounts) == 0 {
		return
	}

	g.Logger.Info("Refreshing volume", "kind", "secret", "name", secret.Name, "pods", len(mounts))
	for _, m := range mounts {
		err := volume.RefreshSecretDirect(secret, m.vol, m.hostDir)
		if err != nil {
			g.Logger.Warn("Failed to refresh volume", "kind", "secret", "name", secret.Name, "pod", m.podUID, "err", err)
		}
	}
}

// trackVolumeRefs scans a pod's volumes for ConfigMap and Secret types,
// populating the volRefs reverse index for live refresh. Must be called
// with g.mu held.
func (g *Gambit) trackVolumeRefs(uid string, pod *corev1.Pod) {
	var keys []string
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
		hostDir := filepath.Join(g.Config.BaseDir, "pawns", g.Config.Name, "pods", uid, "volumes", kind, vol.Name)
		g.volRefs[key] = append(g.volRefs[key], volumeMount{
			podUID:  uid,
			hostDir: hostDir,
			vol:     vol,
		})
		keys = append(keys, key)
	}
	if len(keys) > 0 {
		g.volRefsByPod[uid] = keys
	}
}

// untrackVolumeRefs removes all volume reference entries for a pod.
// Must be called with g.mu held.
func (g *Gambit) untrackVolumeRefs(uid string) {
	keys := g.volRefsByPod[uid]
	for _, key := range keys {
		mounts := g.volRefs[key]
		filtered := mounts[:0]
		for _, m := range mounts {
			if m.podUID != uid {
				filtered = append(filtered, m)
			}
		}
		if len(filtered) == 0 {
			delete(g.volRefs, key)
		} else {
			g.volRefs[key] = filtered
		}
	}
	delete(g.volRefsByPod, uid)
}

func (g *Gambit) SetKubeClient(client kubernetes.Interface) {
	g.kubeClient = client
}

func (g *Gambit) SetClusterDNS(ip string) {
	g.clusterDNS = ip
}

// SetAPIServer configures the Kubernetes API server address injected into pods
// as KUBERNETES_SERVICE_HOST / KUBERNETES_SERVICE_PORT.
func (g *Gambit) SetAPIServer(host, port string) {
	g.Tidal.SetAPIServer(host, port)
}

// IsInFlight reports whether a pod creation goroutine is currently active for uid.
// Used by the Reconciler to avoid killing machines mid-creation.
func (g *Gambit) IsInFlight(uid string) bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.inFlight[uid] != nil
}

// cancelInFlight cancels the in-flight saga for uid (if any) and waits for
// its compensations to finish. Returns immediately if nothing is in flight.
// Called by DeletePod so the delete path never races with a create in progress.
func (g *Gambit) cancelInFlight(uid string) {
	g.mu.RLock()
	saga := g.inFlight[uid]
	g.mu.RUnlock()
	if saga == nil {
		return
	}
	saga.cancel()
	<-saga.done
}

// StopBatchWatcher stops the batch watcher. Called during graceful shutdown.
func (g *Gambit) StopBatchWatcher() {
	if g.batchWatcher != nil {
		g.batchWatcher.Stop()
	}
}

// PodCount returns the number of pods known to this pawn.
func (g *Gambit) PodCount() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.pods)
}

// PodIP returns the IP allocated to a pod, or "" if unknown.
func (g *Gambit) PodIP(uid string) string {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.podIPs[uid]
}

// PodPhase returns the last known phase for a pod, or "" if unknown.
func (g *Gambit) PodPhase(uid string) corev1.PodPhase {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.podPhases[uid]
}

// NodeIP returns the IP this pawn advertises to the apiserver.
func (g *Gambit) NodeIP() string {
	return resolveNodeIP(g.Config)
}

// PodUIDs returns a snapshot of all pod UIDs known to this gambit.
func (g *Gambit) PodUIDs() map[string]string {
	g.mu.RLock()
	defer g.mu.RUnlock()
	uids := make(map[string]string, len(g.pods))
	for uid, pod := range g.pods {
		uids[uid] = pod.Namespace + "/" + pod.Name
	}
	return uids
}

// HasPod reports whether Gambit's in-memory state knows about a pod.
func (g *Gambit) HasPod(uid string) bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	_, ok := g.pods[uid]
	return ok
}

// EvictGhost removes a pod from Gambit's in-memory state without running
// the full DeletePod teardown (no machine stop, no CNI DEL).
// Used by the Reconciler for ghost pods — pods in gambit's map that have
// no systemd unit and are no longer desired by Kubernetes.
func (g *Gambit) EvictGhost(uid string) {
	g.mu.Lock()
	delete(g.pods, uid)
	delete(g.podIPs, uid)
	delete(g.podPhases, uid)
	g.mu.Unlock()
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

	g.mu.Lock()
	for _, state := range states {
		uid := string(state.Pod.UID)
		g.pods[uid] = state.Pod
		if state.PodIP != "" {
			g.podIPs[uid] = state.PodIP
		}
		g.podPhases[uid] = corev1.PodRunning
		g.hydratedUIDs[uid] = true
	}
	diskUIDs := make(map[string]struct{}, len(states))
	for _, s := range states {
		diskUIDs[string(s.Pod.UID)] = struct{}{}
	}
	g.mu.Unlock()

	// Initialize probe states for disk-restored pods (must happen outside the lock).
	for _, state := range states {
		g.initRestartState(state.Pod)
		// initRestartState resets restarts — re-apply the persisted counts.
		if len(state.Restarts) > 0 {
			uid := string(state.Pod.UID)
			g.mu.Lock()
			for cname, count := range state.Restarts {
				if rs, ok := g.restarts[uid][cname]; ok {
					rs.count = count
				}
			}
			g.mu.Unlock()
		}
	}

	// --- Fallback: pick up any running units not on disk (pre-state-persistence pods) ---
	machines, err := g.Runtime.ListManagedMachines(ctx)
	if err != nil {
		g.Logger.Warn("HydrateFromRuntime: ListManagedMachines failed (non-fatal)", "err", err)
		machines = nil
	}

	g.mu.Lock()
	for _, m := range machines {
		if m.UID == "" {
			continue
		}
		if _, onDisk := diskUIDs[m.UID]; onDisk {
			continue // already restored from disk
		}
		if m.PodIP != "" {
			g.podIPs[m.UID] = m.PodIP
		}
		if m.Name != "" && m.Namespace != "" {
			g.pods[m.UID] = &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      m.Name,
					Namespace: m.Namespace,
					UID:       types.UID(m.UID),
				},
			}
			g.podPhases[m.UID] = corev1.PodRunning
			g.hydratedUIDs[m.UID] = true
		}
	}
	g.mu.Unlock()

	g.Logger.Info("Hydrated in-memory state", "from_disk", len(states), "from_systemd", len(machines))

	// Sweep disk for orphan overlay dirs that have no corresponding systemd
	// unit. This handles the case where a pod was mid-deletion (systemd unit
	// stopped) when perigeos restarted, leaving the overlay dir behind.
	podsDir := filepath.Join(g.Config.BaseDir, "pawns", g.Config.Name, "pods")
	entries, err := os.ReadDir(podsDir)
	if err == nil {
		hydratedSet := make(map[string]struct{}, len(g.pods))
		for uid := range g.pods {
			hydratedSet[uid] = struct{}{}
		}
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			name := e.Name()
			uid := name
			if len(name) > 36 && name[36] == '-' {
				uid = name[:36]
			}
			if _, ok := hydratedSet[uid]; !ok {
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
	g.mu.Lock()
	g.Logger.Info("PurgeStaleHydrated: checking hydrated pods",
		"pawn", g.Config.Name, "hydrated", len(g.hydratedUIDs), "total_pods", len(g.pods))
	stale := make([]string, 0)
	for uid := range g.hydratedUIDs {
		// If CreatePod was called for this UID, it's confirmed — skip.
		// (CreatePod replaces the hydration stub with the full pod object,
		// which has Spec.Containers populated.)
		if pod, ok := g.pods[uid]; ok && len(pod.Spec.Containers) > 0 {
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
	// Clear tracking map — hydration window is over.
	g.hydratedUIDs = nil
	g.mu.Unlock()

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
		g.mu.Lock()
		delete(g.pods, uid)
		delete(g.podIPs, uid)
		delete(g.podPhases, uid)
		g.mu.Unlock()
	}
	if len(stale) > 0 {
		g.Logger.Info("Purged stale hydrated pods", "count", len(stale))
	}
}

// ─── Node ────────────────────────────────────────────────────────────────────

// systemdVersion returns the systemd version (e.g. "systemd://259.3-1-arch").
func systemdVersion() string {
	out, err := exec.Command("systemctl", "--version").Output()
	if err != nil {
		return "systemd://"
	}
	// First line: "systemd 259 (259.3-1-arch)" — extract the parenthesized version.
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

// kernelVersion returns the running kernel version via uname.
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

// osImage returns PRETTY_NAME from /etc/os-release.
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

func (g *Gambit) BuildNode() *corev1.Node {
	hostName, _ := os.Hostname()
	pawnName := g.Config.Name
	outBoundIP := net.ParseIP(resolveNodeIP(g.Config))
	swapCapacity := int64(1)

	var stat syscall.Statfs_t
	workspacePath := g.ImageManager.GetLayerCachePath()
	if err := syscall.Statfs(workspacePath, &stat); err != nil {
		g.Logger.Error("Could not stat workspace filesystem", "path", workspacePath, "err", err)
	}

	totalBytes := stat.Blocks * uint64(stat.Bsize)
	ephemeralStorage := *resource.NewQuantity(int64(totalBytes), resource.BinarySI)

	// All nodes carry the host topology label so the constellation agent
	// can discover all nodes sharing a physical host via label selector.
	// Primary nodes get the primary role; regular pawns get the pawn role.
	labels := make(map[string]string, len(g.Config.Labels)+7)
	maps.Copy(labels, g.Config.Labels)
	labels["periapsis.io/host"] = hostName
	labels["kubernetes.io/hostname"] = pawnName
	labels["kubernetes.io/os"] = "linux"
	labels["kubernetes.io/arch"] = runtime.GOARCH
	labels["beta.kubernetes.io/os"] = "linux"
	labels["beta.kubernetes.io/arch"] = runtime.GOARCH
	if g.Config.IsPrimary {
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
			Taints:     g.Config.Taints,
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
				corev1.ResourceCPU:              g.Config.CPU,
				corev1.ResourceMemory:           g.Config.Memory,
				corev1.ResourcePods:             resource.MustParse("110"),
				corev1.ResourceStorage:          ephemeralStorage,
				corev1.ResourceEphemeralStorage: ephemeralStorage,
			},
			Conditions: g.nodeConditions(),
			Addresses: []corev1.NodeAddress{
				{Type: corev1.NodeHostName, Address: pawnName},
				{Type: corev1.NodeInternalIP, Address: outBoundIP.String()},
			},
			DaemonEndpoints: corev1.NodeDaemonEndpoints{
				KubeletEndpoint: corev1.DaemonEndpoint{
					Port: int32(g.Config.Port),
				},
			},
		},
	}
	node.Status.Allocatable = node.Status.Capacity
	return node
}

// Shutdown signals this pawn to begin graceful shutdown. Ping will return an
// error, causing VK to stop renewing the lease. The node becomes NotReady
// after ~40s, and the scheduler stops placing pods on it.
func (g *Gambit) Shutdown() {
	if g.shuttingDown.CompareAndSwap(false, true) {
		close(g.shutdownCh)
		g.Logger.Info("Shutdown initiated", "pawn", g.Config.Name)
	}
}

// DrainPods actively stops all running pods on this pawn. Unlike the passive
// drain (waiting for apiserver DeletePod calls), this directly stops containers
// and cleans up resources. Call after Shutdown().
func (g *Gambit) DrainPods(ctx context.Context) {
	g.mu.RLock()
	pods := make([]*corev1.Pod, 0, len(g.pods))
	for _, pod := range g.pods {
		pods = append(pods, pod)
	}
	g.mu.RUnlock()

	for _, pod := range pods {
		g.Logger.Info("Draining pod", "pawn", g.Config.Name, "pod", pod.Name)
		if err := g.DeletePod(ctx, pod); err != nil {
			g.Logger.Error("Failed to drain pod", "pod", pod.Name, "err", err)
		}
	}
}

// NotifyPods registers a callback for asynchronous pod status updates.
// When set, the BatchWatcher pushes status changes directly to the
// PodController instead of relying on the 5-second polling loop.
func (g *Gambit) NotifyPods(_ context.Context, cb func(*corev1.Pod)) {
	g.podNotify = cb
}

func (g *Gambit) Ping(context.Context) error {
	if g.shuttingDown.Load() {
		return fmt.Errorf("pawn %s is shutting down", g.Config.Name)
	}
	return nil
}

func (g *Gambit) NotifyNodeStatus(ctx context.Context, cb func(*corev1.Node)) {
	g.Logger.Info("Starting pawn status notifier", "pawn", g.Config.Name)
	go func() {
		g.Logger.Info("Sending initial pawn registration", "pawn", g.Config.Name)
		cb(g.BuildNode())

		ticker := time.NewTicker(nodeStatusUpdateInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				g.Logger.Info("Stopping pawn status notifier")
				return
			case <-g.shutdownCh:
				g.Logger.Info("Shutdown signal received, marking node NotReady+Unschedulable", "pawn", g.Config.Name)
				node := g.BuildNode()
				node.Spec.Unschedulable = true
				for i := range node.Status.Conditions {
					if node.Status.Conditions[i].Type == corev1.NodeReady {
						node.Status.Conditions[i].Status = corev1.ConditionFalse
						node.Status.Conditions[i].Reason = "Shutdown"
						node.Status.Conditions[i].Message = "Perigeos shutting down"
					}
				}
				cb(node)
				return
			case <-ticker.C:
				g.Logger.Info("Updating node status", "pawn", g.Config.Name)
				cb(g.BuildNode())
			}
		}
	}()
}

// notifyPodStatus pushes an updated pod to the PodController if a callback
// is registered. Safe to call when podNotify is nil (no-op).
func (g *Gambit) notifyPodStatus(pod *corev1.Pod) {
	if g.podNotify != nil {
		g.podNotify(pod)
	}
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

	g.mu.Lock()
	if g.inFlight[uid] != nil {
		g.mu.Unlock()
		g.Logger.Info("CreatePod: already in-flight, skipping", "pod", pod.Name)
		return nil
	}
	// After a restart HydrateFromRuntime re-populates g.pods from running
	// systemd units. When the informer reconnects it calls CreatePod for
	// every pod it knows about — skip pods that are already running so we
	// don't tear down and recreate a healthy machine.
	if existing, exists := g.pods[uid]; exists {
		// If the existing entry is a hydration stub (no Spec.Containers),
		// replace it with the full pod object from the informer so that
		// GetPodStatus, GetContainerLogs etc. have the real spec.
		if len(existing.Spec.Containers) == 0 {
			g.pods[uid] = pod
			delete(g.hydratedUIDs, uid) // k8s confirmed this pod; show it in snapshots
			g.mu.Unlock()
			// Also initialize probe state — it was never set because
			// createPodSync was skipped for already-running pods.
			g.initRestartState(pod)
		} else {
			delete(g.hydratedUIDs, uid)
			g.mu.Unlock()
		}
		g.Logger.Info("CreatePod: already running (hydrated from runtime), skipping", "pod", pod.Name)
		return nil
	}
	sagaCtx, cancel := context.WithCancel(context.Background())
	saga := &podSaga{cancel: cancel, done: make(chan struct{})}
	g.inFlight[uid] = saga
	// Register immediately as Pending so GetPodStatus never returns NotFound
	// while the pod is queued waiting for a createSem slot. Without this,
	// VK interprets NotFound as the pod not existing and may issue DeletePod,
	// cancelling the saga and causing a create/cancel loop.
	g.pods[uid] = pod
	g.podPhases[uid] = corev1.PodPending
	g.mu.Unlock()

	go func() {
		defer close(saga.done)
		defer func() {
			g.mu.Lock()
			delete(g.inFlight, uid)
			g.mu.Unlock()
		}()
		defer cancel()

		// Wait for a creation slot. If the saga is cancelled (DeletePod
		// arrived while we were queued) bail out without starting work.
		select {
		case g.createSem <- struct{}{}:
		case <-sagaCtx.Done():
			return
		}
		defer func() { <-g.createSem }()

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
		compensate()
		return fmt.Errorf("env population: %w", err)
	}

	// Run init containers sequentially; each must exit 0 before the next starts.
	for i := range pod.Spec.InitContainers {
		ic := &pod.Spec.InitContainers[i]
		g.Logger.Info("Starting init container", "pod", pod.Name, "container", ic.Name)

		g.EventRecorder.Eventf(pod, corev1.EventTypeNormal, "Pulling", "Pulling image %s for init container %s", ic.Image, ic.Name)
		layers, err := g.ImageManager.Pull(ic.Image, string(ic.ImagePullPolicy))
		if err != nil {
			g.EventRecorder.Eventf(pod, corev1.EventTypeWarning, "FailedPull", "Pull %s: %v", ic.Name, err)
			compensate()
			return fmt.Errorf("pull init container %s: %w", ic.Name, err)
		}
		g.EventRecorder.Eventf(pod, corev1.EventTypeNormal, "Pulled", "Pulled image %s for init container %s", ic.Image, ic.Name)

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
		cfg := pruntime.PodConfig{
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
		if state == pruntime.StateFailed {
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

		g.EventRecorder.Eventf(pod, corev1.EventTypeNormal, "Pulling", "Pulling image %s for container %s", c.Image, c.Name)
		layers, err := g.ImageManager.Pull(c.Image, string(c.ImagePullPolicy))
		if err != nil {
			g.EventRecorder.Eventf(pod, corev1.EventTypeWarning, "FailedPull", "Pull %s: %v", c.Name, err)
			compensate()
			return fmt.Errorf("pull container %s: %w", c.Name, err)
		}
		g.EventRecorder.Eventf(pod, corev1.EventTypeNormal, "Pulled", "Pulled image %s for container %s", c.Image, c.Name)

		rootfs, err := g.ImageManager.Mount(uid+"-"+c.Name, layers)
		if err != nil {
			g.EventRecorder.Eventf(pod, corev1.EventTypeWarning, "FailedMount", "Mount %s: %v", c.Name, err)
			compensate()
			return fmt.Errorf("mount container %s: %w", c.Name, err)
		}
		g.EventRecorder.Eventf(pod, corev1.EventTypeNormal, "Created", "Created container %s", c.Name)
		cname := c.Name // capture for closure
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
		cfg := pruntime.PodConfig{
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

		go func(cfg pruntime.PodConfig, cname string) {
			if err := g.Runtime.RunMachine(ctx, uid, cfg); err != nil {
				startResults <- startResult{cname, err}
				return
			}
			if err := g.waitForContainer(ctx, uid, cname, machineStartTimeout); err != nil {
				startResults <- startResult{cname, err}
				return
			}
			startResults <- startResult{cname, nil}
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

	g.mu.Lock()
	g.pods[uid] = pod
	g.podIPs[uid] = podIP
	g.podPhases[uid] = corev1.PodRunning
	// Index volume-mounted ConfigMaps/Secrets for live refresh.
	g.trackVolumeRefs(uid, pod)
	g.mu.Unlock()

	g.Logger.Info("Pod started successfully", "pod", pod.Name, "ip", podIP,
		"containers", len(pod.Spec.Containers))
	g.EventRecorder.Eventf(pod, corev1.EventTypeNormal, "Started", "Started pod %s", pod.Name)

	// Push Running status to PodController immediately instead of waiting
	// for the next poll cycle.
	if status, err := g.GetPodStatus(context.Background(), pod.Namespace, pod.Name); err == nil {
		updated := pod.DeepCopy()
		status.DeepCopyInto(&updated.Status)
		g.notifyPodStatus(updated)
	}

	if err := writePodState(g.Config.BaseDir, g.Config.Name, &PersistedPodState{
		Pod:   pod,
		PodIP: podIP,
		Phase: corev1.PodRunning,
	}); err != nil {
		g.Logger.Warn("Failed to persist pod state", "pod", pod.Name, "err", err)
	}

	return nil
}

// waitForContainer polls MachineStatus until the container is Running or timeout.
func (g *Gambit) waitForContainer(ctx context.Context, uid, containerName string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		state, err := g.Runtime.MachineStatus(ctx, uid, containerName)
		if err == nil && state == pruntime.StateRunning {
			return nil
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
	failedPod := pod.DeepCopy()
	failedPod.Status.Phase = corev1.PodFailed
	failedPod.Status.Reason = "CreateFailed"
	failedPod.Status.Message = err.Error()
	g.mu.Lock()
	g.pods[uid] = failedPod
	g.podPhases[uid] = corev1.PodFailed
	g.mu.Unlock()
	g.notifyPodStatus(failedPod)
}

// initRestartState initializes CrashLoopBackOff tracking for a newly created pod.
// Called after successful pod creation so the batch watcher can manage restarts.
func (g *Gambit) initRestartState(pod *corev1.Pod) {
	uid := string(pod.UID)
	g.mu.Lock()
	rs := make(map[string]*containerRestartState, len(pod.Spec.Containers))
	ps := make(map[string]*ContainerProbeState, len(pod.Spec.Containers))
	for _, c := range pod.Spec.Containers {
		rs[c.Name] = &containerRestartState{
			backoff:     restartBackoffInit,
			lastStarted: time.Now(),
		}
		ps[c.Name] = &ContainerProbeState{
			Ready:         c.ReadinessProbe == nil, // ready by default only if no readiness probe
			LastProbeTime: make(map[string]time.Time),
		}
	}
	g.restarts[uid] = rs
	g.probeStates[uid] = ps
	g.mu.Unlock()
}

// isContainerReady returns whether a container should be reported as Ready.
// If no readiness probe is defined, defaults to true (set in initRestartState).
func (g *Gambit) isContainerReady(uid, containerName string) bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if ps, ok := g.probeStates[uid][containerName]; ok {
		return ps.Ready
	}
	return true // no probe state → default ready
}

// restartContainer implements a single container restart with CrashLoopBackOff.
func (g *Gambit) restartContainer(ctx context.Context, uid string, pod *corev1.Pod, containerName string) {
	// Don't start new machines during graceful shutdown.
	if g.shuttingDown.Load() {
		return
	}
	g.mu.Lock()
	rs, ok := g.restarts[uid][containerName]
	if !ok {
		g.mu.Unlock()
		return
	}
	rs.count++
	backoff := rs.backoff
	rs.backoff *= 2
	if rs.backoff > restartBackoffMax {
		rs.backoff = restartBackoffMax
	}
	g.mu.Unlock()

	// Persist updated restart count.
	g.mu.RLock()
	currentPod := g.pods[uid]
	currentIP := g.podIPs[uid]
	allRestarts := g.restarts[uid]
	g.mu.RUnlock()
	if currentPod != nil {
		counts := make(map[string]int32, len(allRestarts))
		for c, r := range allRestarts {
			counts[c] = r.count
		}
		_ = writePodState(g.Config.BaseDir, g.Config.Name, &PersistedPodState{
			Pod:      currentPod,
			PodIP:    currentIP,
			Phase:    corev1.PodRunning,
			Restarts: counts,
		})
	}

	g.Logger.Info("Restarting container (CrashLoopBackOff)",
		"pod", pod.Name, "container", containerName,
		"restartCount", rs.count, "backoff", backoff)
	g.EventRecorder.Eventf(pod, corev1.EventTypeWarning, "BackOff",
		"Back-off restarting container %s (count: %d)", containerName, rs.count)

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

	layers, err := g.ImageManager.Pull(container.Image, string(container.ImagePullPolicy))
	if err != nil {
		g.Logger.Error("Restart: image pull failed", "container", containerName, "err", err)
		return
	}
	rootfs, err := g.ImageManager.Mount(uid+"-"+containerName, layers)
	if err != nil {
		g.Logger.Error("Restart: mount failed", "container", containerName, "err", err)
		return
	}

	g.mu.RLock()
	podIP := g.podIPs[uid]
	g.mu.RUnlock()

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
	cfg := pruntime.PodConfig{
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

	g.mu.Lock()
	if rs, ok := g.restarts[uid][containerName]; ok {
		rs.lastStarted = time.Now()
	}
	g.mu.Unlock()

	g.Logger.Info("Container restarted successfully", "pod", pod.Name, "container", containerName)
}

func (g *Gambit) UpdatePod(_ context.Context, pod *corev1.Pod) error {
	g.Logger.Info("UpdatePod", "pawn", g.Config.Name, "pod", pod.Name)
	return nil
}

func (g *Gambit) DeletePod(ctx context.Context, pod *corev1.Pod) error {
	g.Logger.Info("DeletePod", "pawn", g.Config.Name, "namespace", pod.Namespace, "name", pod.Name)
	uid := string(pod.UID)
	g.setKind(pod)

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

	g.mu.Lock()
	g.untrackVolumeRefs(uid)
	// Retain UID mapping for completed pods so GetContainerLogs can still
	// retrieve journal entries after the pod is removed from g.pods.
	key := pod.Namespace + "/" + pod.Name
	g.completedPods[key] = uid
	delete(g.pods, uid)
	delete(g.podIPs, uid)
	delete(g.podPhases, uid)
	delete(g.restarts, uid)
	delete(g.probeStates, uid)
	g.mu.Unlock()

	return nil
}

// admitPod checks if the pod's resource requests fit within remaining node capacity.
// Returns an empty string if admitted, or a reason string if rejected.
func (g *Gambit) admitPod(pod *corev1.Pod) string {
	// Sum resource requests for the incoming pod.
	var podCPU, podMem int64
	for _, c := range pod.Spec.Containers {
		if req, ok := c.Resources.Requests[corev1.ResourceCPU]; ok {
			podCPU += req.MilliValue()
		}
		if req, ok := c.Resources.Requests[corev1.ResourceMemory]; ok {
			podMem += req.Value()
		}
	}
	// No requests specified — always admit (best-effort QoS).
	if podCPU == 0 && podMem == 0 {
		return ""
	}

	// Sum requests from all currently running pods.
	g.mu.RLock()
	var usedCPU, usedMem int64
	for _, p := range g.pods {
		for _, c := range p.Spec.Containers {
			if req, ok := c.Resources.Requests[corev1.ResourceCPU]; ok {
				usedCPU += req.MilliValue()
			}
			if req, ok := c.Resources.Requests[corev1.ResourceMemory]; ok {
				usedMem += req.Value()
			}
		}
	}
	g.mu.RUnlock()

	// Check against node capacity.
	nodeCPU := g.Config.CPU.MilliValue()
	nodeMem := g.Config.Memory.Value()

	if nodeCPU > 0 && usedCPU+podCPU > nodeCPU {
		return fmt.Sprintf("Insufficient cpu: requested %dm, used %dm, capacity %dm",
			podCPU, usedCPU, nodeCPU)
	}
	if nodeMem > 0 && usedMem+podMem > nodeMem {
		return fmt.Sprintf("Insufficient memory: requested %d, used %d, capacity %d",
			podMem, usedMem, nodeMem)
	}
	return ""
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
	g.mu.RLock()
	podIP := g.podIPs[uid]
	g.mu.RUnlock()

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
	g.mu.RLock()
	defer g.mu.RUnlock()

	for _, pod := range g.pods {
		if pod.Namespace == namespace && pod.Name == name {
			return pod, nil
		}
	}
	return nil, errdefs.NotFoundf("pod %s/%s not found", namespace, name)
}

func (g *Gambit) GetPodStatus(ctx context.Context, namespace, name string) (*corev1.PodStatus, error) {
	g.mu.RLock()
	var targetPod *corev1.Pod
	for _, pod := range g.pods {
		if pod.Namespace == namespace && pod.Name == name {
			targetPod = pod
			break
		}
	}
	g.mu.RUnlock()

	if targetPod == nil {
		return nil, errdefs.NotFoundf("pod %s/%s not found", namespace, name)
	}

	uid := string(targetPod.UID)

	// Pod is queued, waiting for a createSem slot — no machine exists yet.
	// Return Pending so VK doesn't interpret NotFound as a missing pod.
	g.mu.RLock()
	phase := g.podPhases[uid]
	g.mu.RUnlock()
	if phase == corev1.PodPending {
		return &corev1.PodStatus{Phase: corev1.PodPending}, nil
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
	var stateLookup func(uid, containerName string) pruntime.MachineState
	if g.batchWatcher != nil {
		stateLookup = g.batchWatcher.ContainerState
	} else {
		stateLookup = func(uid, containerName string) pruntime.MachineState {
			state, err := g.Runtime.MachineStatus(ctx, uid, containerName)
			if err != nil {
				return pruntime.StateUnknown
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
func (g *Gambit) buildPodStatus(pod *corev1.Pod, stateLookup func(uid, containerName string) pruntime.MachineState) *corev1.PodStatus {
	uid := string(pod.UID)

	containerStatuses := make([]corev1.ContainerStatus, 0, len(pod.Spec.Containers))
	podPhase := corev1.PodRunning
	allReady := true

	g.mu.RLock()
	podRestarts := g.restarts[uid]
	g.mu.RUnlock()

	policy := pod.Spec.RestartPolicy
	if policy == "" {
		policy = corev1.RestartPolicyAlways
	}

	for _, c := range pod.Spec.Containers {
		state := stateLookup(uid, c.Name)

		var restartCount int32
		if rs, ok := podRestarts[c.Name]; ok {
			restartCount = rs.count
		}

		cs := corev1.ContainerStatus{
			Name:         c.Name,
			Image:        c.Image,
			Ready:        false,
			RestartCount: restartCount,
		}

		switch state {
		case pruntime.StateRunning:
			cs.Ready = g.isContainerReady(uid, c.Name)
			cs.State = corev1.ContainerState{
				Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(g.startTime)},
			}
		case pruntime.StateCreating, pruntime.StateUnknown:
			podPhase = corev1.PodPending
			cs.State = corev1.ContainerState{
				Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"},
			}
		case pruntime.StateFailed:
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
		case pruntime.StateExited:
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

	g.mu.RLock()
	ip := g.podIPs[uid]
	g.mu.RUnlock()

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

	g.mu.RLock()
	defer g.mu.RUnlock()

	list := make([]*corev1.Pod, 0, len(g.pods))
	for _, pod := range g.pods {
		list = append(list, pod)
	}
	return list, nil
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
	if !g.mu.TryRLock() {
		return nil
	}
	defer g.mu.RUnlock()

	snaps := make([]PodSnapshot, 0, len(g.pods))
	for uid, pod := range g.pods {
		// Skip hydrated-only pods that k8s hasn't confirmed yet.
		if g.hydratedUIDs[uid] {
			continue
		}
		snaps = append(snaps, PodSnapshot{
			Name:       pod.Name,
			Namespace:  pod.Namespace,
			UID:        uid,
			IP:         g.podIPs[uid],
			Phase:      g.podPhases[uid],
			Containers: len(pod.Spec.Containers),
		})
	}
	return snaps
}

// ─── Logs & Exec ─────────────────────────────────────────────────────────────

var _ api.ContainerLogsHandlerFunc = (*Gambit)(nil).GetContainerLogs

func (g *Gambit) GetContainerLogs(
	ctx context.Context,
	namespace, podName, containerName string,
	opts api.ContainerLogOpts,
) (io.ReadCloser, error) {
	g.Logger.Info("GetContainerLogs", "pawn", g.Config.Name, "namespace", namespace, "pod", podName, "container", containerName)

	g.mu.RLock()
	var uid string
	for _, pod := range g.pods {
		if pod.Namespace == namespace && pod.Name == podName {
			uid = string(pod.UID)
			break
		}
	}
	// Fall back to completed pods — journal entries survive after DeletePod
	// removes the pod from g.pods.
	if uid == "" {
		uid = g.completedPods[namespace+"/"+podName]
	}
	g.mu.RUnlock()

	if uid == "" {
		return nil, fmt.Errorf("pod %s/%s not found", namespace, podName)
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
	g.mu.RLock()
	defer g.mu.RUnlock()

	for _, pod := range g.pods {
		if pod.Namespace == namespace && pod.Name == podName {
			return string(pod.UID), nil
		}
	}
	return "", fmt.Errorf("pod %s/%s not found", namespace, podName)
}

// ─── Node Conditions ─────────────────────────────────────────────────────────

func (g *Gambit) nodeConditions() []corev1.NodeCondition {
	now := metav1.Now()
	return []corev1.NodeCondition{
		{
			Type:               corev1.NodeReady,
			Status:             corev1.ConditionTrue,
			LastHeartbeatTime:  now,
			LastTransitionTime: now,
			Reason:             "PawnReady",
			Message:            "pawn is ready",
		},
		g.getMemoryPressureCondition(now),
		g.getDiskPressureCondition(now, g.ImageManager.GetLayerCachePath()),
		g.getPIDPressureCondition(now),
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

func (g *Gambit) getMemoryPressureCondition(now metav1.Time) corev1.NodeCondition {
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
		cond.Message = "pawn has sufficient memory available"
	}
	return cond
}

func (g *Gambit) getDiskPressureCondition(now metav1.Time, path string) corev1.NodeCondition {
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
	cond.Message = "pawn has no disk pressure"
	return cond
}

func (g *Gambit) getPIDPressureCondition(now metav1.Time) corev1.NodeCondition {
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

// setKind restores Pod TypeMeta stripped by client-go informers.
// Required for the EventRecorder to construct object references correctly.
func (g *Gambit) setKind(pod *corev1.Pod) {
	pod.Kind = "Pod"
	pod.APIVersion = "v1"
}

// GetStatsSummary returns kubelet-compatible resource usage for this pawn node.
// Called by the /stats/summary HTTP endpoint consumed by metrics-server.
func (g *Gambit) GetStatsSummary(_ context.Context) (*pawstats.Summary, error) {
	now := metav1.Now()
	pawnName := g.Config.Name

	// Node-level stats from the pawn's cgroup slice.
	nodeStats := pawstats.NodeStats{
		NodeName:  pawnName,
		StartTime: now,
	}
	if cpuNs, err := pawstats.ReadSliceCPU(pawnName); err == nil {
		nodeStats.CPU = &pawstats.CPUStats{
			Time:                 now,
			UsageCoreNanoSeconds: &cpuNs,
		}
	}
	if usage, ws, err := pawstats.ReadSliceMemory(pawnName); err == nil {
		nodeStats.Memory = &pawstats.MemoryStats{
			Time:            now,
			UsageBytes:      &usage,
			WorkingSetBytes: &ws,
		}
	}

	g.mu.RLock()
	pods := make([]*corev1.Pod, 0, len(g.pods))
	for _, p := range g.pods {
		pods = append(pods, p)
	}
	g.mu.RUnlock()

	podStats := make([]pawstats.PodStats, 0, len(pods))
	for _, pod := range pods {
		ps := pawstats.PodStats{
			PodRef: pawstats.PodReference{
				Name:      pod.Name,
				Namespace: pod.Namespace,
				UID:       string(pod.UID),
			},
			StartTime: pod.CreationTimestamp,
		}

		var podCPUNs, podMemUsage, podMemWS uint64
		for _, c := range pod.Spec.Containers {
			cs := pawstats.ContainerStats{
				Name:      c.Name,
				StartTime: now,
			}
			if cpuNs, err := pawstats.ReadContainerCPU(pawnName, string(pod.UID), c.Name); err == nil {
				cs.CPU = &pawstats.CPUStats{
					Time:                 now,
					UsageCoreNanoSeconds: &cpuNs,
				}
				podCPUNs += cpuNs
			}
			if usage, ws, err := pawstats.ReadContainerMemory(pawnName, string(pod.UID), c.Name); err == nil {
				cs.Memory = &pawstats.MemoryStats{
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
			ps.CPU = &pawstats.CPUStats{Time: now, UsageCoreNanoSeconds: &podCPUNs}
		}
		if podMemUsage > 0 {
			ps.Memory = &pawstats.MemoryStats{Time: now, UsageBytes: &podMemUsage, WorkingSetBytes: &podMemWS}
		}
		podStats = append(podStats, ps)
	}

	return &pawstats.Summary{
		Node: nodeStats,
		Pods: podStats,
	}, nil
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
