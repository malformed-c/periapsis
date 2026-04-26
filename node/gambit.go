package node

// TODO God object

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/malformed-c/periapsis/internal/config"
	"github.com/malformed-c/periapsis/internal/downward"
	"github.com/malformed-c/periapsis/internal/foci"
	"github.com/malformed-c/periapsis/internal/image"
	"github.com/malformed-c/periapsis/internal/network"
	"github.com/malformed-c/periapsis/internal/pki"
	perigeos "github.com/malformed-c/periapsis/internal/runtime"
	pawnstats "github.com/malformed-c/periapsis/internal/stats"
	"github.com/malformed-c/periapsis/internal/types"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	listersv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
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
	hostNodeName string // Real host node name (from os.Hostname()), used for CSI volumes

	volumes *VolumeTracker // live ConfigMap/Secret volume refresh

	store        *PodStore     // pod spec cache + admission + hydration
	batchWatcher *BatchWatcher // thin D-Bus signal adapter

	// sendFact emits a Fact to the Syzygy event loop.
	// Wired from cmd/perigeos/main.go via SetSendFact.
	sendFact func(fact types.Fact) bool

	// podNotify is the callback registered by NotifyPods. When set, Gambit
	// pushes pod status changes to the PodController instead of relying on
	// the syncProviderWrapper's 5-second polling loop.
	podNotify func(*corev1.Pod)

	// stateReader provides access to Syzygy's pod state machines.
	stateReader StateReader

	// regMu protects regHandles.
	regMu      sync.Mutex
	regHandles map[string]*creationHandle

	// syncRequester is the forward reconciler callback. When set, Gambit
	// (via BatchWatcher or Reconciler) can request the PodController re-sync
	// a pod from the K8s side. The sync handler's podsEffectivelyEqual
	// check prevents infinite loops.
	syncRequester func(namespace, name string)

	node *PawnNode
}

// Compile-time check that Gambit satisfies PodTracker.
var _ PodTracker = (*Gambit)(nil)

// creationHandle tracks an in-flight pod creation or a running watcher.
// The cancel func signals the goroutine to stop; done is closed when it exits.
type creationHandle struct {
	cancel context.CancelFunc
	done   chan struct{}
	// registered is closed by RegisterPodCB once store.RegisterPending
	// completes. The saga goroutine waits on this before proceeding so
	// it never touches PodStore before the pod is registered.
	registered chan struct{}
}

// containerRestartState tracks CrashLoopBackOff state for a single container.
// Deprecated: This state is now tracked by foci.ContainerState. The remaining
// fields are kept for backward compatibility with status.go and gambit.go
// persistence, which will be migrated to read from foci in a future phase.
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

// StateReader provides read access to foci.PodState from Syzygy.
type StateReader interface {
	PodState(uid string) (foci.PodState, bool)
}

func (g *Gambit) SetStateReader(reader StateReader) {
	g.stateReader = reader
}

func (g *Gambit) RegisterHandle(uid string, handle *creationHandle) {
	g.regMu.Lock()
	defer g.regMu.Unlock()
	g.regHandles[uid] = handle
}

func NewGambit(deps GambitDeps) *Gambit {
	nodeIP := resolveNodeIP(deps.Config)
	// Get the real host node name for CSI volume mounting.
	hostNodeName, _ := os.Hostname()
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
		hostNodeName:   hostNodeName,
		regHandles:     make(map[string]*creationHandle),
	}
	if deps.APIServerHost != "" {
		g.Tidal.SetAPIServer(deps.APIServerHost, deps.APIServerPort)
	}
	if deps.CMInformer != nil {
		deps.CMInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
			UpdateFunc: func(_, obj any) {
				cm := obj.(*corev1.ConfigMap)
				g.volumes.RefreshConfigMap(cm)
			},
		})
	}
	if deps.SecretInformer != nil {
		deps.SecretInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
			UpdateFunc: func(_, obj any) {
				s := obj.(*corev1.Secret)
				g.volumes.RefreshSecret(s)
			},
		})
	}
	return g
}

func (g *Gambit) StartBatchWatcher(sendFact func(types.Fact) bool) {
	g.sendFact = sendFact
	g.batchWatcher = StartBatchWatcher(BatchWatcherDeps{
		Runtime:  g.Runtime,
		Logger:   g.Logger,
		PawnName: g.Config.Name,
		Store:    g.store,
		ParseUnitName: func(unitName string) (string, string) {
			return ParseUnitName(g.Config.Name, unitName)
		},
		SendFact: sendFact,
	})
	g.Logger.Info("BatchWatcher started and assigned to Gambit")
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
// Used by the Reconciler for ghost pods - pods in gambit's map that have
// no systemd unit and are no longer desired by Kubernetes.
func (g *Gambit) EvictGhost(uid string) {
	g.store.EvictGhost(uid)
	// Clean up persisted state so the ghost doesn't rehydrate on restart.
	deletePodState(g.Config.BaseDir, g.Config.Name, uid)
}

func (g *Gambit) GetPodCopy(uid string) *corev1.Pod {
	return g.store.GetPodCopy(uid)
}

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
// This is the single funnel for all status changes - persist here rather
// than scattering writePodState across every call site.
func (g *Gambit) notifyPodStatus(pod *corev1.Pod) {
	// Create a snapshot of the pod right now
	podCopy := pod.DeepCopy()

	uid := string(pod.UID)
	var caller string
	if _, file, line, ok := runtime.Caller(1); ok {
		caller = fmt.Sprintf("%s:%d", filepath.Base(file), line)
	}
	g.Logger.Debug("notifyPodStatus",
		"uid", uid,
		"pod", pod.Name,
		"phase", pod.Status.Phase,
		"ip", pod.Status.PodIP,
		"reason", pod.Status.Reason,
		"containers", summarizeContainerStatuses(pod.Status.ContainerStatuses),
		"caller", caller,
	)
	if g.podNotify != nil {
		g.podNotify(podCopy)
	}

	// Persist state to disk.
	g.PersistPodStateByUID(uid)
}

// NotifyPodStatus is the exported wrapper for notifyPodStatus.
func (g *Gambit) NotifyPodStatus(pod *corev1.Pod) {
	g.notifyPodStatus(pod)
}

// PersistPodState writes the current in-memory state for a pod to disk
// without pushing a status update to Kubernetes.
// Deprecated: Use PersistPodStateByUID.
func (g *Gambit) PersistPodState(pod *corev1.Pod) {
	g.PersistPodStateByUID(string(pod.UID))
}

func (g *Gambit) RegisterPodCB(e types.RegisterPod) {
	g.regMu.Lock()
	handle := g.regHandles[e.UID]
	delete(g.regHandles, e.UID)
	g.regMu.Unlock()

	if e.InFlight {
		g.store.RegisterPending(e.UID, e.Pod, handle)
	}
	// else: hydrated pods — AlreadyRunning already registered them.

	// Unblock the saga goroutine. Must happen after RegisterPending so
	// the pod is visible in PodStore before the saga touches it.
	if handle != nil {
		close(handle.registered)
	}
}

// PersistPodStateByUID persists pod state to disk. Uses foci.PodState
// as the source of truth when available. This is the callback wired into Syzygy.
func (g *Gambit) PersistPodStateByUID(uid string) {
	pod := g.store.GetPodCopy(uid)
	if pod == nil {
		return
	}

	var state foci.PodState
	if g.stateReader != nil {
		if s, ok := g.stateReader.PodState(uid); ok {
			state = s
		} else {
			// foci state not ready yet (e.g. during initial creation before
			// PodRegisterFact is processed). Write pod-only state so at least
			// the spec survives a restart — foci state will be rebuilt from
			// D-Bus signals and anti-entropy on next startup.
			g.Logger.Debug("PersistPodStateByUID: foci state not found yet, persisting spec only", "uid", uid)
		}
	} else {
		// stateReader not wired — should not happen in production but
		// persist what we have rather than silently dropping.
		g.Logger.Warn("PersistPodStateByUID: stateReader not set, persisting spec only", "uid", uid)
	}

	if err := writePodState(g.Config.BaseDir, g.Config.Name, &PersistedPodState{
		Pod:   pod,
		State: state,
	}); err != nil {
		g.Logger.Warn("Failed to persist pod state", "pod", pod.Name, "err", err)
	}
}

// RestartContainerCB is the exported wrapper for restartContainer callback.
// Called by Horizon when a RestartContainer effect is dispatched.
func (g *Gambit) RestartContainerCB(ctx context.Context, uid string, pod *corev1.Pod, containerName string, count int32, backoff time.Duration) {
	g.restartContainer(ctx, uid, pod, containerName, count, backoff)
}

// --- Pod Lifecycle ---

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
func writeResolvConf(rootfs, dnsIP, namespace string) error {
	content := "nameserver " + dnsIP + "\n" +
		"search " + namespace + ".svc.cluster.local svc.cluster.local cluster.local\n" +
		"options ndots:5\n"
	path := filepath.Join(rootfs, "etc", "resolv.conf")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	// Remove symlink (or existing file) so WriteFile creates a real file.
	_ = os.Remove(path)
	return os.WriteFile(path, []byte(content), 0o644)
}
