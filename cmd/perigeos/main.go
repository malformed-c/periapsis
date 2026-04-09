package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"

	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/coreos/go-systemd/v22/daemon"
	"github.com/lmittmann/tint"
	"github.com/malformed-c/periapsis/errdefs"
	"github.com/malformed-c/periapsis/internal/config"
	"github.com/malformed-c/periapsis/internal/control"
	"github.com/malformed-c/periapsis/internal/image"
	"github.com/malformed-c/periapsis/internal/join"
	"github.com/malformed-c/periapsis/internal/network"
	"github.com/malformed-c/periapsis/internal/pki"
	"github.com/malformed-c/periapsis/internal/plugin"
	perigeos "github.com/malformed-c/periapsis/internal/runtime"
	"github.com/malformed-c/periapsis/internal/runtime/systemd"
	"github.com/malformed-c/periapsis/internal/server"
	"github.com/malformed-c/periapsis/internal/vklogger"
	vklog "github.com/malformed-c/periapsis/log"
	"github.com/malformed-c/periapsis/node"
	"golang.org/x/time/rate"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	v1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

// perigeosRetryFunc is a custom retry policy for the pod sync queue.
// NotFound errors are permanent (pod deleted from apiserver).
// Transient errors (image pull, CNI timeout) use linear backoff up to 30s.
func perigeosRetryFunc(_ context.Context, key string, timesTried int, _ time.Time, err error) (*time.Duration, error) {
	if errdefs.IsNotFound(err) {
		return nil, fmt.Errorf("not retrying %q: %w", key, err)
	}
	if timesTried < 60 {
		delay := time.Duration(min(timesTried*5, 30)) * time.Second
		return &delay, nil
	}
	return nil, fmt.Errorf("maximum retries (%d) reached for %q: %w", timesTried, key, err)
}

func main() {
	logger := slog.New(tint.NewHandler(os.Stdout, &tint.Options{
		Level:      slog.LevelDebug,
		TimeFormat: "15:04:05",
	}))
	slog.SetDefault(logger)
	klog.SetSlogLogger(logger.WithGroup("klog"))
	vklog.L = vklogger.New(logger.WithGroup("vk"))
	klog.InitFlags(nil)

	// Subcommand dispatch — must happen before flag.Parse().
	if len(os.Args) > 1 && os.Args[1] == "join" {
		runJoin(logger)
		return
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	vkLogger := vklogger.New(logger.WithGroup("vk"))
	vklog.L = vkLogger
	ctx = vklog.WithLogger(ctx, vkLogger)
	defer cancel()

	// Force-exit if goroutines don't drain within 60s of receiving signal
	// (45s drain window + 15s for actual shutdown work).
	go func() {
		<-ctx.Done()
		time.AfterFunc(60*time.Second, func() {
			logger.Error("Force exit: goroutines did not stop in time")
			os.Exit(1)
		})
	}()

	perigeosConfigPath := flag.String("perigeosconfig", "", "Path to the perigeos TOML config (required)")
	kubeConfigPath := flag.String("kubeconfig", "", "Path to kubeconfig")
	baseDirFlag := flag.String("base-dir", "/var/lib/apsis/perigeos",
		"Base directory for state. Use a local path (e.g. ./var/lib/apsis/perigeos) for dev.")
	execStrategyFlag := flag.String("exec-strategy", "nsenter",
		"RunInContainer strategy: nsenter (default) or machinectl")
	controlSocketFlag := flag.String("control-socket", control.DefaultSocketPath,
		"Path to the control Unix socket for apsis CLI")
	controlTCPFlag := flag.String("control-tcp", "",
		"TCP address for remote Varlink access with mTLS, e.g. :7443 (requires --server-ca)")
	flag.Parse()

	if *perigeosConfigPath == "" {
		logger.Error("--perigeosconfig is required")
		os.Exit(1)
	}

	// Ensure the pawn's own IP is in NO_PROXY so outbound calls to the
	// Kubernetes API server are not routed through any ambient HTTP proxy.
	// The same proxy misconfiguration causes kube-apiserver to fail when it
	// tries to reach the pawn's log/exec endpoint — that must be fixed on the
	// control-plane side (add pawn subnet to NO_PROXY for the apiserver unit).
	addNoProxy(pki.GetOutboundIP().String())

	// Load Perigeos config first so we know the pawn count before configuring
	// the Kubernetes client. QPS/Burst must scale with the number of virtual
	// nodes: each NodeController and PodController issues concurrent API calls,
	// and the default 5 QPS / 10 Burst causes heavy throttling at >5 pawns.
	rawCfg, err := config.Load(*perigeosConfigPath)
	if err != nil {
		logger.Error("Config load failed", "err", err)
		os.Exit(1)
	}

	perigeoCfg, err := rawCfg.Process(*baseDirFlag)
	if err != nil {
		logger.Error("Config process failed", "err", err)
		os.Exit(1)
	}

	pawnCount := len(perigeoCfg.Pawns)
	if pawnCount < 1 {
		pawnCount = 1
	}

	kubeConfig, err := clientcmd.BuildConfigFromFlags("", *kubeConfigPath)
	if err != nil {
		logger.Error("Failed to load kubeconfig", "err", err)
		os.Exit(1)
	}
	// 10 QPS and 20 Burst per pawn; the shared client handles all virtual nodes.
	kubeConfig.QPS = float32(pawnCount) * 10
	kubeConfig.Burst = pawnCount * 20
	logger.Info("Kubernetes client configured", "pawns", pawnCount,
		"qps", kubeConfig.QPS, "burst", kubeConfig.Burst)

	kubeClient, err := kubernetes.NewForConfig(kubeConfig)
	if err != nil {
		logger.Error("Failed to create kubernetes client", "err", err)
		os.Exit(1)
	}

	dynClient, err := dynamic.NewForConfig(kubeConfig)
	if err != nil {
		logger.Error("Failed to create dynamic client", "err", err)
		os.Exit(1)
	}

	serverVersion, err := kubeClient.Discovery().ServerVersion()
	if err != nil {
		klog.Fatalf("Failed to connect to Kubernetes API server: %v", err)
	}

	klog.Infof("Connected to Kubernetes API server %s", serverVersion)

	var execStrategy perigeos.ExecStrategy
	switch *execStrategyFlag {
	case "machinectl":
		execStrategy = perigeos.ExecMachinectl
		logger.Info("Using machinectl exec strategy")
	default:
		execStrategy = perigeos.ExecNsenter
		logger.Info("Using nsenter exec strategy")
	}

	globalInformerFactory := informers.NewSharedInformerFactory(kubeClient, 40*time.Second)
	cmInformer := globalInformerFactory.Core().V1().ConfigMaps()
	secretInformer := globalInformerFactory.Core().V1().Secrets()
	svcInformer := globalInformerFactory.Core().V1().Services()
	// Force registration of the underlying shared informers before Start().
	// Without this, the informer backing store is created lazily on first
	// Lister()/Informer() call — which happens inside pawn goroutines,
	// after Start() has already been called. Informers registered after
	// Start() are never started, leading to empty caches.
	cmInformer.Informer()
	secretInformer.Informer()
	svcInformer.Informer()
	globalInformerFactory.Start(ctx.Done())

	logger.Info("Waiting for global informer caches to sync...")
	for typeRef, synced := range globalInformerFactory.WaitForCacheSync(ctx.Done()) {
		if !synced {
			logger.Error("Failed to sync global cache", "type", typeRef)

			os.Exit(1)
		}
	}
	logger.Info("Global caches synced")

	// --- Auto-detect cluster DNS ---
	var clusterDNS string
	if svc, err := kubeClient.CoreV1().Services("kube-system").Get(ctx, "kube-dns", metav1.GetOptions{}); err != nil {
		logger.Warn("Could not auto-detect cluster DNS from kube-dns service; DNS may not work in containers", "err", err)
	} else {
		clusterDNS = svc.Spec.ClusterIP
		logger.Info("Auto-detected cluster DNS", "ip", clusterDNS)
	}

	// --- Auto-detect API server address for pod env injection ---
	// Pods need KUBERNETES_SERVICE_HOST/PORT to use in-cluster auth.
	// Use the "kubernetes" service ClusterIP (what real kubelets inject).
	// The kubeconfig Host (e.g. 127.0.0.1:6443) doesn't work from inside
	// nspawn containers because localhost refers to the container, not the host.
	var apiServerHost, apiServerPort string
	if svc, err := kubeClient.CoreV1().Services("default").Get(ctx, "kubernetes", metav1.GetOptions{}); err == nil {
		apiServerHost = svc.Spec.ClusterIP
		if len(svc.Spec.Ports) > 0 {
			apiServerPort = fmt.Sprintf("%d", svc.Spec.Ports[0].Port)
		} else {
			apiServerPort = "443"
		}
		logger.Info("API server address for pod injection", "host", apiServerHost, "port", apiServerPort)
	} else {
		logger.Warn("Could not auto-detect kubernetes service ClusterIP; in-cluster auth may not work in containers", "err", err)
	}

	// --- Clean up stale pawn slices from previous config ---
	{
		pawnNames := make([]string, len(perigeoCfg.Pawns))
		for i, p := range perigeoCfg.Pawns {
			pawnNames[i] = p.Name
		}
		cleanupRT, err := systemd.NewSystemdRuntime(ctx, "_cleanup", nil, logger, execStrategy)
		if err != nil {
			logger.Warn("Could not create cleanup runtime for stale slice check", "err", err)
		} else {
			cleaned, err := cleanupRT.CleanStalePawnSlices(ctx, pawnNames)
			if err != nil {
				logger.Warn("Stale pawn slice cleanup failed", "err", err)
			} else if len(cleaned) > 0 {
				logger.Info("Cleaned stale pawn slices", "count", len(cleaned), "pawns", cleaned)
				// Also clean disk directories for stale pawns.
				for _, pawn := range cleaned {
					pawnDir := fmt.Sprintf("%s/pawns/%s", *baseDirFlag, pawn)
					if err := os.RemoveAll(pawnDir); err != nil {
						logger.Warn("Failed to remove stale pawn dir", "pawn", pawn, "err", err)
					}
				}
			}
			cleanupRT.Close()
		}
	}

	var (
		wg            sync.WaitGroup
		activeServers []*server.PawnServer
	)

	// --- Control socket for apsis CLI and remote control (Varlink) ---
	controlSrv := control.New(*controlSocketFlag, perigeoCfg, logger.With("component", "control"))

	if *controlTCPFlag != "" && perigeoCfg.Global.ServerCAPath != "" {
		caCert, caKey, err := pki.LoadCA(perigeoCfg.Global.ServerCAPath, perigeoCfg.Global.ServerCAKeyPath)
		if err != nil {
			logger.Warn("Could not load CA for control TCP listener — remote access disabled", "err", err)
		} else {
			cert, err := pki.GenerateCert("perigeos-control", caCert, caKey)
			if err != nil {
				logger.Warn("Could not generate control server cert", "err", err)
			} else {
				controlSrv.SetTCPListener(*controlTCPFlag, &cert, caCert)
			}
		}
	}
	wg.Go(func() {
		if err := controlSrv.Start(ctx); err != nil {
			logger.Error("Control socket exited", "err", err)
		}
	})

	// --- Constellation CNI: one agent per host, shared across all pawns ---
	// The agent/operator are deployed separately (not by perigeos).
	// Perigeos only consumes the CNI interface (ADD/DEL via libcni).
	var sharedNM network.NetworkManager
	if perigeoCfg.Global.CNI != nil {
		cnm, err := network.NewConstellationNetworkManager(
			ctx,
			logger.With("component", "network"),
			network.ConstellationConfig{
				ConfDir: perigeoCfg.Global.CNI.ConfDir,
				BinDir:  perigeoCfg.Global.CNI.BinDir,
				Debug:   perigeoCfg.Global.CNI.Debug,
			},
		)
		if err != nil {
			logger.Error("Failed to init Constellation CNI manager", "err", err)
			os.Exit(1)
		}
		sharedNM = cnm
		logger.Info("Constellation CNI active")
	} else {
		logger.Warn("No [global.cni] config and constellation-agent socket not found — falling back to built-in veth networking; cross-host pod connectivity will NOT work. Add [global.cni] to perigeos.toml if Constellation is deployed (socket auto-detection fails when the agent is managed by perigeos and not yet started).")
	}

	// --- Primary node ---
	// If a real kubelet already registered a node with our hostname (e.g.
	// k3s agent), just label it. Otherwise the config processor already
	// added a primary virtual node that will be created like any pawn.
	if perigeoCfg.Global.Primary {
		hostName, _ := os.Hostname()
		if hostName != "" {
			existingNode, err := kubeClient.CoreV1().Nodes().Get(ctx, hostName, metav1.GetOptions{})
			isRealKubelet := err == nil && !strings.HasPrefix(existingNode.Spec.ProviderID, "perigeos://")
			if isRealKubelet {
				// Real kubelet node exists — label it and remove the virtual primary pawn.
				labelPatch := []byte(`{"metadata":{"labels":{` +
					`"periapsis.io/host":"` + hostName + `",` +
					`"periapsis.io/primary":"true",` +
					`"node-role.kubernetes.io/primary":""}}}`)
				if _, err := kubeClient.CoreV1().Nodes().Patch(ctx, hostName, k8stypes.StrategicMergePatchType, labelPatch, metav1.PatchOptions{}); err != nil {
					logger.Warn("Could not label primary node", "node", hostName, "err", err)
				} else {
					logger.Info("Labeled existing primary node", "node", hostName)
				}
				// Drop the auto-generated primary pawn — real kubelet handles it.
				perigeoCfg.Pawns = slices.DeleteFunc(perigeoCfg.Pawns, func(p config.PawnConfig) bool {
					return p.IsPrimary
				})
			} else {
				logger.Info("Creating virtual primary node", "node", hostName)
			}
		}
	}

	// Shared image manager — one manifest cache + singleflight across all pawns.
	sharedIM := image.NewImageManager(perigeoCfg.Global.BaseDir, logger)
	sharedIM.SweepStaleTmpDirs()

	// P2P blob cache — peers pull layers from each other before hitting upstream.
	// Blobs are served via /blobs/{digest} on each pawn's HTTPS server.
	// Port is read per-node from node.Status.DaemonEndpoints.KubeletEndpoint.Port.
	sharedIM.SetPeers(image.PeerConfig{
		Client: kubeClient,
		SelfIP: pki.GetOutboundIP().String(),
	})

	// --- Plugin registration: watch CSI driver sockets, create CSINode per pawn ---
	{
		pawnNames := make([]string, len(perigeoCfg.Pawns))
		for i, p := range perigeoCfg.Pawns {
			pawnNames[i] = p.Name
		}
		pw := plugin.NewPluginWatcher(kubeClient, pawnNames, logger.With("component", "plugin-watcher"))
		wg.Go(func() {
			if err := pw.Run(ctx); err != nil {
				logger.Error("Plugin watcher exited", "err", err)
			}
		})
	}

	var (
		serversMu  sync.Mutex
		allGambits []*node.Gambit
		gambitsMu  sync.Mutex
	)

	for _, pawnCfg := range perigeoCfg.Pawns {
		wg.Go(func() {

			pawnName := pawnCfg.Name
			pawnLogger := logger.With("pawn", pawnName)
			var nm network.NetworkManager
			if sharedNM != nil {
				nm = sharedNM
			} else {
				nm = network.NewLinuxNetworkManager(pawnLogger.With("component", "network"))
			}

			// Use Background context for D-Bus connection — it must survive
			// signal cancellation so DrainPods can stop containers during shutdown.
			rt, err := systemd.NewSystemdRuntime(context.Background(), pawnName, sharedIM, pawnLogger, execStrategy)
			if err != nil {
				pawnLogger.Error("Failed to init runtime", "err", err)
				return
			}

			sliceCfg := perigeos.PawnSliceConfig{
				Name:                pawnCfg.Name,
				BaseDir:             perigeoCfg.Global.BaseDir,
				CPU:                 pawnCfg.CPU,
				Memory:              pawnCfg.Memory,
				CPUWeight:           pawnCfg.CPUWeight,
				IOReadBandwidthMax:  pawnCfg.IOReadBandwidthMax,
				IOWriteBandwidthMax: pawnCfg.IOWriteBandwidthMax,
			}
			if err := rt.InitPawnSlice(ctx, sliceCfg); err != nil {
				pawnLogger.Error("Failed to init pawn slice", "err", err)
				return
			}

			broadcaster := record.NewBroadcaster(record.WithContext(ctx))
			broadcaster.StartRecordingToSink(&v1.EventSinkImpl{
				Interface: kubeClient.CoreV1().Events(corev1.NamespaceAll),
			})
			eventRecorder := broadcaster.NewRecorder(
				clientgoscheme.Scheme,
				corev1.EventSource{Host: pawnName, Component: "perigeos"},
			)

			// Create PodStore for this pawn
			store := node.NewPodStore(rt, max(5, pawnCfg.CreateConcurrency), pawnLogger)
			volumes := node.NewVolumeTracker(pawnCfg.BaseDir, pawnCfg.Name, pawnLogger)
			pawnNode := node.NewPawnNode(pawnCfg, store, sharedIM, pawnLogger)
			g := node.NewGambit(node.GambitDeps{
				Config:         pawnCfg,
				Store:          store,
				Volumes:        volumes,
				Node:           pawnNode,
				ImageManager:   sharedIM,
				NetworkManager: nm,
				Runtime:        rt,
				Logger:         pawnLogger,
				EventRecorder:  eventRecorder,
				CMLister:       cmInformer.Lister(),
				SecretLister:   secretInformer.Lister(),
				SvcLister:      svcInformer.Lister(),
				KubeClient:     kubeClient,
				ClusterDNS:     clusterDNS,
				APIServerHost:  apiServerHost,
				APIServerPort:  apiServerPort,
				CMInformer:     cmInformer.Informer(),
				SecretInformer: secretInformer.Informer(),
			})
			pawnNode.SetDeletePod(g.DeletePod)
			controlSrv.RegisterGambit(g)

			if err := g.HydrateFromRuntime(ctx); err != nil {
				pawnLogger.Warn("Startup hydration failed (non-fatal)", "err", err)
			}

			gambitsMu.Lock()
			allGambits = append(allGambits, g)
			gambitsMu.Unlock()

			// Start the batch watcher — single goroutine per pawn that monitors
			// all containers and handles restarts + probes.
			g.StartBatchWatcher()
			wg.Go(func() { <-ctx.Done(); g.StopBatchWatcher() })

			nodeController, err := node.NewNodeController(
				pawnNode,
				pawnNode.BuildNode(),
				kubeClient.CoreV1().Nodes(),
				node.WithNodeEnableLeaseV1(
					kubeClient.CoordinationV1().Leases(corev1.NamespaceNodeLease),
					node.DefaultLeaseDuration,
				),
				node.WithNodeStatusUpdateErrorHandler(func(ctx context.Context, err error) error {
					pawnLogger.Error("Pawn status update failed", "err", err)
					return err
				}),
				node.WithNodeEventRecorder(eventRecorder),
			)
			if err != nil {
				pawnLogger.Error("Error creating NodeController", "err", err)
				return
			}

			localInformer := informers.NewSharedInformerFactoryWithOptions(
				kubeClient,
				40*time.Second,
				informers.WithTweakListOptions(func(lo *metav1.ListOptions) {
					lo.FieldSelector = "spec.nodeName=" + pawnName
				}),
			)

			// High-throughput rate limiter: 500 items/sec burst 1000, with
			// exponential backoff only on failures (1ms base, 30s max).
			// TODO: Figure out what type it wants
			fastLimiter := workqueue.NewTypedMaxOfRateLimiter(
				&workqueue.TypedBucketRateLimiter[any]{Limiter: rate.NewLimiter(rate.Limit(500), 1000)},
				workqueue.NewTypedItemExponentialFailureRateLimiter[any](1*time.Millisecond, 30*time.Second),
			)
			podController, err := node.NewPodController(node.PodControllerConfig{
				PodClient:                             kubeClient.CoreV1(),
				Provider:                              g,
				EventRecorder:                         eventRecorder,
				PodInformer:                           localInformer.Core().V1().Pods(),
				ConfigMapInformer:                     cmInformer,
				SecretInformer:                        secretInformer,
				ServiceInformer:                       svcInformer,
				SyncPodsFromKubernetesRateLimiter:     fastLimiter,
				SyncPodsFromKubernetesShouldRetryFunc: perigeosRetryFunc,
				DeletePodsFromKubernetesRateLimiter:   fastLimiter,
				SyncPodStatusFromProviderRateLimiter:  fastLimiter,
			})
			if err != nil {
				pawnLogger.Error("Error creating PodController", "err", err)
				return
			}
			controlSrv.RegisterQueues(pawnName, podController)

			localInformer.Start(ctx.Done())
			pawnLogger.Info("Waiting for local informer caches to sync...")
			for _, synced := range localInformer.WaitForCacheSync(ctx.Done()) {
				if !synced {
					pawnLogger.Error("Failed to sync local cache")
				}
			}
			pawnLogger.Info("Local caches synced")

			podLister := localInformer.Core().V1().Pods().Lister().Pods(corev1.NamespaceAll)
			reconciler := node.NewReconciler(g, rt, nm, sharedIM, podLister, pawnLogger.With("component", "reconciler"))

			// Wire the forward reconciler callback (the only setter that stays).
			g.SetSyncRequester(podController.RequestSync)

			// Pre-create CiliumNode so the operator allocates a CIDR before
			// the agent finishes booting. Eliminates the race where the agent
			// restarts and loses its sub-allocator because the CiliumNode
			// didn't exist yet when it looked.
			if sharedNM != nil {
				nodeIP := g.NodeIP()
				if err := network.EnsureCiliumNode(ctx, dynClient, pawnLogger, pawnName, nodeIP); err != nil {
					pawnLogger.Warn("Failed to ensure CiliumNode (non-fatal)", "err", err)
				}
			}

			// Start NodeController — registers the node and keeps lease alive.
			wg.Go(func() {
				pawnLogger.Info("Starting NodeController")

				if err := nodeController.Run(ctx); err != nil {
					pawnLogger.Error("NodeController exited", "err", err)
				}
			})

			// Start PodController — watches and reconciles pods for this pawn.
			wg.Go(func() {
				pawnLogger.Info("Starting PodController")

				if err := podController.Run(ctx, max(5, pawnCfg.CreateConcurrency)); err != nil {
					pawnLogger.Error("PodController exited", "err", err)
				}
			})

			// Purge hydrated pods that k8s no longer knows about.
			// Wait briefly so the PodController can call CreatePod for real pods.
			go func() {
				select {
				case <-time.After(30 * time.Second):
					g.PurgeStaleHydrated(podLister)

				case <-ctx.Done():
				}
			}()

			wg.Go(func() {
				reconciler.Run(ctx)
			})

			pawnServer, err := server.NewPawnServer(g, server.PawnServerConfig{
				CACertPath:   perigeoCfg.Global.ServerCAPath,
				CAKeyPath:    perigeoCfg.Global.ServerCAKeyPath,
				ConfigDir:    filepath.Dir(*perigeosConfigPath),
				KubeClient:   kubeClient,
				ImageManager: sharedIM,
			})
			if err != nil {
				pawnLogger.Error("Error creating PawnServer — port already bound or TLS failure", "port", pawnCfg.Port, "err", err)
				return
			}
			pawnLogger.Info("Pawn API server listening", "port", pawnCfg.Port)

			serversMu.Lock()
			activeServers = append(activeServers, pawnServer)
			serversMu.Unlock()

			wg.Go(func() {
				if err := pawnServer.Start(); err != http.ErrServerClosed {
					pawnLogger.Error("API server exited", "err", err)
				}
			})
		})
	}

	// Wait briefly for at least one pawn to start, then check.
	time.Sleep(2 * time.Second)
	serversMu.Lock()
	startedCount := len(activeServers)
	serversMu.Unlock()
	if startedCount == 0 {
		logger.Warn("No pawns started within 2s, waiting for all to complete...")
	}

	logger.Info("Perigeos running", "pawns", len(perigeoCfg.Pawns), "base-dir", *baseDirFlag)

	// Notify systemd that startup is complete. With Type=notify, systemd
	// waits for this before reporting the unit as active.
	if sent, err := daemon.SdNotify(false, daemon.SdNotifyReady); err != nil {
		logger.Warn("sd_notify READY failed", "err", err)
	} else if sent {
		logger.Debug("sd_notify READY sent")
	}

	// Start watchdog pings if WatchdogSec is configured. The interval
	// returned by SdWatchdogEnabled is half the configured period.
	if watchdogInterval, err := daemon.SdWatchdogEnabled(false); err == nil && watchdogInterval > 0 {
		go func() {
			ticker := time.NewTicker(watchdogInterval / 2)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					_, _ = daemon.SdNotify(false, daemon.SdNotifyWatchdog)
				case <-ctx.Done():
					return
				}
			}
		}()
		logger.Info("Watchdog pings enabled", "interval", watchdogInterval/2)
	}

	// Sweep stale network namespaces left by ghost pods from previous runs.
	if cnm, ok := sharedNM.(*network.ConstellationNetworkManager); ok {
		go func() {
			// Wait for hydration and informer sync to settle.
			select {
			case <-time.After(15 * time.Second):
			case <-ctx.Done():
				return
			}
			activeUIDs := controlSrv.AllPodUIDs()
			cnm.SweepStaleNetns(ctx, activeUIDs)
		}()
	}

	<-ctx.Done()

	logger.Info("Shutdown signal received")
	_, _ = daemon.SdNotify(false, daemon.SdNotifyStopping)

	// Signal all pawns to begin graceful shutdown — Ping returns error,
	// nodes go NotReady, scheduler stops placing new pods.
	gambitsMu.Lock()
	for _, g := range allGambits {
		g.Shutdown()
	}
	gambitsMu.Unlock()

	// With KillMode=process, containers survive perigeos restart.
	// We do NOT drain pods here — HydrateFromRuntime will rediscover
	// them on next start. For explicit node decommission, use
	// `perigeos drain` before stopping the service.

	// Wait for any in-progress deletions (from apiserver) to finish.
	// This prevents the daemon from exiting while containers are still stopping.
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer drainCancel()
	for drainCtx.Err() == nil {
		anyDeleting := false
		gambitsMu.Lock()
		for _, g := range allGambits {
			if g.DeletionsInProgress() {
				anyDeleting = true
				break
			}
		}
		gambitsMu.Unlock()
		if !anyDeleting {
			break
		}
		logger.Info("Waiting for in-progress deletions to complete...")
		select {
		case <-drainCtx.Done():
			logger.Warn("Drain timeout — exiting with deletions still in progress")
		case <-time.After(500 * time.Millisecond):
		}
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := controlSrv.Stop(shutdownCtx); err != nil {
		logger.Warn("Control socket shutdown error", "err", err)
	}
	for _, srv := range activeServers {
		if err := srv.Stop(shutdownCtx); err != nil {
			logger.Warn("Server shutdown error", "err", err)
		}
	}

	// Preserve CiliumNode CRDs across restarts so the operator doesn't
	// reassign CIDRs. CiliumNodes are only deleted on explicit teardown
	// (e.g. node decommissioning), not on service restart.

	wg.Wait()

	logger.Info("Shutdown complete")
}

// runJoin parses join flags and runs the join command.
func runJoin(logger *slog.Logger) {
	fs := flag.NewFlagSet("perigeos join", flag.ExitOnError)
	opts := &join.Options{}
	opts.RegisterFlags(fs)
	_ = fs.Parse(os.Args[2:])

	if err := opts.Validate(); err != nil {
		logger.Error("Invalid join options", "err", err)
		fs.Usage()
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	runner := join.New(opts, logger)
	if err := runner.Run(ctx); err != nil {
		logger.Error("Join failed", "err", err)
		os.Exit(1)
	}
}

// addNoProxy appends addr to the NO_PROXY / no_proxy environment variables
// so the Go HTTP client does not route connections to local pawn addresses
// through any ambient HTTPS_PROXY. Both casing variants are updated because
// different programs read different forms.
func addNoProxy(addr string) {
	if addr == "" {
		return
	}
	// Strip port if present (net.SplitHostPort returns an error for bare IPs).
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}

	for _, key := range []string{"NO_PROXY", "no_proxy"} {
		cur := os.Getenv(key)
		if cur == "" {
			_ = os.Setenv(key, host)

			continue
		}

		for entry := range strings.SplitSeq(cur, ",") {
			if strings.TrimSpace(entry) == host {
				goto next
			}
		}

		_ = os.Setenv(key, fmt.Sprintf("%s,%s", cur, host))

	next:
	}
}
