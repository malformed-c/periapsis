package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"

	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/lmittmann/tint"
	"github.com/malformed-c/periapsis/internal/config"
	"github.com/malformed-c/periapsis/internal/control"
	"github.com/malformed-c/periapsis/internal/image"
	"github.com/malformed-c/periapsis/internal/network"
	"github.com/malformed-c/periapsis/internal/pki"
	"github.com/malformed-c/periapsis/internal/provider"
	pruntime "github.com/malformed-c/periapsis/internal/runtime"
	"github.com/malformed-c/periapsis/internal/runtime/systemd"
	"github.com/malformed-c/periapsis/internal/server"
	"github.com/malformed-c/periapsis/node"
	vklog "github.com/malformed-c/periapsis/log"
	"github.com/malformed-c/periapsis/internal/vklogger"
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
	"k8s.io/klog/v2"
)

func main() {
	logger := slog.New(tint.NewHandler(os.Stdout, &tint.Options{
		Level:      slog.LevelDebug,
		TimeFormat: "15:04:05",
	}))
	slog.SetDefault(logger)
	klog.SetSlogLogger(logger.WithGroup("klog"))
	vklog.L = vklogger.New(logger.WithGroup("vk"))
	klog.InitFlags(nil)

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
	baseDirFlag := flag.String("base-dir", "/var/lib/perigeos",
		"Base directory for Perigeos state. Use a local path (e.g. ./var/lib/perigeos) for dev.")
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

	dynamicClient, err := dynamic.NewForConfig(kubeConfig)
	if err != nil {
		logger.Error("Failed to create dynamic kubernetes client", "err", err)
		os.Exit(1)
	}

	serverVersion, err := kubeClient.Discovery().ServerVersion()
	if err != nil {
		klog.Fatalf("Failed to connect to Kubernetes API server: %v", err)
	}

	klog.Infof("Connected to Kubernetes API server %s", serverVersion)

	var execStrategy pruntime.ExecStrategy
	switch *execStrategyFlag {
	case "machinectl":
		execStrategy = pruntime.ExecMachinectl
		logger.Info("Using machinectl exec strategy")
	default:
		execStrategy = pruntime.ExecNsenter
		logger.Info("Using nsenter exec strategy")
	}

	globalInformerFactory := informers.NewSharedInformerFactory(kubeClient, 40*time.Second)
	cmInformer := globalInformerFactory.Core().V1().ConfigMaps()
	secretInformer := globalInformerFactory.Core().V1().Secrets()
	svcInformer := globalInformerFactory.Core().V1().Services()
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
	// Parse from the rest config's Host field (e.g. "https://192.168.0.200:6443").
	var apiServerHost, apiServerPort string
	if u, err := url.Parse(kubeConfig.Host); err == nil {
		apiServerHost = u.Hostname()
		apiServerPort = u.Port()
		if apiServerPort == "" {
			apiServerPort = "443"
		}
		logger.Info("API server address for pod injection", "host", apiServerHost, "port", apiServerPort)
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
		logger.Debug("No [global.cni] config, using built-in veth networking per pawn")
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
					`"perigeos.io/host":"` + hostName + `",` +
					`"perigeos.io/primary":"true",` +
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

	var (
		serversMu  sync.Mutex
		allGambits []*provider.Gambit
		gambitsMu  sync.Mutex
	)

	for _, pawnCfg := range perigeoCfg.Pawns {
		wg.Go(func() {

			pawnName := pawnCfg.Name
			pawnLogger := logger.With("pawn", pawnName)

			im := image.NewImageManager(perigeoCfg.Global.BaseDir, pawnName, logger)
			im.SweepStaleTmpDirs()
			var nm network.NetworkManager
			if sharedNM != nil {
				nm = sharedNM
			} else {
				nm = network.NewLinuxNetworkManager(pawnLogger.With("component", "network"))
			}

			// Use Background context for D-Bus connection — it must survive
			// signal cancellation so DrainPods can stop containers during shutdown.
			rt, err := systemd.NewSystemdRuntime(context.Background(), pawnName, im, pawnLogger, execStrategy)
			if err != nil {
				pawnLogger.Error("Failed to init runtime", "err", err)
				return
			}

			sliceCfg := pruntime.PawnSliceConfig{
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
				corev1.EventSource{Host: pawnName, Component: "Perigeos"},
			)

			g := provider.NewGambit(pawnCfg, im, nm, rt, pawnLogger, eventRecorder)
			controlSrv.RegisterGambit(g)

			if err := g.HydrateFromRuntime(ctx); err != nil {
				pawnLogger.Warn("Startup hydration failed (non-fatal)", "err", err)
			}

			gambitsMu.Lock()
			allGambits = append(allGambits, g)
			gambitsMu.Unlock()

			// Start the batch watcher — single goroutine per pawn that monitors
			// all containers and handles restarts + probes.
			bw := provider.StartBatchWatcher(g)
			wg.Go(func() { <-ctx.Done(); bw.Stop() })

			lp := provider.NewLoggingProvider(g, pawnLogger)

			nodeController, err := node.NewNodeController(
				lp,
				g.BuildNode(),
				kubeClient.CoreV1().Nodes(),
				node.WithNodeEnableLeaseV1(
					kubeClient.CoordinationV1().Leases(corev1.NamespaceNodeLease),
					node.DefaultLeaseDuration,
				),
				node.WithNodeStatusUpdateErrorHandler(func(ctx context.Context, err error) error {
					pawnLogger.Error("Pawn status update failed", "err", err)
					return err
				}),
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

			podController, err := node.NewPodController(node.PodControllerConfig{
				PodClient:         kubeClient.CoreV1(),
				Provider:          lp,
				EventRecorder:     eventRecorder,
				PodInformer:       localInformer.Core().V1().Pods(),
				ConfigMapInformer: cmInformer,
				SecretInformer:    secretInformer,
				ServiceInformer:   svcInformer,
			})
			if err != nil {
				pawnLogger.Error("Error creating PodController", "err", err)
				return
			}

			localInformer.Start(ctx.Done())
			pawnLogger.Info("Waiting for local informer caches to sync...")
			for _, synced := range localInformer.WaitForCacheSync(ctx.Done()) {
				if !synced {
					pawnLogger.Error("Failed to sync local cache")
				}
			}
			pawnLogger.Info("Local caches synced")

			podLister := localInformer.Core().V1().Pods().Lister().Pods(corev1.NamespaceAll)
			reconciler := provider.NewReconciler(g, rt, nm, im, podLister, pawnLogger.With("component", "reconciler"))

			// Wire volume listers so configMap/secret volumes can be resolved.
			g.SetVolumeListers(
				cmInformer.Lister(),
				secretInformer.Lister(),
			)
			g.SetKubeClient(kubeClient)
			if clusterDNS != "" {
				g.SetClusterDNS(clusterDNS)
			}
			if apiServerHost != "" {
				g.SetAPIServer(apiServerHost, apiServerPort)
			}

			// Create CiliumNode for this pawn so the constellation operator
			// allocates a per-pawn /24 CIDR for IPAM. Only when CNI is active.
			if sharedNM != nil {
				if err := network.EnsureCiliumNode(ctx, dynamicClient, pawnLogger, pawnName, g.NodeIP()); err != nil {
					pawnLogger.Warn("Could not create CiliumNode (IPAM will use primary CIDR)", "err", err)
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

				if err := podController.Run(ctx, 1); err != nil {
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

			pawnServer, err := server.NewPawnServer(g, perigeoCfg.Global.ServerCAPath, perigeoCfg.Global.ServerCAKeyPath)
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

	// Signal all pawns to begin graceful shutdown — Ping returns error,
	// nodes go NotReady, scheduler stops placing new pods.
	gambitsMu.Lock()
	for _, g := range allGambits {
		g.Shutdown()
	}
	gambitsMu.Unlock()

	// Actively drain all pods — stop containers, tear down networking,
	// clean up volumes. Don't wait passively for apiserver DeletePod calls
	// since VK's pod controller is no longer processing events.
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer drainCancel()

	gambitsMu.Lock()
	for _, g := range allGambits {
		g.DrainPods(drainCtx)
	}
	gambitsMu.Unlock()

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

	// Clean up CiliumNode CRDs for pawns that had CNI active.
	if sharedNM != nil {
		for _, pawnCfg := range perigeoCfg.Pawns {
			network.DeleteCiliumNode(shutdownCtx, dynamicClient, logger, pawnCfg.Name)
		}
	}

	wg.Wait()

	logger.Info("Shutdown complete")
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
