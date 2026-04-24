package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/fsnotify/fsnotify"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	storagev1 "k8s.io/api/storage/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	registerapi "k8s.io/kubelet/pkg/apis/pluginregistration/v1"
)

const (
	// pluginRegistryDir is where node-driver-registrar places its sockets.
	// perigeos creates this directory at startup so hostPath volumes that
	// reference it with type=Directory or type=DirectoryOrCreate succeed.
	pluginRegistryDir = "/var/lib/kubelet/plugins_registry"

	// pluginRegistryKubeletDir is where kubelet normally keeps its own plugin
	// sockets; some CSI DaemonSets mount this too.
	pluginRegistryKubeletDir = "/var/lib/kubelet/plugins"

	// handleRetryInterval is how long to wait between GetInfo retries.
	// The registrar's gRPC server may not be ready immediately after the
	// socket file appears (kernel creates the inode before bind(2) completes).
	handleRetryInterval = 500 * time.Millisecond

	// handleRetryTimeout caps how long we wait for the registrar to answer.
	handleRetryTimeout = 30 * time.Second
)

// PluginWatcher watches for CSI driver socket registrations and creates CSINode objects.
// It acts as the kubelet-side of the node plugin registration protocol:
//
//  1. node-driver-registrar creates a socket at pluginRegistryDir/<driver>-reg.sock
//  2. PluginWatcher detects the socket via fsnotify and calls GetInfo() on it
//  3. PluginWatcher dials the CSI endpoint returned by GetInfo, calls NodeGetInfo()
//  4. PluginWatcher creates/updates CSINode objects for each pawn
//  5. PluginWatcher calls NotifyRegistrationStatus(ok) - this unblocks the registrar
//     and prevents it from timing out and crashing
type PluginWatcher struct {
	kubeClient kubernetes.Interface
	pawnNames  []string
	logger     *slog.Logger
}

// NewPluginWatcher creates a new PluginWatcher for the given pawn names.
func NewPluginWatcher(kubeClient kubernetes.Interface, pawnNames []string, logger *slog.Logger) *PluginWatcher {
	return &PluginWatcher{
		kubeClient: kubeClient,
		pawnNames:  pawnNames,
		logger:     logger,
	}
}

// Run starts watching the kubelet plugins registry directory.
// It ensures the directory exists (so CSI DaemonSet hostPath volumes succeed),
// scans existing sockets on startup, and watches for new ones.
func (pw *PluginWatcher) Run(ctx context.Context) error {
	// Ensure both kubelet plugin directories exist. CSI DaemonSets reference
	// them as hostPath volumes (type=Directory or DirectoryOrCreate). If they
	// don't exist the bind mount fails and the registrar container can't create
	// its socket - making the pod crash before registration even starts.
	for _, dir := range []string{pluginRegistryDir, pluginRegistryKubeletDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			pw.logger.Warn("Failed to create plugin directory", "dir", dir, "err", err)
		}
	}

	// Scan existing sockets on startup (registrar may have run before us).
	if entries, err := os.ReadDir(pluginRegistryDir); err == nil {
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sock") {

				continue
			}

			socketPath := filepath.Join(pluginRegistryDir, entry.Name())
			go pw.handleSocketWithRetry(ctx, socketPath)
		}

	} else {
		pw.logger.Warn("Could not scan plugin registry directory", "dir", pluginRegistryDir, "err", err)
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("create fsnotify watcher: %w", err)
	}
	defer watcher.Close()

	if err := watcher.Add(pluginRegistryDir); err != nil {
		return fmt.Errorf("add plugin registry dir to watcher: %w", err)
	}

	pw.logger.Info("Plugin watcher started", "dir", pluginRegistryDir)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case event, ok := <-watcher.Events:
			if !ok {
				return fmt.Errorf("fsnotify watcher closed")
			}

			if event.Op != fsnotify.Create {
				continue
			}

			if !strings.HasSuffix(event.Name, ".sock") {
				continue
			}

			pw.logger.Debug("New plugin socket detected", "socket", event.Name)

			// Handle in a goroutine - retry loop must not block the watcher.
			go pw.handleSocketWithRetry(ctx, event.Name)

		case err, ok := <-watcher.Errors:
			if !ok {
				return fmt.Errorf("fsnotify watcher error channel closed")
			}

			pw.logger.Error("fsnotify watcher error", "err", err)
		}
	}
}

// handleSocketWithRetry retries handleSocket until success or timeout.
// The socket file exists before the registrar's gRPC server is bound to it
// (kernel creates the inode first), so the first dial attempt often races.
// A 30-second window covers slow image pulls and container start latency.
func (pw *PluginWatcher) handleSocketWithRetry(ctx context.Context, socketPath string) {
	retryCtx, cancel := context.WithTimeout(ctx, handleRetryTimeout)
	defer cancel()

	for {
		err := pw.handleSocket(retryCtx, socketPath)
		if err == nil {
			return
		}

		pw.logger.Debug("Plugin registration attempt failed, will retry",
			"socket", socketPath, "err", err)

		select {
		case <-retryCtx.Done():
			pw.logger.Warn("Plugin registration timed out", "socket", socketPath, "err", err)

			return

		case <-time.After(handleRetryInterval):
		}
	}
}

// handleSocket performs a single registration attempt for a socket:
//  1. Dials the registrar's gRPC server and calls GetInfo()
//  2. Resolves the CSI endpoint to a host-visible path
//  3. Calls NodeGetInfo() on the CSI plugin
//  4. Creates/updates CSINode for each pawn
//  5. Calls NotifyRegistrationStatus(ok) to unblock the registrar
func (pw *PluginWatcher) handleSocket(ctx context.Context, socketPath string) error {
	conn, err := grpc.NewClient(
		"unix://"+socketPath,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)

	if err != nil {
		return fmt.Errorf("dial registration socket: %w", err)
	}
	defer conn.Close()

	regClient := registerapi.NewRegistrationClient(conn)
	info, err := regClient.GetInfo(ctx, &registerapi.InfoRequest{})
	if err != nil {
		return fmt.Errorf("GetInfo: %w", err)
	}

	if info.Type != registerapi.CSIPlugin {
		pw.logger.Debug("Skipping non-CSI plugin", "socket", socketPath, "type", info.Type)
		// Still notify so the registrar doesn't hang.
		_, _ = regClient.NotifyRegistrationStatus(ctx, &registerapi.RegistrationStatus{
			PluginRegistered: true,
		})

		return nil
	}

	pw.logger.Info("Registering CSI plugin", "driver", info.Name, "socket", socketPath)

	// The endpoint returned by GetInfo() is the path as seen INSIDE the
	// registrar container (e.g. "/csi/csi.sock"). perigeos bind-mounts the
	// emptyDir backing that path to a host directory under the pod state dir.
	// We need the HOST-side path so we can dial the CSI plugin from the host.
	//
	// The CSI plugin socket is also a hostPath or emptyDir volume, so it is
	// reachable on the host at the same path (hostPath volumes) or via the
	// emptyDir host directory. For the common seaweedfs layout where the CSI
	// socket is at /csi/csi.sock backed by an emptyDir, the host path is
	// pluginRegistryKubeletDir/<driverName>/csi.sock - the conventional
	// staging path that perigeos creates during NodeStageVolume.
	//
	// Fall back: if the endpoint path exists on the host, use it directly
	// (works for hostPath-backed sockets). Otherwise derive the staging path.
	endpoint := info.Endpoint
	if endpoint == "" {
		endpoint = socketPath
	}
	endpoint = pw.resolveCSIEndpoint(info.Name, endpoint)

	csiConn, err := grpc.NewClient(
		"unix://"+endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("dial CSI endpoint %s: %w", endpoint, err)
	}
	defer csiConn.Close()

	nodeClient := csi.NewNodeClient(csiConn)
	nodeResp, err := nodeClient.NodeGetInfo(ctx, &csi.NodeGetInfoRequest{})
	if err != nil {
		return fmt.Errorf("NodeGetInfo on %s: %w", endpoint, err)
	}

	for _, pawnName := range pw.pawnNames {
		if err := pw.ensureCSINode(ctx, pawnName, info.Name, nodeResp); err != nil {
			pw.logger.Warn("Failed to ensure CSINode",
				"pawn", pawnName, "driver", info.Name, "err", err)
		}
	}

	// Critical: notify the registrar that registration succeeded.
	// Without this call the registrar blocks, then times out and exits,
	// crashing the CSI pod.
	if _, err := regClient.NotifyRegistrationStatus(ctx, &registerapi.RegistrationStatus{
		PluginRegistered: true,
	}); err != nil {
		// Log but don't fail - CSINode is already created, the side effect is done.
		pw.logger.Warn("NotifyRegistrationStatus failed", "driver", info.Name, "err", err)
	}

	pw.logger.Info("CSI plugin registered", "driver", info.Name)

	return nil
}

// resolveCSIEndpoint converts a container-internal socket path to a path that
// is reachable from the host.
//
// Strategy:
//  1. If the path exists on the host as-is, use it (hostPath-backed socket).
//  2. Otherwise look for the socket under pluginRegistryKubeletDir/<driverName>/,
//     which is the conventional staging location and where perigeos surfaces
//     emptyDir-backed sockets via bind mount.
//  3. Fall back to the original path and let the caller surface the error.
func (pw *PluginWatcher) resolveCSIEndpoint(driverName, containerPath string) string {
	// Fast path: socket is directly accessible on the host (e.g. HostPath volume).
	if _, err := os.Stat(containerPath); err == nil {
		return containerPath
	}

	// Derive host path: pluginRegistryKubeletDir/<driverName>/<basename>
	base := filepath.Base(containerPath)
	hostPath := filepath.Join(pluginRegistryKubeletDir, driverName, base)
	if _, err := os.Stat(hostPath); err == nil {
		return hostPath
	}

	// Also try pluginRegistryKubeletDir/<driverName>/csi.sock - some drivers
	// use non-standard names but are always staged here by perigeos.
	stagingSocket := filepath.Join(pluginRegistryKubeletDir, driverName, "csi.sock")
	if _, err := os.Stat(stagingSocket); err == nil {
		return stagingSocket
	}

	pw.logger.Debug("Could not resolve CSI endpoint to host path, using as-is",
		"driver", driverName, "path", containerPath)

	return containerPath
}

// ensureCSINode creates or updates the CSINode for a pawn with the given driver.
func (pw *PluginWatcher) ensureCSINode(ctx context.Context, pawnName, driverName string, nodeInfo *csi.NodeGetInfoResponse) error {
	node, err := pw.kubeClient.CoreV1().Nodes().Get(ctx, pawnName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get node %s: %w", pawnName, err)
	}

	var topologyKeys []string
	if nodeInfo.AccessibleTopology != nil {
		for key := range nodeInfo.AccessibleTopology.Segments {
			topologyKeys = append(topologyKeys, key)
		}
	}

	var allocatable *storagev1.VolumeNodeResources
	if nodeInfo.MaxVolumesPerNode > 0 {
		count := int32(nodeInfo.MaxVolumesPerNode)
		allocatable = &storagev1.VolumeNodeResources{Count: &count}
	}

	driverEntry := storagev1.CSINodeDriver{
		Name:         driverName,
		NodeID:       nodeInfo.NodeId,
		TopologyKeys: topologyKeys,
		Allocatable:  allocatable,
	}

	csiNode, err := pw.kubeClient.StorageV1().CSINodes().Get(ctx, pawnName, metav1.GetOptions{})
	if k8serrors.IsNotFound(err) {
		newCSINode := &storagev1.CSINode{
			ObjectMeta: metav1.ObjectMeta{
				Name: pawnName,
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: "v1",
					Kind:       "Node",
					Name:       pawnName,
					UID:        node.UID,
				}},
			},
			Spec: storagev1.CSINodeSpec{
				Drivers: []storagev1.CSINodeDriver{driverEntry},
			},
		}

		if _, err := pw.kubeClient.StorageV1().CSINodes().Create(ctx, newCSINode, metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("create CSINode %s: %w", pawnName, err)
		}

		pw.logger.Info("Created CSINode", "node", pawnName, "driver", driverName)

	} else if err != nil {
		return fmt.Errorf("get CSINode %s: %w", pawnName, err)

	} else {
		// Update existing: add driver if not already present, or patch nodeID if changed.
		updated := false
		for i, d := range csiNode.Spec.Drivers {
			if d.Name == driverName {
				if d.NodeID != nodeInfo.NodeId {
					csiNode.Spec.Drivers[i] = driverEntry
					updated = true
				}

				goto patchAnnotation
			}
		}

		csiNode.Spec.Drivers = append(csiNode.Spec.Drivers, driverEntry)
		updated = true

	patchAnnotation:
		if updated {
			if _, err := pw.kubeClient.StorageV1().CSINodes().Update(ctx, csiNode, metav1.UpdateOptions{}); err != nil {
				return fmt.Errorf("update CSINode %s: %w", pawnName, err)
			}

			pw.logger.Info("Updated CSINode", "node", pawnName, "driver", driverName)
		}
	}

	// Keep the csi.volume.kubernetes.io/nodeid annotation on the Node object
	// in sync. Some external schedulers and admission controllers read this.
	nodeIDMap := make(map[string]string)
	const nodeIDAnnotationKey = "csi.volume.kubernetes.io/nodeid"
	if existing, ok := node.Annotations[nodeIDAnnotationKey]; ok {
		_ = json.Unmarshal([]byte(existing), &nodeIDMap)
	}
	nodeIDMap[driverName] = nodeInfo.NodeId

	// 1. Marshal the inner map to a JSON byte slice
	innerJSONBytes, err := json.Marshal(nodeIDMap)
	if err != nil {
		return fmt.Errorf("marshal nodeID annotation: %w", err)
	}

	// 2. Build the outer patch using proper Go maps.
	// By casting innerJSONBytes to a string, json.Marshal will automatically
	// escape it into a proper JSON string (e.g. "{\"seaweedfs...\"}")
	patchMap := map[string]any{
		"metadata": map[string]any{
			"annotations": map[string]string{
				nodeIDAnnotationKey: string(innerJSONBytes),
			},
		},
	}

	patchBytes, err := json.Marshal(patchMap)
	if err != nil {
		return fmt.Errorf("marshal patch: %w", err)
	}

	if _, err := pw.kubeClient.CoreV1().Nodes().Patch(
		ctx, pawnName, k8stypes.StrategicMergePatchType, patchBytes, metav1.PatchOptions{},
	); err != nil {
		return fmt.Errorf("patch node annotation: %w", err)
	}

	return nil
}
