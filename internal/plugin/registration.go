package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/fsnotify/fsnotify"
	storagev1 "k8s.io/api/storage/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	registerapi "k8s.io/kubelet/pkg/apis/pluginregistration/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// PluginWatcher watches for CSI driver socket registrations and creates CSINode objects.
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
// It scans existing sockets on startup and watches for new ones.
func (pw *PluginWatcher) Run(ctx context.Context) error {
	pluginRegistryDir := "/var/lib/kubelet/plugins_registry/"

	// Scan existing sockets on startup
	if entries, err := os.ReadDir(pluginRegistryDir); err == nil {
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			if !strings.HasSuffix(entry.Name(), ".sock") {
				continue
			}
			socketPath := filepath.Join(pluginRegistryDir, entry.Name())
			if err := pw.handleSocket(ctx, socketPath); err != nil {
				pw.logger.Warn("Failed to handle existing socket", "socket", socketPath, "err", err)
			}
		}
	} else {
		pw.logger.Warn("Could not scan plugin registry directory", "dir", pluginRegistryDir, "err", err)
	}

	// Start fsnotify watcher for new sockets
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
			pw.logger.Debug("New socket detected", "socket", event.Name)
			if err := pw.handleSocket(ctx, event.Name); err != nil {
				pw.logger.Warn("Failed to handle socket", "socket", event.Name, "err", err)
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				return fmt.Errorf("fsnotify watcher error channel closed")
			}
			pw.logger.Error("fsnotify watcher error", "err", err)
		}
	}
}

// handleSocket processes a single socket file by dialing the plugin registration
// service, getting plugin info, dialing the CSI endpoint, getting node info,
// and creating CSINode objects for each pawn.
func (pw *PluginWatcher) handleSocket(ctx context.Context, socketPath string) error {
	// Dial the plugin registration service
	conn, err := grpc.NewClient(
		"unix://"+socketPath,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("dial plugin registration socket %s: %w", socketPath, err)
	}
	defer conn.Close()

	regClient := registerapi.NewRegistrationClient(conn)
	info, err := regClient.GetInfo(ctx, &registerapi.InfoRequest{})
	if err != nil {
		return fmt.Errorf("call GetInfo on %s: %w", socketPath, err)
	}

	// Only process CSI plugins
	if info.Type != registerapi.CSIPlugin {
		pw.logger.Debug("Skipping non-CSI plugin", "socket", socketPath, "type", info.Type)
		return nil
	}

	pw.logger.Debug("Processing CSI plugin", "plugin", info.Name, "socket", socketPath)

	// Determine the CSI endpoint
	endpoint := info.Endpoint
	if endpoint == "" {
		endpoint = socketPath
	}

	// Dial the CSI endpoint
	csiConn, err := grpc.NewClient(
		"unix://"+endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("dial CSI endpoint %s: %w", endpoint, err)
	}
	defer csiConn.Close()

	// Get node info
	nodeClient := csi.NewNodeClient(csiConn)
	nodeResp, err := nodeClient.NodeGetInfo(ctx, &csi.NodeGetInfoRequest{})
	if err != nil {
		return fmt.Errorf("call NodeGetInfo on %s: %w", endpoint, err)
	}

	// Create CSINode for each pawn
	for _, pawnName := range pw.pawnNames {
		if err := pw.ensureCSINode(ctx, pawnName, info.Name, nodeResp); err != nil {
			pw.logger.Warn("Failed to ensure CSINode for pawn", "pawn", pawnName, "plugin", info.Name, "err", err)
		}
	}

	// Notify the plugin that registration is complete
	if _, err := regClient.NotifyRegistrationStatus(ctx, &registerapi.RegistrationStatus{
		PluginRegistered: true,
	}); err != nil {
		pw.logger.Warn("Failed to notify registration status", "plugin", info.Name, "err", err)
	}

	return nil
}

// ensureCSINode ensures that a CSINode exists for the given pawn with the
// specified driver information. It creates the CSINode if it doesn't exist,
// or appends the driver if the CSINode already exists.
func (pw *PluginWatcher) ensureCSINode(ctx context.Context, pawnName, driverName string, nodeInfo *csi.NodeGetInfoResponse) error {
	// Get the Node object to obtain its UID for OwnerReference
	node, err := pw.kubeClient.CoreV1().Nodes().Get(ctx, pawnName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get node %s: %w", pawnName, err)
	}

	// Extract topology keys from AccessibleTopology
	var topologyKeys []string
	if nodeInfo.AccessibleTopology != nil && nodeInfo.AccessibleTopology.Segments != nil {
		for key := range nodeInfo.AccessibleTopology.Segments {
			topologyKeys = append(topologyKeys, key)
		}
	}

	// Build allocatable resources if MaxVolumesPerNode is set
	var allocatable *storagev1.VolumeNodeResources
	if nodeInfo.MaxVolumesPerNode > 0 {
		count := int32(nodeInfo.MaxVolumesPerNode)
		allocatable = &storagev1.VolumeNodeResources{
			Count: &count,
		}
	}

	// Create the driver entry
	driverEntry := storagev1.CSINodeDriver{
		Name:         driverName,
		NodeID:       nodeInfo.NodeId,
		TopologyKeys: topologyKeys,
		Allocatable:  allocatable,
	}

	// Try to get existing CSINode
	csiNode, err := pw.kubeClient.StorageV1().CSINodes().Get(ctx, pawnName, metav1.GetOptions{})
	if err != nil && !k8serrors.IsNotFound(err) {
		return fmt.Errorf("get CSINode %s: %w", pawnName, err)
	}

	if k8serrors.IsNotFound(err) {
		// Create new CSINode
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
	} else {
		// Check if driver already exists
		driverExists := false
		for _, existingDriver := range csiNode.Spec.Drivers {
			if existingDriver.Name == driverName {
				driverExists = true
				break
			}
		}

		if !driverExists {
			// Append the driver
			csiNode.Spec.Drivers = append(csiNode.Spec.Drivers, driverEntry)
			if _, err := pw.kubeClient.StorageV1().CSINodes().Update(ctx, csiNode, metav1.UpdateOptions{}); err != nil {
				return fmt.Errorf("update CSINode %s: %w", pawnName, err)
			}
			pw.logger.Info("Added driver to CSINode", "node", pawnName, "driver", driverName)
		}
	}

	// Update Node annotation with CSI node IDs
	nodeIDAnnotationKey := "csi.volume.kubernetes.io/nodeid"
	if node.Annotations == nil {
		node.Annotations = make(map[string]string)
	}

	// Parse existing annotation value (it's a JSON object)
	nodeIDMap := make(map[string]string)
	if existingJSON, ok := node.Annotations[nodeIDAnnotationKey]; ok {
		if err := json.Unmarshal([]byte(existingJSON), &nodeIDMap); err != nil {
			pw.logger.Warn("Could not parse existing node ID annotation", "node", pawnName, "err", err)
		}
	}

	// Add/update the driver entry
	nodeIDMap[driverName] = nodeInfo.NodeId

	// Re-marshal and update annotation
	newJSON, err := json.Marshal(nodeIDMap)
	if err != nil {
		return fmt.Errorf("marshal node ID map: %w", err)
	}

	// Strategic merge patch to update Node annotation
	annotationPatch := []byte(fmt.Sprintf(`{"metadata":{"annotations":{"%s":%s}}}`,
		nodeIDAnnotationKey, string(newJSON)))

	if _, err := pw.kubeClient.CoreV1().Nodes().Patch(ctx, pawnName, k8stypes.StrategicMergePatchType, annotationPatch, metav1.PatchOptions{}); err != nil {
		return fmt.Errorf("patch node annotation: %w", err)
	}

	return nil
}
