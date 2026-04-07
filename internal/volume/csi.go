// Package volume handles CSI gRPC calls for mounting CSI-provisioned volumes.
package volume

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/container-storage-interface/spec/lib/go/csi"
	corev1 "k8s.io/api/core/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// CSIClient manages CSI gRPC calls for a single CSI driver.
type CSIClient struct {
	driverName string
	socketPath string
	conn       *grpc.ClientConn
}

// NewCSIClient creates a CSI client by discovering the socket for the given driver name.
// The socket is expected at /var/lib/kubelet/plugins/<driverName>/csi.sock
func NewCSIClient(driverName string) (*CSIClient, error) {
	socketPath := filepath.Join("/var/lib/kubelet/plugins", driverName, "csi.sock")

	if _, err := os.Stat(socketPath); err != nil {
		return nil, fmt.Errorf("CSI socket not found for driver %s at %s: %w", driverName, socketPath, err)
	}

	conn, err := grpc.NewClient(
		"unix://"+socketPath,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("dial CSI socket %s: %w", socketPath, err)
	}

	return &CSIClient{
		driverName: driverName,
		socketPath: socketPath,
		conn:       conn,
	}, nil
}

// pvAccessModeToCSI converts a Kubernetes PV access mode list to the corresponding
// CSI VolumeCapability_AccessMode_Mode.
func pvAccessModeToCSI(modes []corev1.PersistentVolumeAccessMode) csi.VolumeCapability_AccessMode_Mode {
	for _, m := range modes {
		switch m {
		case corev1.ReadWriteMany:
			return csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER
		case corev1.ReadOnlyMany:
			return csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY
		case corev1.ReadWriteOncePod:
			return csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER
		}
	}
	// Default: RWO
	return csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER
}

// NodeStageVolume calls the CSI NodeStageVolume RPC to prepare a volume for use.
func (c *CSIClient) NodeStageVolume(
	ctx context.Context,
	volumeID string,
	stagingPath string,
	accessModes []corev1.PersistentVolumeAccessMode,
	volumeContext map[string]string,
	publishContext map[string]string,
	secrets map[string]string,
) error {
	client := csi.NewNodeClient(c.conn)

	req := &csi.NodeStageVolumeRequest{
		VolumeId:          volumeID,
		StagingTargetPath: stagingPath,
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: pvAccessModeToCSI(accessModes),
			},
		},
		VolumeContext:  volumeContext,
		PublishContext: publishContext,
		Secrets:        secrets,
	}

	_, err := client.NodeStageVolume(ctx, req)
	if err != nil {
		return fmt.Errorf("NodeStageVolume failed for volume %s: %w", volumeID, err)
	}
	return nil
}

// NodePublishVolume calls the CSI NodePublishVolume RPC to make a volume available
// at a target path. This is called after NodeStageVolume.
func (c *CSIClient) NodePublishVolume(
	ctx context.Context,
	volumeID string,
	stagingPath string,
	targetPath string,
	accessModes []corev1.PersistentVolumeAccessMode,
	volumeContext map[string]string,
	publishContext map[string]string,
	readOnly bool,
) error {
	client := csi.NewNodeClient(c.conn)

	req := &csi.NodePublishVolumeRequest{
		VolumeId:          volumeID,
		PublishContext:     publishContext,
		StagingTargetPath: stagingPath,
		TargetPath:        targetPath,
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: pvAccessModeToCSI(accessModes),
			},
		},
		Readonly:      readOnly,
		VolumeContext: volumeContext,
	}

	_, err := client.NodePublishVolume(ctx, req)
	if err != nil {
		return fmt.Errorf("NodePublishVolume failed for volume %s: %w", volumeID, err)
	}
	return nil
}

// NodeUnpublishVolume calls the CSI NodeUnpublishVolume RPC.
func (c *CSIClient) NodeUnpublishVolume(ctx context.Context, volumeID string, targetPath string) error {
	client := csi.NewNodeClient(c.conn)

	_, err := client.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{
		VolumeId:   volumeID,
		TargetPath: targetPath,
	})
	if err != nil {
		return fmt.Errorf("NodeUnpublishVolume failed for volume %s: %w", volumeID, err)
	}
	return nil
}

// NodeUnstageVolume calls the CSI NodeUnstageVolume RPC.
func (c *CSIClient) NodeUnstageVolume(ctx context.Context, volumeID string, stagingPath string) error {
	client := csi.NewNodeClient(c.conn)

	_, err := client.NodeUnstageVolume(ctx, &csi.NodeUnstageVolumeRequest{
		VolumeId:          volumeID,
		StagingTargetPath: stagingPath,
	})
	if err != nil {
		return fmt.Errorf("NodeUnstageVolume failed for volume %s: %w", volumeID, err)
	}
	return nil
}

// Close closes the gRPC connection.
func (c *CSIClient) Close() error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}
