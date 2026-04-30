// Copyright (C) 2025-2026 Malformed C. All rights reserved.
// SPDX-License-Identifier: BUSL-1.1

package image

// Store is the interface that lifecycle.go and hydration.go use to interact
// with image storage. ImageManager is the current implementation; an importd-
// backed implementation will follow once the D-Bus integration is ready.
//
// All methods must be safe for concurrent use.
type Store interface {
	// PullWithOptions ensures all layers for imageName are available locally.
	// Returns (layerPaths, fromCache, error). layerPaths are absolute paths,
	// ordered bottom-to-top, ready for overlayfs lowerdir or mstack layer@ entries.
	PullWithOptions(imageName, pullPolicy string, opts PullOptions) ([]string, bool, error)

	// ImageEntrypoint returns the OCI Entrypoint and Cmd for an image.
	// Returns (nil, nil) if unknown.
	ImageEntrypoint(imageName string) (entrypoint, cmd []string)

	// PrepareMStack creates or reuses a .mstack directory for a container.
	// On restart, only resets the rw/ writable layer; layers and bind entries
	// are preserved.
	PrepareMStack(podUID, cName string, layers []string, binds []MStackBindEntry) (string, error)

	// InvalidateMStack removes the .mstack directory entirely, forcing a full
	// rebuild on the next PrepareMStack call. Use on structural errors (EBADMSG).
	InvalidateMStack(podUID, cName string) error

	// RemoveMStack deletes the .mstack directory. Called at pod deletion.
	RemoveMStack(podUID, cName string) error

	// Mount assembles an overlayfs rootfs for a container.
	// Legacy path for systemd < 260; returns the merged directory path.
	Mount(podUID string, layerPaths []string) (string, error)

	// Unmount tears down a legacy overlayfs rootfs.
	Unmount(podUID string) error

	// GetLayerCachePath returns the directory where extracted layers are stored.
	// Used for disk-pressure calculation in PawnNode.
	GetLayerCachePath() string

	// ListCachedImages returns metadata for all locally cached images.
	ListCachedImages() ([]CachedImage, error)
}
