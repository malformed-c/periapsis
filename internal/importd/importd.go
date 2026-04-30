// Copyright (C) 2025-2026 Malformed C. All rights reserved.
// SPDX-License-Identifier: BUSL-1.1

// Package importd wraps the org.freedesktop.import1 D-Bus interface exposed
// by systemd-importd. It provides OCI image pulling via PullOci() and image
// lifecycle management (list, remove). The returned .mstack directories
// contain layer@N.raw DDI symlinks that systemd-nspawn and systemd-mstack
// consume natively.
package importd

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	dbus "github.com/godbus/dbus/v5"
)

const (
	importdDest      = "org.freedesktop.import1"
	importdPath      = "/org/freedesktop/import1"
	importdIface     = "org.freedesktop.import1.Manager"
	transferIface    = "org.freedesktop.import1.Transfer"
	machineClass     = "machine"

	// flagForce bit 0: overwrite existing image with same local name.
	flagForce uint64 = 1 << 0
)

// Client is a D-Bus client for systemd-importd.
type Client struct {
	conn   *dbus.Conn
	obj    dbus.BusObject
	logger *slog.Logger
}

// New connects to the system D-Bus and returns a Client.
func New(logger *slog.Logger) (*Client, error) {
	conn, err := dbus.ConnectSystemBus()
	if err != nil {
		return nil, fmt.Errorf("importd: connect system bus: %w", err)
	}

	return &Client{
		conn:   conn,
		obj:    conn.Object(importdDest, dbus.ObjectPath(importdPath)),
		logger: logger,
	}, nil
}

// Close releases the D-Bus connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

// ImageInfo is returned by ListImages.
type ImageInfo struct {
	Class     string
	LocalName string
	Type      string
	Path      string
	ReadOnly  bool
	CreatedAt time.Time
	ModifiedAt time.Time
}

// PullOci downloads an OCI image via importd and waits for the transfer to
// complete. localName is used as the image name under /var/lib/machines/;
// it must be hostname-safe. On success, the .mstack directory is available
// at /var/lib/machines/<localName>.mstack.
//
// If an image with localName already exists, it is overwritten (flagForce).
func (c *Client) PullOci(ctx context.Context, ref, localName string) error {
	c.logger.Info("importd: pulling OCI image", "ref", ref, "localName", localName)

	var transferID uint32
	var transferPath dbus.ObjectPath

	err := c.obj.CallWithContext(ctx, importdIface+".PullOci", 0,
		ref,          // OCI container reference
		localName,    // local image name
		machineClass, // class: "machine"
		flagForce,    // flags: force overwrite
	).Store(&transferID, &transferPath)
	if err != nil {
		return fmt.Errorf("importd: PullOci(%s): %w", ref, err)
	}

	c.logger.Info("importd: transfer started", "id", transferID, "path", transferPath)

	return c.waitTransfer(ctx, transferID)
}

// waitTransfer subscribes to TransferRemoved signals and blocks until the
// transfer with id completes, fails, or ctx is cancelled.
func (c *Client) waitTransfer(ctx context.Context, id uint32) error {
	if err := c.conn.AddMatchSignal(
		dbus.WithMatchInterface(importdIface),
		dbus.WithMatchMember("TransferRemoved"),
		dbus.WithMatchObjectPath(importdPath),
	); err != nil {
		return fmt.Errorf("importd: subscribe TransferRemoved: %w", err)
	}
	defer c.conn.RemoveMatchSignal( //nolint:errcheck
		dbus.WithMatchInterface(importdIface),
		dbus.WithMatchMember("TransferRemoved"),
		dbus.WithMatchObjectPath(importdPath),
	)

	ch := make(chan *dbus.Signal, 8)
	c.conn.Signal(ch)
	defer c.conn.RemoveSignal(ch)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case sig, ok := <-ch:
			if !ok {
				return fmt.Errorf("importd: signal channel closed")
			}
			if sig.Name != importdIface+".TransferRemoved" || len(sig.Body) < 3 {
				continue
			}

			sigID, _ := sig.Body[0].(uint32)
			if sigID != id {
				continue
			}

			result, _ := sig.Body[2].(string)
			switch result {
			case "done":
				c.logger.Info("importd: transfer complete", "id", id)
				return nil
			case "canceled":
				return fmt.Errorf("importd: transfer %d canceled", id)
			default:
				return fmt.Errorf("importd: transfer %d failed: %s", id, result)
			}
		}
	}
}

// ListImages returns all images of class "machine" known to importd.
func (c *Client) ListImages(ctx context.Context) ([]ImageInfo, error) {
	var raw [][]interface{}

	err := c.obj.CallWithContext(ctx, importdIface+".ListImages", 0,
		machineClass, // class filter
		uint64(0),    // flags (must be 0)
	).Store(&raw)
	if err != nil {
		return nil, fmt.Errorf("importd: ListImages: %w", err)
	}

	images := make([]ImageInfo, 0, len(raw))
	for _, item := range raw {
		if len(item) < 8 {
			continue
		}
		info := ImageInfo{
			Class:     strVal(item[0]),
			LocalName: strVal(item[1]),
			Type:      strVal(item[2]),
			Path:      strVal(item[3]),
			ReadOnly:  boolVal(item[4]),
		}
		if us, ok := item[5].(uint64); ok && us > 0 {
			info.CreatedAt = time.UnixMicro(int64(us))
		}
		if us, ok := item[6].(uint64); ok && us > 0 {
			info.ModifiedAt = time.UnixMicro(int64(us))
		}
		images = append(images, info)
	}

	return images, nil
}

// ImageMStackPath returns the expected .mstack directory path for a local
// image name. importd stores machine images under /var/lib/machines/.
func ImageMStackPath(localName string) string {
	return "/var/lib/machines/" + localName + ".mstack"
}

// LocalName converts an OCI image reference to a hostname-safe local name
// suitable for importd. importd requires names that are valid hostnames.
//
// Examples:
//
//	docker.io/library/nginx:latest → nginx-latest
//	docker.io/library/busybox:uclibc → busybox-uclibc
//	ghcr.io/org/myapp:v1.2.3 → myapp-v1.2.3
func LocalName(ref string) string {
	// Strip registry prefix (everything before the last /)
	if i := strings.LastIndex(ref, "/"); i >= 0 {
		ref = ref[i+1:]
	}
	// Replace : with - for tag separator
	ref = strings.ReplaceAll(ref, ":", "-")
	// Replace any remaining non-hostname chars with -
	var b strings.Builder
	for _, r := range ref {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' || r == '.' {
			b.WriteRune(r)
		} else {
			b.WriteByte('-')
		}
	}
	name := strings.Trim(b.String(), "-")
	if name == "" {
		return "image"
	}
	return name
}

func strVal(v interface{}) string {
	s, _ := v.(string)
	return s
}

func boolVal(v interface{}) bool {
	b, _ := v.(bool)
	return b
}
