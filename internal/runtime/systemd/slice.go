// Copyright (C) 2024-2026 Malformed C. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-only

package systemd

import (
	"context"
	"fmt"
	"strings"

	"github.com/coreos/go-systemd/v22/dbus"
	dbusv5 "github.com/godbus/dbus/v5"
	"github.com/malformed-c/periapsis/internal/runtime"
	"golang.org/x/sys/unix"
)

// InitPawnSlice creates and starts the cgroup slice for a pawn,
// applying CPU, memory, and IO resource limits.
func (s *SystemdRuntime) InitPawnSlice(ctx context.Context, cfg runtime.PawnSliceConfig) error {
	s.logger.Info("Initializing Pawn Slice", "pawn", cfg.Name)

	name := sliceName(cfg.Name)

	// Reset failed state so a previously-failed slice doesn't block recreation.
	s.conn.ResetFailedUnitContext(ctx, name)

	properties := []dbus.Property{
		dbus.PropDescription(fmt.Sprintf("Perigeos Pawn: %s", cfg.Name)),
		{Name: "MemoryAccounting", Value: dbusv5.MakeVariant(true)},
		{Name: "CPUAccounting", Value: dbusv5.MakeVariant(true)},
		{Name: "IOAccounting", Value: dbusv5.MakeVariant(true)},
	}

	if cfg.Memory.Value() > 0 {
		properties = append(properties, dbus.Property{
			Name:  "MemoryMax",
			Value: dbusv5.MakeVariant(uint64(cfg.Memory.Value())),
		})
	}

	if cfg.CPU.MilliValue() > 0 {
		// CPUQuotaPerSecUSec: 1 mCPU = 1000 µs quota per second
		quotaUSec := uint64(cfg.CPU.MilliValue() * 1000)
		properties = append(properties, dbus.Property{
			Name:  "CPUQuotaPerSecUSec",
			Value: dbusv5.MakeVariant(quotaUSec),
		})
	}

	if cfg.CPUWeight > 0 {
		properties = append(properties, dbus.Property{
			Name:  "CPUWeight",
			Value: dbusv5.MakeVariant(cfg.CPUWeight),
		})
	}

	if cfg.IOReadBandwidthMax.Value() > 0 || cfg.IOWriteBandwidthMax.Value() > 0 {
		devicePath, err := getBackingDevice(cfg.BaseDir)
		if err != nil {
			s.logger.Error("Cannot resolve backing device for IO limits", "err", err)
		} else {
			type ioLimit struct {
				Path  string
				Limit uint64
			}

			if cfg.IOReadBandwidthMax.Value() > 0 {
				properties = append(properties, dbus.Property{
					Name:  "IOReadBandwidthMax",
					Value: dbusv5.MakeVariant([]ioLimit{{Path: devicePath, Limit: uint64(cfg.IOReadBandwidthMax.Value())}}),
				})
			}

			if cfg.IOWriteBandwidthMax.Value() > 0 {
				properties = append(properties, dbus.Property{
					Name:  "IOWriteBandwidthMax",
					Value: dbusv5.MakeVariant([]ioLimit{{Path: devicePath, Limit: uint64(cfg.IOWriteBandwidthMax.Value())}}),
				})
			}
		}
	}

	ch := make(chan string, 1)
	_, err := s.conn.StartTransientUnitContext(ctx, name, "replace", properties, ch)
	if err != nil {
		if strings.Contains(err.Error(), "already loaded") || strings.Contains(err.Error(), "already exists") {
			s.logger.Info("Slice already loaded, updating properties", "slice", name)

			if err := s.conn.SetUnitPropertiesContext(ctx, name, true, properties...); err != nil {
				return fmt.Errorf("failed to update slice properties: %w", err)
			}

			chStart := make(chan string, 1)
			if _, err := s.conn.StartUnitContext(ctx, name, "replace", chStart); err != nil {
				return fmt.Errorf("failed to start existing slice: %w", err)
			}
			select {
			case res := <-chStart:
				if res != "done" {
					return fmt.Errorf("slice restart failed: %s", res)
				}
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		}
		return fmt.Errorf("failed to create pawn slice: %w", err)
	}

	select {
	case result := <-ch:
		if result != "done" {
			return fmt.Errorf("slice creation failed: %s", result)
		}
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}

// CleanStalePawnSlices stops perigeos-*.slice units that are not in the
// configured pawn set. Called once at startup to garbage-collect slices
// left behind when pawns are removed from the config.
func (s *SystemdRuntime) CleanStalePawnSlices(ctx context.Context, activePawns []string) ([]string, error) {
	units, err := s.conn.ListUnitsByPatternsContext(ctx, nil, []string{"perigeos-*.slice"})
	if err != nil {
		return nil, fmt.Errorf("list perigeos slices: %w", err)
	}

	active := make(map[string]struct{}, len(activePawns))
	for _, name := range activePawns {
		active[sliceName(name)] = struct{}{}
	}

	var cleaned []string
	for _, unit := range units {
		if _, ok := active[unit.Name]; ok {
			continue
		}
		// Skip parent slices that contain active pawn sub-slices.
		// e.g. perigeos-compute.slice is parent of perigeos-compute-00.slice
		isParent := false
		prefix := strings.TrimSuffix(unit.Name, ".slice") + "-"
		for activeName := range active {
			if strings.HasPrefix(activeName, prefix) {
				isParent = true
				break
			}
		}
		if isParent {
			continue
		}
		// First stop any pod services under this slice.
		pawn := strings.TrimPrefix(unit.Name, "perigeos-")
		pawn = strings.TrimSuffix(pawn, ".slice")
		podPattern := fmt.Sprintf("perigeos-%s-pod-*.service", pawn)
		podUnits, _ := s.conn.ListUnitsByPatternsContext(ctx, nil, []string{podPattern})
		for _, pu := range podUnits {
			ch := make(chan string, 1)
			s.conn.StopUnitContext(ctx, pu.Name, "replace", ch)
			s.conn.ResetFailedUnitContext(ctx, pu.Name)
		}

		ch := make(chan string, 1)
		if _, err := s.conn.StopUnitContext(ctx, unit.Name, "replace", ch); err != nil {
			s.logger.Warn("Failed to stop stale slice", "slice", unit.Name, "err", err)
			continue
		}
		s.conn.ResetFailedUnitContext(ctx, unit.Name)
		cleaned = append(cleaned, pawn)
		s.logger.Info("Cleaned stale pawn slice", "slice", unit.Name)
	}
	return cleaned, nil
}

// SliceActive returns whether this pawn's cgroup slice is active in systemd.
func (s *SystemdRuntime) SliceActive(ctx context.Context) bool {
	name := sliceName(s.pawnName)
	units, err := s.conn.ListUnitsByPatternsContext(ctx, []string{"active"}, []string{name})
	if err != nil {
		return false
	}
	return len(units) > 0
}

// getBackingDevice resolves the block device path for a given filesystem path.
// Returns a path suitable for systemd IO limit properties (e.g. /dev/block/8:0).
func getBackingDevice(path string) (string, error) {
	var stat unix.Stat_t
	if err := unix.Stat(path, &stat); err != nil {
		return "", fmt.Errorf("stat %s: %w", path, err)
	}

	dev := stat.Dev
	major := (dev >> 8) & 0xfff
	minor := (dev & 0xff) | ((dev >> 12) & 0xfff00)

	return fmt.Sprintf("/dev/block/%d:%d", major, minor), nil
}
