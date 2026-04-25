// Copyright (C) 2025-2026 Malformed C. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-only

// Package cgroup contains helpers for translating Kubernetes resource
// quantities into cgroup v2 / systemd unit properties.
package cgroup

const (
	// Kubernetes CPU "shares" range and default semantics.
	// See kubelet milliCPU->shares conversion and cgroup v2 weight mapping.
	MinCPUShares int64 = 2
	MaxCPUShares int64 = 262144
)

// MilliCPUToCPUWeight converts Kubernetes millicores to systemd CPUWeight.
// Conversion follows Kubernetes CPU shares semantics:
//
//	shares = milliCPU * 1024 / 1000, clamped to [2, 262144]
//	weight = 1 + ((shares - 2) * 9999) / 262142
func MilliCPUToCPUWeight(milliCPU int64) uint64 {
	if milliCPU <= 0 {
		return 0
	}
	shares := min(max(milliCPU*1024/1000, MinCPUShares), MaxCPUShares)
	return uint64(1 + ((shares-MinCPUShares)*9999)/(MaxCPUShares-MinCPUShares))
}
