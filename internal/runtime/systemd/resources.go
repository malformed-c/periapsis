// Copyright (C) 2024-2026 Malformed C. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-only

package systemd

import (
	"github.com/containerd/cgroups/v3/cgroup2"
	"github.com/malformed-c/periapsis/internal/cgroup"
	"github.com/malformed-c/periapsis/internal/runtime"
)

// buildPodResources lifts per-container resource limits from the pod spec into
// a cgroup2.Resources struct. The systemd property emitter in internal/cgroup
// then translates this into the appropriate transient-unit D-Bus properties.
//
// This is the single seam where future pod-label-driven cgroup knobs should
// be wired in (IO throttling, Pids limits, memory.high, cpuset, etc.) so that
// both runProgram and RunMachine inherit them for free.
func buildPodResources(cfg runtime.PodConfig) *cgroup2.Resources {
	res := &cgroup2.Resources{}

	var cpu cgroup2.CPU
	hasCPU := false
	if w := cgroup.MilliCPUToCPUWeight(cfg.CPURequestMillis); w > 0 {
		cpu.Weight = &w
		hasCPU = true
	}
	if cfg.CPULimitMillis > 0 {
		cpu.Max = cgroup.MilliCPUToCPUMax(cfg.CPULimitMillis)
		hasCPU = true
	}
	if hasCPU {
		res.CPU = &cpu
	}

	if cfg.MemoryLimitBytes > 0 {
		memMax := int64(cfg.MemoryLimitBytes)
		res.Memory = &cgroup2.Memory{Max: &memMax}
	}

	return res
}
