// Copyright (C) 2025-2026 Malformed C. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-only

// Package psi reads Linux Pressure Stall Information (PSI) from /proc/pressure.
// PSI reports the percentage of time tasks are stalled on CPU, memory, or I/O.
// See https://docs.kernel.org/accounting/psi.html
package psi

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// Pressure holds the avg10/avg60/avg300 values from a single PSI line.
type Pressure struct {
	Avg10  float64
	Avg60  float64
	Avg300 float64
}

// HostPressure holds CPU and memory pressure readings.
type HostPressure struct {
	CPU    Pressure // "some" line from /proc/pressure/cpu
	Memory Pressure // "full" line from /proc/pressure/memory
}

// Read returns the current host-wide PSI readings.
// CPU uses the "some" metric (any task stalled), memory uses "full"
// (all tasks stalled - the signal that actually causes OOM pressure).
func Read() (HostPressure, error) {
	var hp HostPressure
	var err error

	hp.CPU, err = parsePSIFile("/proc/pressure/cpu", "some")
	if err != nil {
		return hp, fmt.Errorf("cpu pressure: %w", err)
	}

	hp.Memory, err = parsePSIFile("/proc/pressure/memory", "full")
	if err != nil {
		return hp, fmt.Errorf("memory pressure: %w", err)
	}

	return hp, nil
}

func parsePSIFile(path, prefix string) (Pressure, error) {
	f, err := os.Open(path)
	if err != nil {
		return Pressure{}, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, prefix+" ") {
			continue
		}
		var p Pressure
		// Format: some avg10=0.00 avg60=0.00 avg300=0.00 total=12345
		_, err := fmt.Sscanf(line, prefix+" avg10=%f avg60=%f avg300=%f",
			&p.Avg10, &p.Avg60, &p.Avg300)
		if err != nil {
			return Pressure{}, fmt.Errorf("parsing %s: %w", path, err)
		}
		return p, nil
	}
	return Pressure{}, fmt.Errorf("%s: no %q line found", path, prefix)
}
