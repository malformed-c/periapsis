// Copyright (C) 2024-2026 Malformed C. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-only

package join

import (
	"errors"
	"log/slog"
	"os"
	"os/exec"
)

// checkPrerequisites verifies the host satisfies join requirements:
//   - running as root
//   - systemd is PID 1
//   - systemd-nspawn is on PATH
//   - hostname is set
func checkPrerequisites(logger *slog.Logger) error {
	var errs []error

	// Must be root.
	if os.Getuid() != 0 {
		errs = append(errs, errors.New("perigeos join must run as root"))
	}

	// systemd must be PID 1.
	target, err := os.Readlink("/proc/1/exe")
	if err != nil || (target != "/usr/lib/systemd/systemd" && target != "/lib/systemd/systemd" && target != "/sbin/init") {
		// Try a secondary check: /run/systemd/private must exist.
		if _, err2 := os.Stat("/run/systemd/private"); err2 != nil {
			errs = append(errs, errors.New("systemd is required as PID 1 (not detected)"))
		}
	}

	// systemd-nspawn must be available.
	if _, err := exec.LookPath("systemd-nspawn"); err != nil {
		errs = append(errs, errors.New("systemd-nspawn not found on PATH; install systemd-container"))
	}

	// Hostname must be set.
	hn, err := os.Hostname()
	if err != nil || hn == "" || hn == "localhost" {
		errs = append(errs, errors.New("hostname is not set or is 'localhost'; set a unique hostname before joining"))
	} else {
		logger.Info("Hostname detected", "hostname", hn)
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}
