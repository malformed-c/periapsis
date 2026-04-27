// Copyright (C) 2025-2026 Malformed C. All rights reserved.
// SPDX-License-Identifier: BUSL-1.1

package systemd

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"

	"github.com/malformed-c/periapsis/internal/runtime"
)

// prepareBindFiles generates host-side files that are bind-mounted into the
// container instead of being written into the overlay rootfs. This approach
// works with both the legacy --directory= path and the new --overlay= path
// since it does not require a writable merged rootfs to exist before start.
//
// Returns additional BindMount entries to append to cfg.BindMounts.
//
// The caller is responsible for cleaning up the returned tmpDir after the
// container stops.
func prepareBindFiles(cfg runtime.PodConfig, logger *slog.Logger) ([]runtime.BindMount, string, error) {
	if cfg.RunAsUser == nil && cfg.ClusterDNS == "" {
		return nil, "", nil
	}

	// All generated files live under a single tmpDir keyed by pod+container
	// so cleanup is a single os.RemoveAll.
	tmpDir, err := os.MkdirTemp("", fmt.Sprintf("perigeos-%s-%s-", cfg.UID, cfg.ContainerName))
	if err != nil {
		return nil, "", fmt.Errorf("prepareBindFiles: mktemp: %w", err)
	}

	var mounts []runtime.BindMount

	// resolv.conf
	if cfg.ClusterDNS != "" {
		p, err := writeResolvConfFile(tmpDir, cfg.ClusterDNS, cfg.Namespace)
		if err != nil {
			os.RemoveAll(tmpDir)
			return nil, "", err
		}
		mounts = append(mounts, runtime.BindMount{
			HostPath:      p,
			ContainerPath: "/etc/resolv.conf",
			ReadOnly:      true,
		})
	}

	if cfg.RunAsUser != nil {
		uid := *cfg.RunAsUser
		gid := int64(0)
		if cfg.RunAsGroup != nil {
			gid = *cfg.RunAsGroup
		}

		// /etc/passwd
		p, err := writePasswdFile(tmpDir, uid, gid)
		if err != nil {
			os.RemoveAll(tmpDir)
			return nil, "", err
		}
		mounts = append(mounts, runtime.BindMount{
			HostPath:      p,
			ContainerPath: "/etc/passwd",
			ReadOnly:      true,
		})

		// /etc/group
		p, err = writeGroupFile(tmpDir, gid)
		if err != nil {
			os.RemoveAll(tmpDir)
			return nil, "", err
		}
		mounts = append(mounts, runtime.BindMount{
			HostPath:      p,
			ContainerPath: "/etc/group",
			ReadOnly:      true,
		})

		// home dir (writable bind from host tmpdir)
		if uid != 0 {
			username := fmt.Sprintf("peri-%d", uid)
			homeHost := filepath.Join(tmpDir, "home", username)
			if err := os.MkdirAll(homeHost, 0750); err == nil {
				_ = os.Chown(homeHost, int(uid), int(gid))
				mounts = append(mounts, runtime.BindMount{
					HostPath:      homeHost,
					ContainerPath: fmt.Sprintf("/home/%s", username),
					ReadOnly:      false,
				})
			}
		}
	}

	// getent shim: detect musl from layer paths if available, fall back to
	// checking cfg.RootFS. TODO(overlay-refactor): detect from erofs manifest
	// once RootImage= path is implemented.
	isMusl := false
	if cfg.RootFS != "" {
		isMusl = isMuslRootFS(cfg.RootFS)
	}
	if isMusl {
		p, err := writeGetentShim(tmpDir, logger)
		if err != nil {
			logger.Warn("prepareBindFiles: failed to write getent shim", "err", err)
		} else {
			mounts = append(mounts, runtime.BindMount{
				HostPath:      p,
				ContainerPath: "/usr/local/bin/getent",
				ReadOnly:      true,
			})
		}
	}

	return mounts, tmpDir, nil
}

func writeResolvConfFile(dir, dnsIP, namespace string) (string, error) {
	searchDomains := fmt.Sprintf("%s.svc.cluster.local svc.cluster.local cluster.local", namespace)
	content := fmt.Sprintf("nameserver %s\nsearch %s\noptions ndots:5\n", dnsIP, searchDomains)
	p := filepath.Join(dir, "resolv.conf")
	if err := os.WriteFile(p, []byte(content), 0644); err != nil {
		return "", fmt.Errorf("write resolv.conf: %w", err)
	}
	return p, nil
}

func writePasswdFile(dir string, uid, gid int64) (string, error) {
	username := fmt.Sprintf("peri-%d", uid)
	home := "/"
	if uid != 0 {
		home = fmt.Sprintf("/home/%s", username)
	}
	line := fmt.Sprintf("%s:x:%s:%s::%s:/bin/sh\n",
		username, strconv.FormatInt(uid, 10), strconv.FormatInt(gid, 10), home)
	p := filepath.Join(dir, "passwd")
	if err := os.WriteFile(p, []byte(line), 0644); err != nil {
		return "", fmt.Errorf("write passwd: %w", err)
	}
	return p, nil
}

func writeGroupFile(dir string, gid int64) (string, error) {
	groupName := fmt.Sprintf("peri-%d", gid)
	line := fmt.Sprintf("%s:x:%s:\n", groupName, strconv.FormatInt(gid, 10))
	p := filepath.Join(dir, "group")
	if err := os.WriteFile(p, []byte(line), 0644); err != nil {
		return "", fmt.Errorf("write group: %w", err)
	}
	return p, nil
}

func writeGetentShim(dir string, logger *slog.Logger) (string, error) {
	p := filepath.Join(dir, "getent")
	if err := os.WriteFile(p, []byte(getentShimScript), 0755); err != nil {
		return "", fmt.Errorf("write getent shim: %w", err)
	}
	logger.Debug("prepareBindFiles: wrote getent shim", "path", p)
	return p, nil
}
