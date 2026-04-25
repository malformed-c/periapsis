// Copyright (C) 2024-2026 Malformed C. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-only

package systemd

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
)

// isMuslRootFS returns true if the rootfs uses musl libc (e.g. Alpine).
// Detection: musl-based images have /lib/ld-musl-*.so.* as their dynamic linker.
func isMuslRootFS(rootfs string) bool {
	matches, _ := filepath.Glob(filepath.Join(rootfs, "lib", "ld-musl-*.so.*"))
	return len(matches) > 0
}

// getentShimScript is a POSIX shell shim for `getent initgroups` that uses
// only shell builtins - no grep, cat, ls, or other external commands.
// systemd-nspawn v260 runs `getent initgroups <user>` during --user= resolution,
// and musl libc's getent doesn't support the initgroups database.
const getentShimScript = `#!/bin/sh
# Shim for musl libc: handles "getent initgroups" using only shell builtins.
# musl's getent lacks initgroups support; systemd-nspawn requires it for --user=.
if [ "$1" = "initgroups" ]; then
    user="$2"
    [ -z "$user" ] && exit 1
    # Resolve user - may be a username or numeric UID.
    resolved=""
    while IFS=: read -r name x uid gid gecos home shell; do
        if [ "$name" = "$user" ] || [ "$uid" = "$user" ]; then
            resolved="$name"
            break
        fi
    done < /etc/passwd
    [ -z "$resolved" ] && exit 2
    groups=""
    while IFS=: read -r gname x gid members; do
        rest=",$members,"
        case "$rest" in
            *",$resolved,"*) groups="$groups $gid" ;;
        esac
    done < /etc/group
    printf "%s%s\n" "$resolved" "$groups"
    exit 0
fi
# For all other databases (passwd, group, etc.), delegate to the real getent.
for p in /usr/bin/getent /bin/getent /usr/sbin/getent; do
    [ -x "$p" ] && [ "$p" != "$0" ] && exec "$p" "$@"
done
exit 1
`

// installGetentShim installs a getent shim into the rootfs that handles the
// `initgroups` database using shell builtins. The original getent is preserved
// at /usr/bin/getent and the shim is placed at /usr/local/bin/getent (which
// nspawn's PATH prefers over /usr/bin).
func installGetentShim(rootfs string, logger *slog.Logger) error {
	shimDir := filepath.Join(rootfs, "usr", "local", "bin")
	if err := os.MkdirAll(shimDir, 0755); err != nil {
		return fmt.Errorf("create shim dir: %w", err)
	}

	shimPath := filepath.Join(shimDir, "getent")
	if err := os.WriteFile(shimPath, []byte(getentShimScript), 0755); err != nil {
		return fmt.Errorf("write getent shim: %w", err)
	}

	logger.Info("Installed getent initgroups shim for musl rootfs", "path", shimPath)
	return nil
}

// ensureGetentShim checks if the rootfs uses musl libc and installs a getent
// shim if needed. This must be called before nspawn starts with --user= on
// non-root UIDs, because musl's getent lacks initgroups support which nspawn
// requires for user resolution.
func ensureGetentShim(rootfs string, logger *slog.Logger) {
	if !isMuslRootFS(rootfs) {
		return
	}
	logger.Warn("Container rootfs uses musl libc - getent initgroups unsupported, installing shim",
		"rootfs", rootfs)
	if err := installGetentShim(rootfs, logger); err != nil {
		logger.Error("Failed to install getent shim", "error", err)
	}
}
