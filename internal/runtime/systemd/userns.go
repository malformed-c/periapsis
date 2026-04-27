// Copyright (C) 2025-2026 Malformed C. All rights reserved.
// SPDX-License-Identifier: BUSL-1.1

package systemd

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

// uidbaseSlots is the number of deterministic UIDBASE buckets per node.
// Collision risk is acceptable for single-node deployments at typical pod counts.
const uidbaseSlots = 256

// computeUIDBASE returns a deterministic UIDBASE for userns isolation.
// UIDBASE = 65536 * (2 + hash(podUID) % 256). Slot 0 is reserved for the host,
// slot 1 for systemd-nspawn's own use; usable slots start at offset 2.
func computeUIDBASE(podUID string) uint32 {
	h := sha256.Sum256([]byte(podUID))
	slot := binary.LittleEndian.Uint32(h[:4]) % uidbaseSlots
	return 65536 * (2 + slot)
}

// injectPasswdEntry ensures /etc/passwd in rootfs contains an entry for the
// given uid/gid. If the UID already exists, no changes are made. Otherwise a
// new entry is appended with username "peri-<uid>".
func injectPasswdEntry(rootfs string, uid, gid int64, logger *slog.Logger) error {
	passwdPath := filepath.Join(rootfs, "etc", "passwd")
	uidStr := strconv.FormatInt(uid, 10)

	if entryExists(passwdPath, uidStr, 2) {
		return nil
	}

	username := fmt.Sprintf("peri-%d", uid)
	home := "/"
	if uid != 0 {
		home = fmt.Sprintf("/home/%s", username)
	}
	line := fmt.Sprintf("%s:x:%d:%d::%s:/bin/sh\n", username, uid, gid, home)

	f, err := os.OpenFile(passwdPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("open %s: %w", passwdPath, err)
	}
	defer f.Close()

	if _, err := f.WriteString(line); err != nil {
		return fmt.Errorf("write passwd entry: %w", err)
	}

	logger.Info("Injected /etc/passwd entry", "uid", uid, "gid", gid, "username", username)

	return nil
}

// injectGroupEntry ensures /etc/group in rootfs contains an entry for the
// given gid. If the GID already exists, no changes are made.
func injectGroupEntry(rootfs string, gid int64, logger *slog.Logger) error {
	groupPath := filepath.Join(rootfs, "etc", "group")
	gidStr := strconv.FormatInt(gid, 10)

	if entryExists(groupPath, gidStr, 2) {
		return nil
	}

	groupName := fmt.Sprintf("peri-%d", gid)
	line := fmt.Sprintf("%s:x:%d:\n", groupName, gid)

	f, err := os.OpenFile(groupPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("open %s: %w", groupPath, err)
	}
	defer f.Close()
	if _, err := f.WriteString(line); err != nil {
		return fmt.Errorf("write group entry: %w", err)
	}
	logger.Info("Injected /etc/group entry", "gid", gid, "group", groupName)
	return nil
}

// createHomeDir creates the home directory for a non-root user inside rootfs.
// Best-effort: failures are logged but non-fatal.
func createHomeDir(rootfs string, uid, gid int64, logger *slog.Logger) {
	if uid == 0 {
		return
	}
	home := filepath.Join(rootfs, "home", fmt.Sprintf("peri-%d", uid))
	if err := os.MkdirAll(home, 0750); err != nil {
		logger.Warn("Failed to create home directory", "path", home, "error", err)
		return
	}
	if err := os.Chown(home, int(uid), int(gid)); err != nil {
		logger.Warn("Failed to chown home directory", "path", home, "error", err)
	}
}

// entryExists checks if a colon-separated file (passwd/group) already has an
// entry where the field at fieldIdx matches value.
func entryExists(path, value string, fieldIdx int) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		fields := strings.SplitN(scanner.Text(), ":", fieldIdx+2)
		if len(fields) > fieldIdx && fields[fieldIdx] == value {
			return true
		}

	}

	return false
}

// prepareUserIdentity injects passwd/group entries and creates a home directory
// for the target UID/GID before container start. Called from both runtime paths.
//
// TODO(overlay-refactor): Remove once prepareBindFiles is the sole code path.
// This function writes into the overlay rootfs which doesn't exist when using
// --overlay= with LayerPaths. prepareBindFiles replaces it with --bind-ro= mounts.
func prepareUserIdentity(rootfs string, runAsUser, runAsGroup *int64, logger *slog.Logger) {
	if runAsUser == nil {
		return
	}

	uid := *runAsUser
	gid := int64(0)
	if runAsGroup != nil {
		gid = *runAsGroup
	}

	if err := injectPasswdEntry(rootfs, uid, gid, logger); err != nil {
		logger.Error("Failed to inject passwd entry", "error", err)
	}

	if err := injectGroupEntry(rootfs, gid, logger); err != nil {
		logger.Error("Failed to inject group entry", "error", err)
	}

	createHomeDir(rootfs, uid, gid, logger)
}

// ---
// Host-side userns shim support (ADR-0010)
//
// The userns-shim binary runs inside the container, calls
// unshare(CLONE_NEWUSER), then waits for perigeos to write uid_map/gid_map
// and send the target uid:gid via a FIFO. This creates the user namespace
// AFTER nspawn has joined the CNI netns, avoiding the --private-users +
// --network-namespace-path incompatibility.
// ---

const (
	// usernsShimHostPath is the install location of the static userns-shim binary.
	usernsShimHostPath = "/usr/local/lib/perigeos/userns-shim"
	// usernsShimContainerPath is where the shim is bind-mounted inside the container.
	usernsShimContainerPath = "/usr/local/bin/userns-shim"
	// usernsFIFOBase is the host-side directory for per-container FIFO dirs.
	usernsFIFOBase = "/run/perigeos/userns"
)

// usernsShimExists returns true if the userns-shim binary is installed.
func usernsShimExists() bool {
	_, err := os.Stat(usernsShimHostPath)
	return err == nil
}

// setupUserNSFIFOs creates a per-container directory containing the ready and
// gate FIFOs used for the userns shim handshake. Returns the host-side path.
func setupUserNSFIFOs(podUID, containerName string) (string, error) {
	dir := filepath.Join(usernsFIFOBase, podUID+"-"+containerName)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return "", fmt.Errorf("mkdir %s: %w", dir, err)
	}

	for _, name := range []string{"ready", "gate"} {
		p := filepath.Join(dir, name)
		os.Remove(p) // remove stale FIFO from previous attempt
		if err := syscall.Mkfifo(p, 0600); err != nil {
			return "", fmt.Errorf("mkfifo %s: %w", p, err)
		}
	}

	return dir, nil
}

// cleanupUserNSFIFOs removes the FIFO directory for a container.
func cleanupUserNSFIFOs(podUID, containerName string) {
	dir := filepath.Join(usernsFIFOBase, podUID+"-"+containerName)
	os.RemoveAll(dir)
}

// completeUserNSSetup runs the host side of the userns shim protocol.
// It blocks on the ready FIFO until the shim has called unshare(CLONE_NEWUSER),
// then writes uid_map/gid_map for the shim process and sends the target
// uid:gid through the gate FIFO. Runs as a goroutine after StartTransientUnit.
func (s *SystemdRuntime) completeUserNSSetup(fifoDir, machineName, podUID string, targetUID, targetGID int64) {
	logger := s.logger.With("machine", machineName, "op", "userns-setup")

	// Step 1: Wait for shim to signal readiness (blocks until unshare is done).
	readyPath := filepath.Join(fifoDir, "ready")
	rf, err := os.Open(readyPath)
	if err != nil {
		logger.Error("Failed to open ready FIFO", "error", err)

		return
	}

	buf := make([]byte, 4)
	if _, err := rf.Read(buf); err != nil {
		rf.Close()
		logger.Error("Failed to read ready FIFO", "error", err)

		return
	}

	rf.Close()
	logger.Info("Shim signaled ready, writing uid_map/gid_map")

	// Step 2: Find the shim's host PID via machined.
	pid, err := s.getMachineLeaderPID(machineName)
	if err != nil {
		logger.Error("Failed to get shim PID", "error", err)

		return
	}

	// Step 3: Write uid_map and gid_map.
	// Map: inside 0-65535 -> host UIDBASE to UIDBASE+65535.
	// The shim (host uid 0) is unmapped (65534) in the new userns until
	// it calls setuid() to adopt the target identity.
	uidbase := computeUIDBASE(podUID)
	mapLine := fmt.Sprintf("0 %d 65536\n", uidbase)

	uidMapPath := fmt.Sprintf("/proc/%d/uid_map", pid)
	if err := os.WriteFile(uidMapPath, []byte(mapLine), 0); err != nil {
		logger.Error("Failed to write uid_map", "path", uidMapPath, "error", err)

		return
	}

	// Deny setgroups inside the userns (defense in depth). The kernel
	// already blocks setgroups() before gid_map is written, but this
	// makes the restriction permanent for child processes too.
	setgroupsPath := fmt.Sprintf("/proc/%d/setgroups", pid)
	if err := os.WriteFile(setgroupsPath, []byte("deny"), 0); err != nil {
		logger.Error("Failed to write setgroups deny", "path", setgroupsPath, "error", err)

		return
	}

	gidMapPath := fmt.Sprintf("/proc/%d/gid_map", pid)
	if err := os.WriteFile(gidMapPath, []byte(mapLine), 0); err != nil {
		logger.Error("Failed to write gid_map", "path", gidMapPath, "error", err)

		return
	}

	logger.Info("Wrote userns mappings", "uidbase", uidbase, "pid", pid)

	// Step 4: Send target uid:gid through gate FIFO - shim will setgid/setuid and exec.
	gatePath := filepath.Join(fifoDir, "gate")
	gf, err := os.OpenFile(gatePath, os.O_WRONLY, 0)
	if err != nil {
		logger.Error("Failed to open gate FIFO", "error", err)

		return
	}
	defer gf.Close()

	payload := fmt.Sprintf("%d:%d\n", targetUID, targetGID)
	if _, err := gf.Write([]byte(payload)); err != nil {
		logger.Error("Failed to write gate FIFO", "error", err)
	}

	logger.Info("Userns setup complete", "targetUID", targetUID, "targetGID", targetGID)
}
