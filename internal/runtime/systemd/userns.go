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
