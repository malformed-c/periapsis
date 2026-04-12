package systemd

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
)

func TestIsMuslRootFS(t *testing.T) {
	t.Run("musl detected", func(t *testing.T) {
		dir := t.TempDir()
		libDir := filepath.Join(dir, "lib")
		os.MkdirAll(libDir, 0755)
		os.WriteFile(filepath.Join(libDir, "ld-musl-x86_64.so.1"), nil, 0755)
		if !isMuslRootFS(dir) {
			t.Fatal("expected musl to be detected")
		}
	})

	t.Run("glibc not detected", func(t *testing.T) {
		dir := t.TempDir()
		libDir := filepath.Join(dir, "lib")
		os.MkdirAll(libDir, 0755)
		os.WriteFile(filepath.Join(libDir, "ld-linux-x86-64.so.2"), nil, 0755)
		if isMuslRootFS(dir) {
			t.Fatal("expected musl NOT to be detected for glibc rootfs")
		}
	})

	t.Run("empty rootfs", func(t *testing.T) {
		dir := t.TempDir()
		if isMuslRootFS(dir) {
			t.Fatal("expected musl NOT to be detected for empty rootfs")
		}
	})
}

func TestInstallGetentShim(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	if err := installGetentShim(dir, logger); err != nil {
		t.Fatalf("installGetentShim: %v", err)
	}

	shimPath := filepath.Join(dir, "usr", "local", "bin", "getent")
	data, err := os.ReadFile(shimPath)
	if err != nil {
		t.Fatalf("read shim: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("shim is empty")
	}

	info, _ := os.Stat(shimPath)
	if info.Mode().Perm()&0111 == 0 {
		t.Fatal("shim is not executable")
	}
}
