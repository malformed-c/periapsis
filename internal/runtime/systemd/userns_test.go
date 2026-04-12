package systemd

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestComputeUIDBASE(t *testing.T) {
	base := computeUIDBASE("test-pod-uid")
	if base%65536 != 0 {
		t.Fatalf("UIDBASE %d is not a multiple of 65536", base)
	}
	if base < 65536*2 {
		t.Fatalf("UIDBASE %d is below minimum slot 2", base)
	}
	if base >= 65536*(2+uidbaseSlots) {
		t.Fatalf("UIDBASE %d exceeds max slot", base)
	}

	// Deterministic: same input → same output.
	if got := computeUIDBASE("test-pod-uid"); got != base {
		t.Fatalf("non-deterministic: got %d then %d", base, got)
	}

	// Different input → likely different output (not guaranteed but extremely likely).
	other := computeUIDBASE("other-pod-uid")
	if other == base {
		t.Log("hash collision (acceptable but unlikely)")
	}
}

func TestInjectPasswdEntry(t *testing.T) {
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "etc"), 0755)
	os.WriteFile(filepath.Join(dir, "etc", "passwd"), []byte("root:x:0:0:root:/root:/bin/sh\n"), 0644)

	logger := discardLogger()

	if err := injectPasswdEntry(dir, 1000, 1000, logger); err != nil {
		t.Fatal(err)
	}

	data, _ := os.ReadFile(filepath.Join(dir, "etc", "passwd"))
	if got := string(data); !contains(got, "peri-1000:x:1000:1000::/home/peri-1000:/bin/sh") {
		t.Fatalf("passwd missing injected entry:\n%s", got)
	}

	// Idempotent: second call should not duplicate.
	if err := injectPasswdEntry(dir, 1000, 1000, logger); err != nil {
		t.Fatal(err)
	}
	data2, _ := os.ReadFile(filepath.Join(dir, "etc", "passwd"))
	if len(data2) != len(data) {
		t.Fatal("duplicate entry injected")
	}
}

func TestInjectPasswdEntry_ExistingUID(t *testing.T) {
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "etc"), 0755)
	os.WriteFile(filepath.Join(dir, "etc", "passwd"),
		[]byte("root:x:0:0:root:/root:/bin/sh\nnginx:x:101:101:nginx:/var/cache/nginx:/sbin/nologin\n"), 0644)

	logger := discardLogger()
	before, _ := os.ReadFile(filepath.Join(dir, "etc", "passwd"))

	if err := injectPasswdEntry(dir, 101, 101, logger); err != nil {
		t.Fatal(err)
	}

	after, _ := os.ReadFile(filepath.Join(dir, "etc", "passwd"))
	if string(after) != string(before) {
		t.Fatal("should not modify passwd when UID already exists")
	}
}

func TestInjectGroupEntry(t *testing.T) {
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "etc"), 0755)
	os.WriteFile(filepath.Join(dir, "etc", "group"), []byte("root:x:0:\n"), 0644)

	logger := discardLogger()

	if err := injectGroupEntry(dir, 1000, logger); err != nil {
		t.Fatal(err)
	}

	data, _ := os.ReadFile(filepath.Join(dir, "etc", "group"))
	if got := string(data); !contains(got, "peri-1000:x:1000:") {
		t.Fatalf("group missing injected entry:\n%s", got)
	}
}

func TestCreateHomeDir(t *testing.T) {
	dir := t.TempDir()
	logger := discardLogger()

	createHomeDir(dir, 1000, 1000, logger)
	home := filepath.Join(dir, "home", "peri-1000")
	if _, err := os.Stat(home); err != nil {
		t.Fatalf("home dir not created: %v", err)
	}

	// Root should not create home.
	createHomeDir(dir, 0, 0, logger)
	if _, err := os.Stat(filepath.Join(dir, "home", "peri-0")); err == nil {
		t.Fatal("should not create home for root")
	}
}

func TestPrepareUserIdentity_NilUser(t *testing.T) {
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "etc"), 0755)
	os.WriteFile(filepath.Join(dir, "etc", "passwd"), []byte("root:x:0:0:root:/root:/bin/sh\n"), 0644)

	logger := discardLogger()
	before, _ := os.ReadFile(filepath.Join(dir, "etc", "passwd"))

	prepareUserIdentity(dir, nil, nil, logger)

	after, _ := os.ReadFile(filepath.Join(dir, "etc", "passwd"))
	if string(after) != string(before) {
		t.Fatal("should not modify files when RunAsUser is nil")
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(sub) == 0 ||
		(len(s) > 0 && len(sub) > 0 && searchString(s, sub)))
}

func searchString(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
