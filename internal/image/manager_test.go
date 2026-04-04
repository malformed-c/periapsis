package image

import (
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestSweepStaleTmpDirs(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sweep-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	im := NewImageManager(tmpDir, slog.Default())
	os.MkdirAll(im.layerCache, 0755)

	staleDir := filepath.Join(im.layerCache, ".tmp-hash-123")
	okDir := filepath.Join(im.layerCache, "abcdef123456")

	if err := os.Mkdir(staleDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(okDir, 0755); err != nil {
		t.Fatal(err)
	}

	im.SweepStaleTmpDirs()

	if _, err := os.Stat(staleDir); !os.IsNotExist(err) {
		t.Error("expected stale tmp dir to be removed")
	}
	if _, err := os.Stat(okDir); err != nil {
		t.Error("expected ok dir to be kept")
	}
}

func TestImageEntrypoint(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "entrypoint-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	im := NewImageManager(tmpDir, slog.Default())

	imageName := "test-image"
	cfg := &imageConfig{
		Entrypoint: []string{"/app"},
		Cmd:        []string{"--run"},
	}

	// Test persistence and loading
	im.saveImageConfig(imageName, cfg)

	// Clear in-memory cache to force disk load
	im.configCache = make(map[string]*imageConfig)

	entrypoint, cmd := im.ImageEntrypoint(imageName)
	if !reflect.DeepEqual(entrypoint, cfg.Entrypoint) {
		t.Errorf("expected entrypoint %v, got %v", cfg.Entrypoint, entrypoint)
	}
	if !reflect.DeepEqual(cmd, cfg.Cmd) {
		t.Errorf("expected cmd %v, got %v", cfg.Cmd, cmd)
	}
}

func TestLayerCachePersistence(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "layercache-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	im := NewImageManager(tmpDir, slog.Default())

	imageName := "test-image"
	layers := []string{
		filepath.Join(im.layerCache, "layer1"),
		filepath.Join(im.layerCache, "layer2"),
	}

	// Create dummy layers on disk
	for _, l := range layers {
		if err := os.MkdirAll(l, 0755); err != nil {
			t.Fatal(err)
		}
	}

	im.saveLayerCache(imageName, layers)

	loaded, err := im.loadLayerCache(imageName)
	if err != nil {
		t.Fatalf("loadLayerCache failed: %v", err)
	}
	if !reflect.DeepEqual(loaded, layers) {
		t.Errorf("expected layers %v, got %v", layers, loaded)
	}

	// Test missing layer
	os.RemoveAll(layers[0])
	_, err = im.loadLayerCache(imageName)
	if err == nil {
		t.Error("expected error when layer is missing from disk")
	}
}

func TestEnsureOSRelease(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "os-release-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	im := NewImageManager(tmpDir, slog.Default())

	// Test injection
	if err := im.ensureOSRelease(tmpDir); err != nil {
		t.Fatalf("ensureOSRelease failed: %v", err)
	}

	content, err := os.ReadFile(filepath.Join(tmpDir, "usr/lib/os-release"))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(string(content), "NAME=Periapsis\nID=periapsis\nPRETTY_NAME=\"Periapsis Pawn\"\n") {
		t.Errorf("unexpected os-release content: %q", string(content))
	}

	// Test idempotency (should not overwrite or fail if already exists)
	if err := im.ensureOSRelease(tmpDir); err != nil {
		t.Fatalf("second ensureOSRelease failed: %v", err)
	}
}
