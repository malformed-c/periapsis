package image

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/singleflight"
	"golang.org/x/sys/unix"
)

// layerConcurrency is the maximum number of layers downloaded in parallel.
const layerConcurrency = 8

// ImageManager handles OCI image pulling and overlayfs mounting.
//
// Layer cache and pod workspaces live under baseDir. A single ImageManager
// is shared across all pawns so manifest resolution and layer downloads
// are deduplicated process-wide.
type ImageManager struct {
	baseDir    string
	layerCache string // <baseDir>/layers/
	logger     *slog.Logger
	peers      *PeerConfig  // nil until SetPeers is called
	peerClient *http.Client // shared transport; nil until SetPeers is called

	mu            sync.Mutex
	manifestCache map[string]v1.Image     // image name → resolved manifest
	configCache   map[string]*imageConfig // image name → persisted config (entrypoint/cmd)
	imageSF       singleflight.Group      // deduplicates manifest resolution by image name
	layerSF       singleflight.Group      // deduplicates layer downloads by content hash
}

// imageConfig holds the subset of OCI image config we need across restarts.
type imageConfig struct {
	Entrypoint []string `json:"entrypoint,omitempty"`
	Cmd        []string `json:"cmd,omitempty"`
}

// NewImageManager creates a shared ImageManager.
// baseDir is typically /var/lib/apsis/perigeos.
func NewImageManager(baseDir string, logger *slog.Logger) *ImageManager {
	return &ImageManager{
		baseDir:       baseDir,
		layerCache:    filepath.Join(baseDir, "layers"),
		manifestCache: make(map[string]v1.Image),
		configCache:   make(map[string]*imageConfig),
		logger:        logger,
	}
}

// SweepStaleTmpDirs removes leftover .tmp-* directories in the layer cache
// from interrupted extractions. Call once at startup.
func (im *ImageManager) SweepStaleTmpDirs() {
	entries, err := os.ReadDir(im.layerCache)
	if err != nil {
		return // layer cache may not exist yet
	}
	for _, e := range entries {
		if e.IsDir() && strings.HasPrefix(e.Name(), ".tmp-") {
			path := filepath.Join(im.layerCache, e.Name())
			im.logger.Info("Removing stale tmp layer dir", "path", path)
			os.RemoveAll(path)
		}
	}
}

// GetLayerCachePath returns the shared layer cache path (used for disk stats).
func (im *ImageManager) GetLayerCachePath() string {
	return im.layerCache
}

// ImageEntrypoint returns the OCI image's Entrypoint and Cmd.
// Checks the in-memory manifest cache first, then falls back to the
// disk-persisted config cache (survives process restarts).
func (im *ImageManager) ImageEntrypoint(imageName string) (entrypoint, cmd []string) {
	im.mu.Lock()
	img, ok := im.manifestCache[imageName]
	if !ok {
		// Fall back to persisted config cache.
		if cfg, cached := im.configCache[imageName]; cached {
			im.mu.Unlock()
			return cfg.Entrypoint, cfg.Cmd
		}
		// Try loading from disk.
		if cfg, err := im.loadImageConfig(imageName); err == nil {
			im.configCache[imageName] = cfg
			im.mu.Unlock()
			return cfg.Entrypoint, cfg.Cmd
		}
		im.mu.Unlock()
		return nil, nil
	}
	im.mu.Unlock()
	cf, err := img.ConfigFile()
	if err != nil {
		return nil, nil
	}
	return cf.Config.Entrypoint, cf.Config.Cmd
}

// Pull ensures all OCI image layers are extracted to disk.
// Returns ordered layer paths (bottom → top) ready for overlayfs lowerdir.
//
// pullPolicy follows Kubernetes semantics:
//   - "Always"       — always resolve the manifest from the registry (default)
//   - "IfNotPresent" — skip the pull if a cached manifest exists locally
//   - "Never"        — fail if no cached manifest exists
//
// When pullPolicy is empty it defaults to "Always".
func (im *ImageManager) Pull(imageName string, pullPolicy string) ([]string, bool, error) {
	return im.PullWithProgress(imageName, pullPolicy, nil)
}

// PullWithProgress is like Pull but calls progress after each layer completes.
// Returns (layerPaths, cached, error). cached is true when layers were served
// entirely from local cache without a registry fetch.
func (im *ImageManager) PullWithProgress(imageName string, pullPolicy string, progress PullProgress) ([]string, bool, error) {
	if pullPolicy == "" {
		pullPolicy = "Always"
	}

	cacheKey := "manifest:" + imageName

	// IfNotPresent / Never: try in-memory cache, then disk cache.
	if pullPolicy == "IfNotPresent" || pullPolicy == "Never" {
		im.mu.Lock()
		cached, ok := im.manifestCache[imageName]
		im.mu.Unlock()
		if ok {
			paths, err := im.layersFromImage(cached, progress)
			return paths, err == nil, err
		}
		// Check disk-persisted layer cache (survives process restart).
		if paths, err := im.loadLayerCache(imageName); err == nil {
			return paths, true, nil
		}
		if pullPolicy == "Never" {
			return nil, false, fmt.Errorf("image %s not in cache and pullPolicy=Never", imageName)
		}
	}

	manifestObj, err, _ := im.imageSF.Do(cacheKey, func() (interface{}, error) {
		im.logger.Info("Resolving image manifest", "image", imageName)
		ref, err := name.ParseReference(imageName)
		if err != nil {
			return nil, fmt.Errorf("failed to parse reference: %w", err)
		}
		img, err := remote.Image(ref, remote.WithAuthFromKeychain(authn.DefaultKeychain))
		if err != nil {
			return nil, err
		}
		return img, nil
	})
	if err != nil {
		// Always policy: fall back to cached manifest on transient errors
		// (rate limits, network issues) so retries don't fail unnecessarily.
		im.mu.Lock()
		cached, ok := im.manifestCache[imageName]
		im.mu.Unlock()
		if ok {
			im.logger.Info("Registry unavailable, using cached manifest", "image", imageName, "err", err)
			paths, layerErr := im.layersFromImage(cached, progress)
			return paths, layerErr == nil, layerErr
		}
		// Try disk-persisted layer cache (survives process restart).
		if paths, diskErr := im.loadLayerCache(imageName); diskErr == nil {
			im.logger.Info("Registry unavailable, using disk-cached layers", "image", imageName, "err", err)
			return paths, true, nil
		}
		return nil, false, fmt.Errorf("failed to pull manifest: %w", err)
	}

	img := manifestObj.(v1.Image)

	im.mu.Lock()
	im.manifestCache[imageName] = img
	im.mu.Unlock()

	layerPaths, err := im.layersFromImage(img, progress)
	if err != nil {
		return nil, false, err
	}

	// Persist layer list and image config to disk so they survive process restart.
	im.saveLayerCache(imageName, layerPaths)
	if cf, err := img.ConfigFile(); err == nil {
		im.saveImageConfig(imageName, &imageConfig{
			Entrypoint: cf.Config.Entrypoint,
			Cmd:        cf.Config.Cmd,
		})
	}

	return layerPaths, false, nil
}

// PullProgress is called after each layer is resolved.
// done is the number of layers completed, total is the total layer count.
type PullProgress func(done, total int)

// layersFromImage extracts and caches all layers from a resolved image.
// Layers are downloaded in parallel (up to layerConcurrency at once).
// If progress is non-nil it is called after each layer completes.
func (im *ImageManager) layersFromImage(img v1.Image, progress PullProgress) ([]string, error) {
	layers, err := img.Layers()
	if err != nil {
		return nil, fmt.Errorf("failed to get layers: %w", err)
	}

	total := len(layers)
	layerPaths := make([]string, total)
	var doneCount atomic.Int32

	g := new(errgroup.Group)
	g.SetLimit(layerConcurrency)

	for i, layer := range layers {
		i, layer := i, layer
		g.Go(func() error {
			diffID, err := layer.DiffID()
			if err != nil {
				return err
			}
			pathIface, err, _ := im.layerSF.Do(diffID.Hex, func() (interface{}, error) {
				return im.ensureLayer(diffID.Hex, layer)
			})
			if err != nil {
				return err
			}
			layerPaths[i] = pathIface.(string)
			if progress != nil {
				progress(int(doneCount.Add(1)), total)
			}
			return nil
		})
	}

	return layerPaths, g.Wait()
}

// layerCacheFile returns the path for a disk-persisted layer list.
func (im *ImageManager) layerCacheFile(imageName string) string {
	// Escape slashes and colons so the image name is a valid filename.
	safe := strings.NewReplacer("/", "_", ":", "_").Replace(imageName)
	return filepath.Join(im.layerCache, ".manifests", safe+".json")
}

// imageConfigFile returns the path for a disk-persisted image config.
func (im *ImageManager) imageConfigFile(imageName string) string {
	safe := strings.NewReplacer("/", "_", ":", "_").Replace(imageName)
	return filepath.Join(im.layerCache, ".manifests", safe+".config.json")
}

// saveImageConfig persists an image's Entrypoint and Cmd to disk.
func (im *ImageManager) saveImageConfig(imageName string, cfg *imageConfig) {
	path := im.imageConfigFile(imageName)
	os.MkdirAll(filepath.Dir(path), 0755)
	data, _ := json.Marshal(cfg)
	os.WriteFile(path, data, 0644)
}

// loadImageConfig loads a previously persisted image config from disk.
func (im *ImageManager) loadImageConfig(imageName string) (*imageConfig, error) {
	data, err := os.ReadFile(im.imageConfigFile(imageName))
	if err != nil {
		return nil, err
	}
	var cfg imageConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// saveLayerCache persists the ordered layer paths for an image to disk.
func (im *ImageManager) saveLayerCache(imageName string, layerPaths []string) {
	path := im.layerCacheFile(imageName)
	os.MkdirAll(filepath.Dir(path), 0755)
	data, _ := json.Marshal(layerPaths)
	os.WriteFile(path, data, 0644)
}

// loadLayerCache loads a previously persisted layer list and verifies all
// layer directories still exist on disk.
func (im *ImageManager) loadLayerCache(imageName string) ([]string, error) {
	data, err := os.ReadFile(im.layerCacheFile(imageName))
	if err != nil {
		return nil, err
	}
	var paths []string
	if err := json.Unmarshal(data, &paths); err != nil {
		return nil, err
	}
	// Verify all layers still exist on disk.
	for _, p := range paths {
		if _, err := os.Stat(p); err != nil {
			return nil, fmt.Errorf("cached layer missing: %s", p)
		}
	}
	return paths, nil
}

// blobPath returns the path for the cached compressed blob tarball.
func (im *ImageManager) blobPath(hash string) string {
	return filepath.Join(im.layerCache, hash+".tar.gz")
}

// BlobPath is the exported form for the server.blobProvider interface.
func (im *ImageManager) BlobPath(hash string) string {
	return im.blobPath(hash)
}

// ensureLayer ensures a layer is extracted to {layerCache}/{hash}/.
// Pull order:
//  1. Already extracted — return immediately.
//  2. Blob file exists locally — extract from it (handles mid-extraction crashes).
//  3. Peer node has blob — fetch compressed stream, tee to local blob file + extract.
//  4. Upstream registry — fetch compressed stream, tee to local blob file + extract.
func (im *ImageManager) ensureLayer(hash string, layer v1.Layer) (string, error) {
	destPath := filepath.Join(im.layerCache, hash)

	// 1. Already extracted.
	if _, err := os.Stat(destPath); err == nil {
		abs, _ := filepath.Abs(destPath)
		return abs, nil
	}

	tmpPath := filepath.Join(im.layerCache, fmt.Sprintf(".tmp-%s-%d", hash, time.Now().UnixNano()))
	if err := os.MkdirAll(tmpPath, 0755); err != nil {
		return "", err
	}

	blobFile := im.blobPath(hash)

	// 2. Local blob file exists — extract from it.
	if _, err := os.Stat(blobFile); err == nil {
		im.logger.Info("Extracting layer from local blob", "hash", hash)
		if err := extractCompressedBlob(blobFile, tmpPath); err == nil {
			return commitLayer(tmpPath, destPath)
		}
		// Corrupt blob — remove and fall through.
		os.Remove(blobFile)
		os.RemoveAll(tmpPath)
		if err := os.MkdirAll(tmpPath, 0755); err != nil {
			return "", err
		}
	}

	// 3. Try a peer node.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	if peerBody, ok := im.fetchFromPeers(ctx, hash); ok {
		im.logger.Info("Pulling layer from peer", "hash", hash)
		err := saveAndExtract(peerBody, blobFile, tmpPath)
		peerBody.Close()
		if err == nil {
			return commitLayer(tmpPath, destPath)
		}
		im.logger.Warn("Peer layer fetch failed, falling back to upstream", "hash", hash, "err", err)
		os.Remove(blobFile)
		os.RemoveAll(tmpPath)
		if err := os.MkdirAll(tmpPath, 0755); err != nil {
			return "", err
		}
	}

	// 4. Upstream registry — retry up to 3 times on stall/error.
	const maxAttempts = 3
	const registryMinRate = 512  // bytes/sec — below this after warmup = stalled
	const registryWarmup = 30 * time.Second
	var lastErr error
	for attempt := range maxAttempts {
		if attempt > 0 {
			im.logger.Warn("Retrying upstream layer fetch", "hash", hash, "attempt", attempt+1, "err", lastErr)
			os.Remove(blobFile)
			os.RemoveAll(tmpPath)
			if err := os.MkdirAll(tmpPath, 0755); err != nil {
				return "", err
			}
			time.Sleep(time.Duration(attempt) * 3 * time.Second)
		}

		im.logger.Info("Pulling layer from upstream", "hash", hash)
		rc, err := layer.Compressed()
		if err != nil {
			lastErr = err
			continue
		}
		guarded := &rateGuard{ReadCloser: rc, started: time.Now(), minRate: registryMinRate, warmup: registryWarmup}
		lastErr = saveAndExtract(guarded, blobFile, tmpPath)
		rc.Close()
		if lastErr == nil {
			return commitLayer(tmpPath, destPath)
		}
	}

	os.RemoveAll(tmpPath)
	return "", fmt.Errorf("upstream layer fetch failed after %d attempts: %w", maxAttempts, lastErr)
}

// saveAndExtract reads a gzip-compressed tar stream, writes it to blobFile,
// and simultaneously decompresses+extracts it into dst.
func saveAndExtract(compressedStream io.Reader, blobFile, dst string) error {
	tmpBlob := blobFile + ".tmp"
	bf, err := os.Create(tmpBlob)
	if err != nil {
		return err
	}

	teeR := io.TeeReader(compressedStream, bf)
	gz, err := gzip.NewReader(teeR)
	if err != nil {
		bf.Close()
		os.Remove(tmpBlob)
		return err
	}

	extractErr := extractLayer(dst, tar.NewReader(gz))
	gz.Close()
	bf.Close()

	if extractErr != nil {
		os.Remove(tmpBlob)
		return extractErr
	}

	return os.Rename(tmpBlob, blobFile)
}

// extractCompressedBlob opens a .tar.gz file and extracts it into dst.
func extractCompressedBlob(blobFile, dst string) error {
	f, err := os.Open(blobFile)
	if err != nil {
		return err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	defer gz.Close()
	return extractLayer(dst, tar.NewReader(gz))
}

// rateGuard wraps a ReadCloser and returns an error if the average download
// rate drops below minRate bytes/sec after the warmup period.
// Checked on every Read call so no extra goroutines are needed.
type rateGuard struct {
	io.ReadCloser
	started time.Time
	total   int64
	minRate int64
	warmup  time.Duration
}

func (r *rateGuard) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	r.total += int64(n)
	if err == nil && time.Since(r.started) > r.warmup && r.total > 0 {
		elapsed := time.Since(r.started).Seconds()
		if int64(float64(r.total)/elapsed) < r.minRate {
			return n, fmt.Errorf("download stalled: %.0f B/s (min %d B/s)", float64(r.total)/elapsed, r.minRate)
		}
	}
	return n, err
}

// commitLayer atomically renames tmpPath to destPath.
func commitLayer(tmpPath, destPath string) (string, error) {
	if _, err := os.Stat(destPath); err == nil {
		os.RemoveAll(tmpPath)
		abs, _ := filepath.Abs(destPath)
		return abs, nil
	}
	if err := os.Rename(tmpPath, destPath); err != nil {
		os.RemoveAll(tmpPath)
		return "", fmt.Errorf("layer commit failed: %w", err)
	}
	abs, _ := filepath.Abs(destPath)
	return abs, nil
}

// Mount creates an overlayfs for a pod using the given ordered layer paths.
// Returns the absolute path to the merged directory (the container's rootfs view).
//
// Layout under <baseDir>/pods/<podUID>/:
//   rootfs/  — merged (container's view)
//   upper/   — writable layer
//   work/    — overlayfs scratch space
func (im *ImageManager) Mount(podUID string, layerPaths []string) (string, error) {
	base, err := filepath.Abs(filepath.Join(im.baseDir, "pods", podUID))
	if err != nil {
		return "", fmt.Errorf("failed to get absolute path: %w", err)
	}

	merged := filepath.Join(base, "rootfs")
	upper := filepath.Join(base, "upper")
	work := filepath.Join(base, "work")

	// Clean up any previous attempt for idempotency
	os.RemoveAll(base)

	for _, dir := range []string{merged, upper, work} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return "", fmt.Errorf("mkdir %s: %w", dir, err)
		}
	}

	// OverlayFS requires lowerdir order: top_layer:...:bottom_layer
	// Our Pull() returns bottom→top, so reverse before joining
	reversed := make([]string, len(layerPaths))
	copy(reversed, layerPaths)
	slices.Reverse(reversed)

	// Deduplicate (some images share layers)
	seen := make(map[string]bool)
	var lowerDirs []string
	for _, p := range reversed {
		if !seen[p] {
			lowerDirs = append(lowerDirs, p)
			seen[p] = true
		}
	}

	opts := fmt.Sprintf("lowerdir=%s,upperdir=%s,workdir=%s,index=off",
		strings.Join(lowerDirs, ":"), upper, work)

	if err := unix.Mount("overlay", merged, "overlay", unix.MS_NODEV, opts); err != nil {
		return "", fmt.Errorf("overlay mount failed: %w", err)
	}

	// Ensure os-release exists so systemd-nspawn doesn't refuse to start
	if err := im.ensureOSRelease(merged); err != nil {
		_ = unix.Unmount(merged, unix.MNT_DETACH)
		return "", err
	}

	return merged, nil
}

// ensureOSRelease injects a minimal os-release file for distroless/scratch images
// that don't have one, satisfying systemd-nspawn's OS check.
func (im *ImageManager) ensureOSRelease(merged string) error {
	checkPaths := []string{"/etc/os-release", "/usr/lib/os-release"}
	for _, p := range checkPaths {
		if _, err := os.Stat(filepath.Join(merged, p)); err == nil {
			return nil // already present
		}
	}

	im.logger.Info("Injecting synthetic os-release for distroless image")

	usrLib := filepath.Join(merged, "usr", "lib")
	if err := os.MkdirAll(usrLib, 0755); err != nil {
		return err
	}

	f, err := os.Create(filepath.Join(usrLib, "os-release"))
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.WriteString("NAME=Periapsis\nID=periapsis\nPRETTY_NAME=\"Periapsis Pawn\"\n")
	return err
}

// Unmount lazily unmounts the overlayfs and removes the pod's workspace directory.
func (im *ImageManager) Unmount(podUID string) error {
	base, err := filepath.Abs(filepath.Join(im.baseDir, "pods", podUID))
	if err != nil {
		return fmt.Errorf("failed to get absolute path: %w", err)
	}

	target := filepath.Join(base, "rootfs")

	if _, err := os.Stat(target); os.IsNotExist(err) {
		im.logger.Info("Unmount skipped, target does not exist", "path", target)
		_ = os.RemoveAll(base)
		return nil
	}

	// Unmount in a loop: overlayfs can be stacked multiple times (e.g. from
	// container restarts without proper cleanup). MNT_DETACH removes one
	// mount per call, so we loop until EINVAL (no mount remaining).
	for {
		if err := unix.Unmount(target, unix.MNT_DETACH); err != nil {
			if errors.Is(err, unix.EINVAL) {
				break // no more mounts
			}
			return fmt.Errorf("unmount %s: %w", target, err)
		}
	}

	// After a lazy unmount the overlay upper/work dirs may briefly remain
	// busy while the kernel tears down references. Retry RemoveAll a few
	// times before giving up.
	var rmErr error
	for range 5 {
		if rmErr = os.RemoveAll(base); rmErr == nil {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return rmErr
}
