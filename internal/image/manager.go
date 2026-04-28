// Copyright (C) 2025-2026 Malformed C. All rights reserved.
// SPDX-License-Identifier: BUSL-1.1

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
	manifestCache map[string]v1.Image     // image name -> resolved manifest
	configCache   map[string]*imageConfig // image name -> persisted config (entrypoint/cmd)
	imageSF       singleflight.Group      // deduplicates manifest resolution by image name
	layerSF       singleflight.Group      // deduplicates layer downloads by content hash

	// inflightLayers tracks layer hashes currently being pulled by this host.
	// Value type is chan struct{} - closed when the pull completes.
	// Exposed via /blobs/inflight so peers can discover and wait on our pulls
	// instead of independently downloading the same layer from upstream.
	inflightLayers sync.Map // hash -> chan struct{}

	// selfMarker is a random token registered as a permanent entry in
	// inflightLayers. When peersWithInflight queries a peer and sees this
	// marker in the response, it knows the "peer" is actually this same
	// perigeos process - belt-and-braces against a misconfigured host
	// filter sending us into a wait-on-self deadlock.
	selfMarker   string
	knownSelfEps sync.Map // ep -> true, cached after first detection
}

// imageConfig holds the subset of OCI image config we need across restarts.
type imageConfig struct {
	Entrypoint []string `json:"entrypoint,omitempty"`
	Cmd        []string `json:"cmd,omitempty"`
}

// For caching
type manifestResult struct {
	img         v1.Image
	indexDigest string
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

// CachedImage describes a locally cached OCI image.
type CachedImage struct {
	Name       string   // full image reference
	Digest     string   // manifest digest ("" if not yet cached)
	Layers     int      // number of layers
	LayerPaths []string // absolute paths to extracted layer dirs
	SizeBytes  int64    // total on-disk size of all extracted layers + blob files
}

// ListCachedImages returns metadata for every image whose layer list is
// persisted on disk. The list survives process restarts.
func (im *ImageManager) ListCachedImages() ([]CachedImage, error) {
	manifestDir := filepath.Join(im.layerCache, ".manifests")
	entries, err := os.ReadDir(manifestDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}

		return nil, fmt.Errorf("read manifest dir: %w", err)
	}

	var images []CachedImage
	seen := make(map[string]bool)

	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}

		// .json files are layer caches; .config.json and .digest are siblings.
		// Derive the image name by reversing the safe-name encoding.
		safe := strings.TrimSuffix(e.Name(), ".json")
		if strings.HasSuffix(safe, ".config") {
			continue // skip .config.json files
		}

		// Only replace the underscores that were actually slashes.
		// Since we kept colons, we can just replace underscores with slashes.
		// TODO: Make a helper and maybe adopt systemd's escaping
		imageName := strings.ReplaceAll(safe, "_", "/")

		// Restore the tag separator: the last "_" before a tag is actually ":"
		// e.g. "library_nginx_latest" -> "library/nginx:latest" isn't fully
		// recoverable without the original, so we store the safe name as-is
		// and show it. This is cosmetic - the actual data is in the paths.
		if seen[imageName] {
			continue
		}

		seen[imageName] = true

		paths, err := im.loadLayerCache(imageName)
		if err != nil {
			// Try with the raw safe name (has colons encoded as _)
			continue
		}

		var totalSize int64
		for _, p := range paths {
			if info, err := dirSize(p); err == nil {
				totalSize += info
			}

			// Also count the blob file if present.
			blobFile := im.blobPath(filepath.Base(p))
			if fi, err := os.Stat(blobFile); err == nil {
				totalSize += fi.Size()
			}
		}

		digest := im.loadManifestDigest(imageName)

		images = append(images, CachedImage{
			Name:       imageName,
			Digest:     digest,
			Layers:     len(paths),
			LayerPaths: paths,
			SizeBytes:  totalSize,
		})
	}

	return images, nil
}

func (im *ImageManager) ListCachedImagesJSON() []map[string]any {
	images, _ := im.ListCachedImages()
	out := make([]map[string]any, len(images))
	for i, img := range images {
		out[i] = map[string]any{
			"name": img.Name, "digest": img.Digest,
			"layers": img.Layers, "size_bytes": img.SizeBytes,
		}
	}

	return out
}

// dirSize returns the total size of all files under dir.
func dirSize(dir string) (int64, error) {
	var size int64
	err := filepath.Walk(dir, func(_ string, fi os.FileInfo, err error) error {
		if err != nil {
			return nil // skip unreadable files
		}

		if !fi.IsDir() {
			size += fi.Size()
		}

		return nil
	})

	return size, err
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
// Returns ordered layer paths (bottom -> top) ready for overlayfs lowerdir.
//
// pullPolicy follows Kubernetes semantics:
//   - "Always"       - always resolve the manifest from the registry (default)
//   - "IfNotPresent" - skip the pull if a cached manifest exists locally
//   - "Never"        - fail if no cached manifest exists
//
// When pullPolicy is empty it defaults to "Always".

// PullWithOptions accepts an event callback for
// notable layer events (peer hit, stall, registry retry).
func (im *ImageManager) PullWithOptions(imageName string, pullPolicy string, opts PullOptions) ([]string, bool, error) {
	if pullPolicy == "" {
		pullPolicy = "Always"
	}

	eventFn := opts.Event

	cacheKey := "manifest:" + imageName

	// IfNotPresent / Never: try in-memory cache, then disk cache.
	if pullPolicy == "IfNotPresent" || pullPolicy == "Never" {
		im.mu.Lock()
		cached, ok := im.manifestCache[imageName]
		im.mu.Unlock()

		if ok {
			// Get paths without triggering progress events
			paths, err := im.loadLayerCache(imageName)
			if err == nil {
				if eventFn != nil {
					eventFn("Normal", "ImageCached", fmt.Sprintf("Image %s is in cache (memory hit)", imageName))
				}

				return paths, true, nil
			}

			// Fallback if disk is gone but memory is there
			paths, err = im.layersFromImage(cached, opts)
			if eventFn != nil {
				eventFn("Normal", "ImageCached", fmt.Sprintf("Image %s is in cache", imageName))
			}

			return paths, err == nil, err
		}

		// Check disk-persisted layer cache (survives process restart).
		if paths, err := im.loadLayerCache(imageName); err == nil {
			if eventFn != nil {
				eventFn("Normal", "ImageCached", fmt.Sprintf("Image %s is in cache", imageName))
			}

			return paths, true, nil
		}

		if pullPolicy == "Never" {
			if eventFn != nil {
				eventFn("Error", "ImageMissing", fmt.Sprintf("Image %s not in cache and pullPolicy=Never", imageName))
			}

			return nil, false, fmt.Errorf("image %s not in cache and pullPolicy=Never", imageName)
		}
	}

	// Always policy: Check the Index Digest against the .index file
	// resolve the remote manifest digest first (cheap HEAD/GET).
	// If the remote digest matches the cached one, all layers are current -
	// return the disk cache without re-downloading anything.
	// This gives correct "always check for updates" semantics while avoiding
	// redundant layer downloads when the image hasn't changed.
	if cachedPaths, cacheErr := im.loadLayerCache(imageName); cacheErr == nil {
		cachedIndex := im.loadIndexDigest(imageName)
		if cachedIndex != "" {
			if remoteDigest, digestErr := im.resolveManifestDigest(imageName); digestErr == nil {
				if remoteDigest == cachedIndex {
					im.logger.Debug("Always: remote index digest matches cache, skipping pull",
						"image", imageName, "Index digest", remoteDigest[:16])

					if eventFn != nil {
						eventFn("Normal", "ImageCached", fmt.Sprintf("Image %s is up to date (Index digest %s...)", imageName, remoteDigest[:16]))
					}

					return cachedPaths, true, nil
				}

				im.logger.Info("Always: remote Index digest changed, re-pulling",
					"image", imageName, "cached", cachedIndex[:16], "remote", remoteDigest[:16])
			}

			// digest fetch failed (offline, rate-limited) - fall through to full manifest pull
		}

		// No cached digest yet (first pull was before this feature) - fall through
	}

	manifestObj, err, _ := im.imageSF.Do(cacheKey, func() (any, error) {
		im.logger.Info("Resolving image manifest", "image", imageName)
		if eventFn != nil {
			eventFn("Normal", "ResolvingManifest", fmt.Sprintf("Resolving image manifest: %s", imageName))
		}

		ref, err := name.ParseReference(imageName)
		if err != nil {
			if eventFn != nil {
				eventFn("Error", "ResolvingManifestFailed", "Failed to parse reference")
			}

			return nil, fmt.Errorf("failed to parse reference: %w", err)
		}

		// Grab the Index Digest (Ignore error here, some registries don't support HEAD)
		indexDigest := ""
		if desc, err := remote.Head(ref, remote.WithAuthFromKeychain(authn.DefaultKeychain)); err == nil {
			indexDigest = desc.Digest.String()
		}

		img, err := remote.Image(ref, remote.WithAuthFromKeychain(authn.DefaultKeychain))
		if err != nil {
			return nil, err
		}

		return manifestResult{img: img, indexDigest: indexDigest}, nil
	})
	if err != nil {
		// Always policy: fall back to cached manifest on transient errors
		// (rate limits, network issues) so retries don't fail unnecessarily.
		im.mu.Lock()
		cached, ok := im.manifestCache[imageName]
		im.mu.Unlock()

		if ok {
			im.logger.Info("Registry unavailable, using cached manifest", "image", imageName, "err", err)
			paths, layerErr := im.layersFromImage(cached, opts)

			return paths, layerErr == nil, layerErr
		}

		// Try disk-persisted layer cache
		if paths, diskErr := im.loadLayerCache(imageName); diskErr == nil {
			im.logger.Info("Registry unavailable, using disk-cached layers", "image", imageName, "err", err)
			if eventFn != nil {
				eventFn("Warning", "RegistryFailed", "Registry unavailable, using disk-cached layers")
			}

			return paths, true, nil
		}

		if eventFn != nil {
			eventFn("Error", "ResolvingManifestFailed", "Failed to pull manifest")
		}

		return nil, false, fmt.Errorf("failed to pull manifest: %w", err)
	}

	// Extract our results
	res := manifestObj.(manifestResult)
	img := res.img

	im.mu.Lock()
	im.manifestCache[imageName] = img
	im.mu.Unlock()

	layerPaths, err := im.layersFromImage(img, opts)
	if err != nil {
		return nil, false, err
	}

	// Persist layer list, manifest digest, and image config to disk.
	im.saveLayerCache(imageName, layerPaths)

	// Save the Index Digest for future cache checks
	if res.indexDigest != "" {
		im.saveIndexDigest(imageName, res.indexDigest)
	}

	// Save the Image Digest for 'apsis images' display
	if digest, derr := img.Digest(); derr == nil {
		im.saveManifestDigest(imageName, digest.String())
	}

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

// PullEventFn is called for notable events during a pull (peer hit, stall, retry).
// eventType is corev1.EventTypeNormal or corev1.EventTypeWarning.
type PullEventFn func(eventType, reason, message string)

// PullOptions configures a pull operation.
type PullOptions struct {
	Progress PullProgress
	Event    PullEventFn // optional; nil disables event emission
}

// layersFromImage extracts and caches all layers from a resolved image.
// Layers are downloaded in parallel (up to layerConcurrency at once).
func (im *ImageManager) layersFromImage(img v1.Image, opts PullOptions) ([]string, error) {
	layers, err := img.Layers()
	if err != nil {
		return nil, fmt.Errorf("failed to get layers: %w", err)
	}

	total := len(layers)
	layerPaths := make([]string, total)
	var doneCount atomic.Int32

	// Snapshot peer list once for the whole pull so all layers share the same
	// healthy set and bad peers are evicted across layers.
	selector := im.newPeerSelector(context.Background())

	// Pre-register ALL layer hashes as inflight before any goroutine starts.
	// This lets peers discover our full intent immediately: they can wait on
	// individual layers as they complete rather than pulling from upstream.
	// We resolve DiffIDs sequentially here (cheap - just reads manifest data).
	type layerInfo struct {
		hash string
		ch   chan struct{} // closed when this layer's pull finishes
	}
	infos := make([]layerInfo, total)
	for i, layer := range layers {
		diffID, err := layer.DiffID()
		if err != nil {
			return nil, fmt.Errorf("resolve diffID[%d]: %w", i, err)
		}

		h := diffID.Hex
		// Only register as inflight if not already on disk - no point announcing
		// a layer we already have.
		if _, statErr := os.Stat(filepath.Join(im.layerCache, h)); statErr != nil {
			infos[i] = layerInfo{hash: h, ch: im.markInflight(h)}

		} else {
			infos[i] = layerInfo{hash: h}
		}
	}

	g := new(errgroup.Group)
	g.SetLimit(layerConcurrency)

	for i, layer := range layers {
		i, layer, info := i, layer, infos[i]
		g.Go(func() error {
			pathIface, err, _ := im.layerSF.Do(info.hash, func() (any, error) {
				path, pullErr := im.ensureLayer(info.hash, layer, selector, opts.Event)

				// Mark done (close channel) regardless of success/failure so
				// waiting peers unblock and fall through to upstream themselves.
				if info.ch != nil {
					im.markLayerDone(info.hash, info.ch)
				}

				return path, pullErr
			})

			if err != nil {
				return err
			}

			layerPaths[i] = pathIface.(string)
			if opts.Progress != nil {
				opts.Progress(int(doneCount.Add(1)), total)
			}

			return nil
		})
	}

	return layerPaths, g.Wait()
}

// layerCacheFile returns the path for a disk-persisted layer list.
func (im *ImageManager) layerCacheFile(imageName string) string {
	// Escape slashes so the image name is a valid filename
	safe := strings.ReplaceAll(imageName, "/", "_")

	return filepath.Join(im.layerCache, ".manifests", safe+".json")
}

// imageConfigFile returns the path for a disk-persisted image config.
func (im *ImageManager) imageConfigFile(imageName string) string {
	safe := strings.ReplaceAll(imageName, "/", "_")

	return filepath.Join(im.layerCache, ".manifests", safe+".config.json")
}

// manifestDigestFile returns the path for a disk-persisted manifest digest.
// Used by the Always pull policy to detect image updates without re-downloading
// all layers: if the remote digest matches the cached digest, layers are current.
func (im *ImageManager) manifestDigestFile(imageName string) string {
	safe := strings.ReplaceAll(imageName, "/", "_")

	return filepath.Join(im.layerCache, ".manifests", safe+".digest")
}

// saveManifestDigest persists the manifest digest for an image to disk.
func (im *ImageManager) saveManifestDigest(imageName, digest string) {
	path := im.manifestDigestFile(imageName)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		im.logger.Error("Failed to create manifest dir", "path", filepath.Dir(path), "err", err)

		return
	}

	if err := os.WriteFile(path, []byte(digest), 0644); err != nil {
		im.logger.Error("Failed to save manifest digest", "path", path, "err", err)
	}
}

// loadManifestDigest loads the cached manifest digest for an image.
// Returns "" on any error (treat as cache miss).
func (im *ImageManager) loadManifestDigest(imageName string) string {
	data, err := os.ReadFile(im.manifestDigestFile(imageName))
	if err != nil {
		return ""
	}

	return strings.TrimSpace(string(data))
}

// resolveManifestDigest fetches the remote manifest (aka index) digest
// for imageName without downloading any layer data.
// This is a lightweight HEAD-equivalent that lets
// the Always pull policy check for image updates in ~100ms instead of pulling
// potentially gigabytes of layers unnecessarily.
func (im *ImageManager) resolveManifestDigest(imageName string) (string, error) {
	ref, err := name.ParseReference(imageName)
	if err != nil {
		return "", err
	}

	desc, err := remote.Head(ref, remote.WithAuthFromKeychain(authn.DefaultKeychain))
	if err != nil {
		// remote.Head is not supported by all registries; fall back to full Get.
		img, imgErr := remote.Image(ref, remote.WithAuthFromKeychain(authn.DefaultKeychain))
		if imgErr != nil {
			return "", fmt.Errorf("head: %w; get: %w", err, imgErr)
		}

		d, digestErr := img.Digest()
		if digestErr != nil {
			return "", digestErr
		}

		return d.String(), nil
	}

	return desc.Digest.String(), nil
}

func (im *ImageManager) loadIndexDigest(imageName string) string {
	safe := strings.ReplaceAll(imageName, "/", "_")
	data, err := os.ReadFile(filepath.Join(im.layerCache, ".manifests", safe+".index"))
	if err != nil {
		return ""
	}

	return strings.TrimSpace(string(data))
}

func (im *ImageManager) saveIndexDigest(imageName, digest string) {
	safe := strings.ReplaceAll(imageName, "/", "_")
	path := filepath.Join(im.layerCache, ".manifests", safe+".index")
	os.WriteFile(path, []byte(digest), 0644)
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

	// Convert absolute paths back to just the hash (folder name)
	var hashes []string
	for _, p := range layerPaths {
		hashes = append(hashes, filepath.Base(p))
	}

	data, _ := json.Marshal(hashes)
	os.WriteFile(path, data, 0644)
}

// loadLayerCache loads a previously persisted layer list and verifies all
// layer directories still exist on disk.
func (im *ImageManager) loadLayerCache(imageName string) ([]string, error) {
	data, err := os.ReadFile(im.layerCacheFile(imageName))
	if err != nil {
		return nil, err
	}

	var hashes []string
	if err := json.Unmarshal(data, &hashes); err != nil {
		return nil, err
	}

	// Reconstruct absolute paths and verify they exist
	var absPaths []string
	for _, h := range hashes {
		fullPath := filepath.Join(im.layerCache, h)
		if _, err := os.Stat(fullPath); err != nil {
			return nil, fmt.Errorf("cached layer missing: %s", fullPath)
		}

		abs, _ := filepath.Abs(fullPath)
		absPaths = append(absPaths, abs)
	}

	return absPaths, nil
}

// blobPath returns the path for the cached compressed blob tarball.
func (im *ImageManager) blobPath(hash string) string {
	return filepath.Join(im.layerCache, hash+".tar.gz")
}

// BlobPath is the exported form for the server.blobProvider interface.
func (im *ImageManager) BlobPath(hash string) string {
	return im.blobPath(hash)
}

// markInflight registers hash as in-flight on this host.
// Returns a done channel that is closed by markLayerDone when the pull finishes.
// Other goroutines (and remote peers) can wait on this channel.
// If the hash is already registered (via layerSF dedup), returns the existing channel.
func (im *ImageManager) markInflight(hash string) chan struct{} {
	ch := make(chan struct{})
	actual, loaded := im.inflightLayers.LoadOrStore(hash, ch)
	if loaded {
		return actual.(chan struct{})
	}

	return ch
}

// markLayerDone marks hash as no longer in-flight and closes the done channel
// so any waiters unblock. Safe to call multiple times (close on already-closed
// channel is caught by recover).
func (im *ImageManager) markLayerDone(hash string, ch chan struct{}) {
	im.inflightLayers.Delete(hash)
	defer func() { recover() }() //nolint:errcheck
	close(ch)
}

// InflightHashes returns the set of layer hashes this host is currently pulling.
// Called by the /blobs/inflight HTTP endpoint so peers can discover our in-flight
// pulls and wait rather than independently pulling from upstream.
func (im *ImageManager) InflightHashes() []string {
	var hashes []string
	im.inflightLayers.Range(func(k, _ any) bool {
		hashes = append(hashes, k.(string))

		return true
	})

	return hashes
}

// ensureLayer ensures a layer is extracted to {layerCache}/{hash}/.
// Pull order:
//  1. Already extracted - return immediately.
//  2. Blob file exists locally - extract from it (handles mid-extraction crashes).
//  3. Peer node has blob - fetch compressed stream, tee to local blob file + extract.
//  4. Upstream registry - fetch compressed stream, tee to local blob file + extract.
func (im *ImageManager) ensureLayer(hash string, layer v1.Layer, selector *peerSelector, eventFn PullEventFn) (string, error) {
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

	// 2. Local blob file exists - extract from it.
	if _, err := os.Stat(blobFile); err == nil {
		im.logger.Info("Extracting layer from local blob", "hash", hash)
		if err := extractCompressedBlob(blobFile, tmpPath); err == nil {
			return commitLayer(tmpPath, destPath)
		}

		// Corrupt blob - remove and fall through.
		os.Remove(blobFile)
		os.RemoveAll(tmpPath)
		if err := os.MkdirAll(tmpPath, 0755); err != nil {
			return "", err
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	if selector != nil {
		// 3a. Check if a peer is already pulling this layer (inflight sharing).
		// If so, wait for it to finish then fetch from that peer instead of
		// independently hitting the upstream registry.
		//
		// peersWithInflight is called with a short-lived context - we just want
		// a quick snapshot, not to block the pull for a slow kube API.
		inflightCtx, inflightCancel := context.WithTimeout(ctx, 5*time.Second)
		inflightPeers := im.peersWithInflight(inflightCtx, map[string]bool{hash: true})
		inflightCancel()
		if peerEp, ok := inflightPeers[hash]; ok {
			im.logger.Info("Layer inflight on peer, waiting", "hash", hash[:12], "peer", peerEp)
			if eventFn != nil {
				eventFn("Normal", "PeerWait", fmt.Sprintf("Layer %s is being pulled from peer %s, waiting", hash[:12], peerEp))
			}

			if waitForPeerLayer(ctx, im.peerClient, peerEp, hash) {
				// Peer finished - fetch from it.
				body, err := fetchOnePeer(ctx, im.peerClient, hash, peerEp)
				if err == nil {
					err = saveAndExtract(stallReader(body, peerStallTimeout), blobFile, tmpPath)
					body.Close()

					if err == nil {
						if eventFn != nil {
							eventFn("Normal", "PulledFromPeer", fmt.Sprintf("Layer %s pulled from peer %s (waited for inflight)", hash[:12], peerEp))
						}

						return commitLayer(tmpPath, destPath)
					}
				}
				// Peer fetch failed after wait - fall through to selector / upstream.
				os.Remove(blobFile)
				os.RemoveAll(tmpPath)

				if err := os.MkdirAll(tmpPath, 0755); err != nil {
					return "", err
				}
			}

			// Peer timed out or disappeared - fall through to upstream.
		}

		// 3b. Try peers that already have the layer (present, not inflight).
		for {
			peerBody, peerEp, ok := selector.fetch(ctx, hash)
			if !ok {
				break // no healthy peers left
			}

			im.logger.Info("Pulling layer from peer", "hash", hash[:12], "peer", peerEp)

			err := saveAndExtract(peerBody, blobFile, tmpPath)
			peerBody.Close()

			if err == nil {
				if eventFn != nil {
					eventFn("Normal", "PulledFromPeer", fmt.Sprintf("Layer %s pulled from peer %s", hash[:12], peerEp))
				}

				return commitLayer(tmpPath, destPath)
			}

			// Stall or extraction error - evict this peer and try the next.
			im.logger.Warn("Peer layer fetch failed, trying next peer", "hash", hash[:12], "peer", peerEp, "err", err)
			if eventFn != nil {
				eventFn("Warning", "PeerFallback", fmt.Sprintf("Peer %s stalled on layer %s, trying next peer", peerEp, hash[:12]))
			}

			selector.markBad(peerEp)
			os.Remove(blobFile)
			os.RemoveAll(tmpPath)

			if err := os.MkdirAll(tmpPath, 0755); err != nil {
				return "", err
			}
		}
	}

	// 4. Upstream registry - retry up to 3 times on stall/error.
	const maxAttempts = 3
	const registryMinRate = 512 // bytes/sec - below this after warmup = stalled
	const registryWarmup = 30 * time.Second
	var lastErr error
	for attempt := range maxAttempts {
		if attempt > 0 {
			im.logger.Warn("Retrying upstream layer fetch", "hash", hash, "attempt", attempt+1, "err", lastErr)
			if eventFn != nil {
				eventFn("Warning", "RegistryRetry", fmt.Sprintf("Retrying layer %s from registry (attempt %d/%d): %v", hash[:12], attempt+1, maxAttempts, lastErr))
			}

			os.Remove(blobFile)
			os.RemoveAll(tmpPath)

			if err := os.MkdirAll(tmpPath, 0755); err != nil {
				return "", err
			}

			time.Sleep(time.Duration(attempt) * 3 * time.Second)
		}

		im.logger.Info("Pulling layer from upstream", "hash", hash)
		if eventFn != nil {
			eventFn("Normal", "RegistryPull", fmt.Sprintf("Pulling layer %s from registry", hash[:12]))
		}

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
// TODO(overlay-refactor): When mstack is the primary path, this
// manual unix.Mount call and the upper/work dir management can be removed.
// Mount() would then just return the layer paths for PodConfig.LayerPaths,
// and Unmount() only cleans up pre-start bind mount temp files.
//
// Layout under <baseDir>/pods/<podUID>/:
//
//	rootfs/  - merged (container's view)
//	upper/   - writable layer
//	work/    - overlayfs scratch space
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
	// Our Pull() returns bottom->top, so reverse before joining
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
//
// TODO(overlay-refactor): Remove once --overlay= is the sole path.
// With --volatile=overlay + nspawn --overlay=, nspawn tears down the overlay on
// container stop. The only remaining cleanup is the layer cache and tmpdir from
// prepareBindFiles (handled by the goroutine in RunMachine). This function and
// the upper/work dir management below can then be deleted entirely.
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

// --- MStack Management ---

// PrepareMStack creates a .mstack directory for a specific container.
// It assembles the root filesystem hierarchy by creating symlinks to the
// provided layers, ordered as layer@0 (bottom) to layer@N (top).
// MStackBindEntry describes a file or directory to inject into the mstack dir
// so it gets bind-mounted into the container at the given path.
// Files are written as regular files; directories are created as subdirs.
// No host tmpdir or symlinks needed - RemoveMStack cleans everything up.
type MStackBindEntry struct {
	// ContainerPath is the absolute destination inside the container.
	ContainerPath string

	// Content is the file content. Mutually exclusive with IsDir.
	Content []byte

	// IsDir creates a directory bind entry instead of a file.
	IsDir bool

	// DirMode sets the permission on a directory entry (0 = default 0755).
	DirMode os.FileMode

	// ReadOnly creates a robind@ entry; false creates a bind@ entry.
	ReadOnly bool
}

// escapeMStackPath encodes a container path as a mstack bind entry name.
// Follows the same escaping rules as systemd unit names: leading / stripped,
// remaining / replaced with -
// e.g. /etc/resolv.conf -> etc-resolv.conf
func escapeMStackPath(containerPath string) string {
	p := strings.TrimPrefix(containerPath, "/")

	return strings.ReplaceAll(p, "/", "-")
}

// PrepareMStack creates a .mstack directory for a container.
// It creates layer@N symlinks for image layers and robind@/bind@ symlinks
// for bind-mounted files (resolv.conf, passwd, group, getent shim, home dirs).
// Bind files are co-located in the mstack dir so RemoveMStack cleans everything up.
func (im *ImageManager) PrepareMStack(podUID, cName string, layers []string, binds []MStackBindEntry) (string, error) {
	// 1. Use baseDir to keep paths consistent with the rest of the manager.
	stacksBase := filepath.Join(im.baseDir, "stacks")

	// 2. Path MUST be per-container to support pods with multiple images.
	// Result: <baseDir>/stacks/<podUID>-<cName>.mstack
	mstackName := fmt.Sprintf("%s-%s.mstack", podUID, cName)
	mstackDir := filepath.Join(stacksBase, mstackName)

	// 3. Idempotency: Clean up any existing stack from a previous attempt.
	if err := os.RemoveAll(mstackDir); err != nil {
		return "", fmt.Errorf("failed to clean existing mstack: %w", err)
	}

	if err := os.MkdirAll(mstackDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create mstack dir: %w", err)
	}

	cleanup := func() { _ = os.RemoveAll(mstackDir) }

	// 4. Create the layer stack.
	// systemd-nspawn sorts these numerically: layer@0 is the bottom-most.
	for i, layerPath := range layers {
		linkName := filepath.Join(mstackDir, fmt.Sprintf("layer@%d", i))
		if err := os.Symlink(layerPath, linkName); err != nil {
			cleanup()

			return "", fmt.Errorf("failed to link layer %d (%s): %w", i, layerPath, err)
		}
	}

	// 5. Write bind entries directly into the mstack dir.
	// robind@<escaped-path> is a regular file/dir with the bind content.
	// mstack interprets it as a read-only bind mount at the escaped path.
	// bind@<escaped-path> is the same but read-write.
	// No symlinks, no external tmpdir - RemoveMStack cleans everything up.
	for _, b := range binds {
		prefix := "robind@"
		if !b.ReadOnly {
			prefix = "bind@"
		}

		escaped := escapeMStackPath(b.ContainerPath)
		entryPath := filepath.Join(mstackDir, prefix+escaped)

		if b.IsDir {
			if err := os.MkdirAll(entryPath, 0755); err != nil {
				cleanup()

				return "", fmt.Errorf("failed to create bind dir %s: %w", b.ContainerPath, err)
			}
			if b.DirMode != 0 {
				_ = os.Chmod(entryPath, b.DirMode)
			}

		} else {
			if err := os.WriteFile(entryPath, b.Content, 0644); err != nil {
				cleanup()

				return "", fmt.Errorf("failed to write bind file %s: %w", b.ContainerPath, err)
			}
		}
	}

	// Return absolute path for the --mstack= flag
	return filepath.Abs(mstackDir)
}

// RemoveMStack deletes the .mstack directory for a specific container.
func (im *ImageManager) RemoveMStack(podUID, cName string) error {
	mstackName := fmt.Sprintf("%s-%s.mstack", podUID, cName)
	mstackDir := filepath.Join(im.baseDir, "stacks", mstackName)

	if err := os.RemoveAll(mstackDir); err != nil {
		return fmt.Errorf("failed to remove mstack %s: %w", mstackName, err)
	}

	return nil
}
