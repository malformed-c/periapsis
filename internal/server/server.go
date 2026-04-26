// Copyright (C) 2025-2026 Malformed C. All rights reserved.
// SPDX-License-Identifier: BUSL-1.1

package server

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/malformed-c/periapsis/internal/pki"
	"github.com/malformed-c/periapsis/node"
	"github.com/malformed-c/periapsis/node/api"
	"k8s.io/client-go/kubernetes"
)

type PawnServer struct {
	port     int
	pawnName string

	provider   *node.Gambit
	httpServer *http.Server
	listener   net.Listener
}

// PawnServerConfig holds the configuration for creating a PawnServer.
type PawnServerConfig struct {
	CACertPath   string
	CAKeyPath    string
	ConfigDir    string // e.g. /etc/apsis/perigeos - for persisting CSR certs
	KubeClient   kubernetes.Interface
	ImageManager blobProvider // optional - enables GET /blobs/{digest}
}

// blobProvider is the subset of image.ImageManager used for blob serving.
type blobProvider interface {
	BlobPath(hash string) string
	InflightHashes() []string
}

func NewPawnServer(g *node.Gambit, cfg PawnServerConfig) (*PawnServer, error) {
	port := g.Config.Port
	pawnName := g.Config.Name

	mux := http.NewServeMux()
	api.AttachPodRoutes(api.PodHandlerConfig{
		GetContainerLogs: g.GetContainerLogs,
		RunInContainer:   g.RunInContainer,
		AttachContainer:  g.AttachContainer,
		PortForward:      g.PortForward,
	}, mux, true)

	// /blobs/{digest} - serves cached compressed OCI layer tarballs to peers.
	// Content-addressed: the digest is the integrity check; TLS cert is not verified by peers.
	if cfg.ImageManager != nil {
		im := cfg.ImageManager
		// GET/HEAD /blobs/{digest} - serves cached compressed OCI layer tarballs.
		// HEAD is used by peers to check presence without downloading.
		mux.HandleFunc("/blobs/", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet && r.Method != http.MethodHead {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)

				return
			}

			digest := strings.TrimPrefix(r.URL.Path, "/blobs/")
			if digest == "" || strings.ContainsAny(digest, "/\\") {
				http.Error(w, "invalid digest", http.StatusBadRequest)

				return
			}

			blobFile := im.BlobPath(digest)
			f, err := os.Open(blobFile)
			if err != nil {
				if os.IsNotExist(err) {
					http.Error(w, "not found", http.StatusNotFound)

				} else {
					http.Error(w, "internal error", http.StatusInternalServerError)
				}

				return
			}
			defer f.Close()

			stat, _ := f.Stat()

			// TODO zstd?
			w.Header().Set("Content-Type", "application/gzip")
			if r.Method == http.MethodHead {
				w.Header().Set("Content-Length", fmt.Sprintf("%d", stat.Size()))
				w.WriteHeader(http.StatusOK)

				return
			}

			http.ServeContent(w, r, digest+".tar.gz", stat.ModTime(), f)
		})

		// GET /blobs/inflight - returns JSON array of layer hashes currently
		// being pulled by this host. Peers use this to discover in-flight pulls
		// and wait rather than independently hitting the upstream registry.
		mux.HandleFunc("/blobs/inflight", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)

				return
			}

			hashes := im.InflightHashes()
			if hashes == nil {
				hashes = []string{} // always return an array, never null
			}

			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(hashes)
		})
	}

	// /stats/summary is the endpoint metrics-server scrapes for resource usage.
	mux.HandleFunc("/stats/summary", func(w http.ResponseWriter, r *http.Request) {
		summary, err := g.GetStatsSummary(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)

			return
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(summary); err != nil {
			slog.Default().Error("stats/summary encode", "err", err)
		}
	})

	addr := fmt.Sprintf(":%d", port)

	tlsCert, err := obtainCert(pawnName, cfg)
	if err != nil {
		return nil, fmt.Errorf("obtain TLS cert for %s: %w", pawnName, err)
	}

	certHolder, err := pki.NewCertHolder(tlsCert, func() (tls.Certificate, error) {
		return obtainCertFresh(pawnName, cfg)
	}, slog.Default())
	if err != nil {
		return nil, fmt.Errorf("init cert holder for %s: %w", pawnName, err)
	}

	tlsCfg := &tls.Config{
		GetCertificate: certHolder.GetCertificate,
	}

	// Bind the port eagerly so we can fail fast and report a useful error
	// before the goroutine even starts. This also means "Pawn API server
	// listening" in main.go is only logged when the socket is actually bound.
	tcpListener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen %s: %w", addr, err)
	}

	tlsListener := tls.NewListener(tcpListener, tlsCfg)

	httpServer := &http.Server{
		Handler:           mux,
		TLSConfig:         tlsCfg,
		ReadHeaderTimeout: 0, // Must be 0 - SPDY stream negotiation for exec/attach exceeds any fixed timeout
		WriteTimeout:      0, // Must be 0 for streaming logs
		IdleTimeout:       60 * time.Second,
	}

	return &PawnServer{
		port:       port,
		pawnName:   g.Config.Name,
		provider:   g,
		httpServer: httpServer,
		listener:   tlsListener,
	}, nil
}

// Start serves on the already-bound listener. Blocks until the server stops.
func (ps *PawnServer) Start() error {
	return ps.httpServer.Serve(ps.listener)
}

// Stop allows for graceful shutdown.
func (s *PawnServer) Stop(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

// obtainCert tries three strategies in order:
//  1. CSR via Kubernetes certificates API (if kubeClient provided)
//  2. Sign with local CA cert+key (legacy k3s path)
//  3. Self-signed fallback
func obtainCert(pawnName string, cfg PawnServerConfig) (tls.Certificate, error) {
	logger := slog.Default()

	// Strategy 1: CSR flow - proper k8s way, no CA key needed on node.
	if cfg.KubeClient != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		cert, err := pki.RequestServingCert(ctx, cfg.KubeClient, pawnName, cfg.ConfigDir, logger)
		if err == nil {
			return cert, nil
		}

		logger.Warn("CSR flow failed, trying CA signing", "pawn", pawnName, "err", err)
	}

	// Strategy 2: Sign with local CA (legacy - requires CA key on node).
	if cfg.CACertPath != "" {
		caCert, caKey, err := pki.LoadCA(cfg.CACertPath, cfg.CAKeyPath)
		if err == nil {
			logger.Info("Signing pawn certificate with local CA", "pawn", pawnName, "ca", cfg.CACertPath)

			return pki.GenerateCert(pawnName, caCert, caKey)
		}

		logger.Warn("Local CA not loaded", "ca", cfg.CACertPath, "err", err)
	}

	// Strategy 3: Self-signed fallback.
	logger.Warn("Using self-signed certificate", "pawn", pawnName)

	return pki.GenerateCert(pawnName, nil, nil)
}

// obtainCertFresh forces a new certificate by clearing the disk cache first.
// Used by CertHolder when the current cert doesn't cover a requested name/IP.
func obtainCertFresh(pawnName string, cfg PawnServerConfig) (tls.Certificate, error) {
	logger := slog.Default()

	// Delete cached cert so RequestServingCert issues a fresh CSR.
	if cfg.ConfigDir != "" {
		pkiDir := cfg.ConfigDir + "/pki"
		_ = os.Remove(pkiDir + "/" + pawnName + ".crt")
		_ = os.Remove(pkiDir + "/" + pawnName + ".key")

		logger.Info("Cleared cached cert for renewal", "pawn", pawnName)
	}

	return obtainCert(pawnName, cfg)
}
