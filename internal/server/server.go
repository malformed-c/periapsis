package server

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
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
	CACertPath string
	CAKeyPath  string
	ConfigDir  string // e.g. /etc/apsis/perigeos — for persisting CSR certs
	KubeClient kubernetes.Interface
}

func NewPawnServer(g *node.Gambit, cfg PawnServerConfig) (*PawnServer, error) {
	port     := g.Config.Port
	pawnName := g.Config.Name

	mux := http.NewServeMux()
	api.AttachPodRoutes(api.PodHandlerConfig{
		GetContainerLogs:  g.GetContainerLogs,
		RunInContainer:    g.RunInContainer,
		AttachToContainer: g.AttachToContainer,
	}, mux, true)

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

	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
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
		ReadHeaderTimeout: 5 * time.Second,
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

	// Strategy 1: CSR flow — proper k8s way, no CA key needed on node.
	if cfg.KubeClient != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		cert, err := pki.RequestServingCert(ctx, cfg.KubeClient, pawnName, cfg.ConfigDir, logger)
		if err == nil {
			return cert, nil
		}
		logger.Warn("CSR flow failed, trying CA signing", "pawn", pawnName, "err", err)
	}

	// Strategy 2: Sign with local CA (legacy — requires CA key on node).
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
