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

	"github.com/malformed-c/periapsis/node"
	"github.com/malformed-c/periapsis/internal/pki"
	"github.com/malformed-c/periapsis/node/api"
)

type PawnServer struct {
	port     int
	pawnName string

	provider   *node.Gambit
	httpServer *http.Server
	listener   net.Listener
}

func NewPawnServer(g *node.Gambit, caPath, caKeyPath string) (*PawnServer, error) {
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

	var tlsCert tls.Certificate
	var err error

	caCert, caKey, errLoad := pki.LoadCA(caPath, caKeyPath)
	if errLoad == nil {
		slog.Default().Info("Signing pawn certificate with k8s CA", "pawn", pawnName, "ca", caPath)
		tlsCert, err = pki.GenerateCert(pawnName, caCert, caKey)
	} else {
		slog.Default().Warn("K8s CA not loaded, falling back to self-signed cert", "ca", caPath, "err", errLoad)
		tlsCert, err = pki.GenerateCert(pawnName, nil, nil)
	}
	if err != nil {
		return nil, fmt.Errorf("generate TLS cert for %s: %w", pawnName, err)
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
