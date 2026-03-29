package join

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	controlSocket   = "/run/apsis/perigeos.sock"
	verifyTimeout   = 60 * time.Second
	verifyPollInterval = 2 * time.Second
)

// verifyRegistration waits for:
//  1. The perigeos control socket to appear (service started).
//  2. At least one pawn Node with periapsis.io/host={hostname} to be Ready.
func verifyRegistration(ctx context.Context, client kubernetes.Interface, logger *slog.Logger) error {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		return fmt.Errorf("cannot determine hostname: %w", err)
	}

	deadline := time.Now().Add(verifyTimeout)
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	logger.Info("Waiting for perigeos control socket...", "socket", controlSocket)
	if err := waitForSocket(ctx, controlSocket); err != nil {
		return fmt.Errorf("control socket did not appear: %w", err)
	}
	logger.Info("Control socket ready")

	logger.Info("Waiting for pawn nodes to register...", "host", hostname)
	if err := waitForPawnNodes(ctx, client, hostname, logger); err != nil {
		return fmt.Errorf("pawn registration check failed: %w", err)
	}

	return nil
}

func waitForSocket(ctx context.Context, path string) error {
	for {
		if _, err := os.Stat(path); err == nil {
			// Try to actually dial it.
			if conn, err := net.DialTimeout("unix", path, time.Second); err == nil {
				conn.Close()
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(verifyPollInterval):
		}
	}
}

func waitForPawnNodes(ctx context.Context, client kubernetes.Interface, hostname string, logger *slog.Logger) error {
	labelSel := "periapsis.io/host=" + hostname
	for {
		nodes, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{
			LabelSelector: labelSel,
		})
		if err == nil && len(nodes.Items) > 0 {
			names := make([]string, len(nodes.Items))
			for i, n := range nodes.Items {
				names[i] = n.Name
			}
			logger.Info("Pawn nodes registered", "nodes", names)
			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for pawn nodes with label %s", labelSel)
		case <-time.After(verifyPollInterval):
			logger.Debug("No pawn nodes yet, retrying...")
		}
	}
}
