package join

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
)

const (
	controlSocket      = "/run/apsis/perigeos.sock"
	verifyTimeout      = 60 * time.Second
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

	// First, check if any nodes already exist with the label.
	// This handles the case where nodes were registered before we started watching.
	nodes, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{
		LabelSelector: labelSel,
	})
	if err == nil && len(nodes.Items) > 0 {
		names := make([]string, len(nodes.Items))
		for i, n := range nodes.Items {
			names[i] = n.Name
		}
		logger.Info("Pawn nodes already registered", "nodes", names)
		return nil
	}

	// No nodes found yet, start watching for new nodes with the label.
	logger.Debug("Starting watch for nodes with label", "label", labelSel)
	watcher, err := client.CoreV1().Nodes().Watch(ctx, metav1.ListOptions{
		LabelSelector: labelSel,
	})
	if err != nil {
		return fmt.Errorf("failed to watch nodes: %w", err)
	}
	defer watcher.Stop()

	// Watch for ADDED or MODIFIED events that indicate a node with the label was created or updated.
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for pawn nodes with label %s", labelSel)
		case event, ok := <-watcher.ResultChan():
			if !ok {
				// Channel closed, try again.
				return fmt.Errorf("watch channel closed while waiting for pawn nodes with label %s", labelSel)
			}

			// Only care about ADDED and MODIFIED events; DELETED is not relevant.
			if event.Type != watch.Added && event.Type != watch.Modified {
				continue
			}

			node, ok := event.Object.(*corev1.Node)
			if !ok {
				continue
			}

			logger.Info("Pawn node registered", "node", node.Name)
			return nil
		}
	}
}
