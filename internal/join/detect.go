// Copyright (C) 2025-2026 Malformed C. All rights reserved.
// SPDX-License-Identifier: BUSL-1.1

package join

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// KubeletPresence describes whether an existing kubelet was found on this host.
type KubeletPresence struct {
	// Found is true when a kubelet or k3s process is running or a Node object exists.
	Found bool
	// Source describes where the kubelet was detected ("process" or "node-api").
	Source string
}

// detectKubelet checks whether an existing kubelet already owns this host.
// It does a quick process scan first (cheap), then falls back to the Node API.
func detectKubelet(ctx context.Context, client kubernetes.Interface, logger *slog.Logger) KubeletPresence {
	// --- Process scan ---
	if p := scanKubeletProcess(); p.Found {
		logger.Info("Existing kubelet process detected", "source", p.Source)
		return p
	}

	// --- Node API check (skip if no client available) ---
	if client == nil {
		logger.Info("No API client - skipping Node API check (process scan only)")
		return KubeletPresence{}
	}

	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		return KubeletPresence{}
	}

	node, err := client.CoreV1().Nodes().Get(ctx, hostname, metav1.GetOptions{})
	if err == nil && !strings.HasPrefix(node.Spec.ProviderID, "perigeos://") {
		logger.Info("Existing kubelet node found in API", "node", hostname)
		return KubeletPresence{Found: true, Source: "node-api"}
	}

	logger.Info("No existing kubelet detected - perigeos will own this host as primary")
	return KubeletPresence{}
}

// scanKubeletProcess reads /proc to look for kubelet or k3s-agent processes.
func scanKubeletProcess() KubeletPresence {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return KubeletPresence{}
	}

	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		// Only numeric entries are PIDs.
		name := e.Name()
		if name == "" || name[0] < '1' || name[0] > '9' {
			continue
		}

		cmdline, err := os.ReadFile(filepath.Join("/proc", name, "cmdline"))
		if err != nil {
			continue
		}

		// cmdline entries are NUL-separated; we only need the first token (argv[0]).
		first := strings.TrimRight(strings.SplitN(string(cmdline), "\x00", 2)[0], " ")
		base := filepath.Base(first)

		switch base {
		case "kubelet":
			return KubeletPresence{Found: true, Source: "process:kubelet"}
		case "k3s", "k3s-agent":
			return KubeletPresence{Found: true, Source: "process:k3s"}
		}
	}

	return KubeletPresence{}
}
