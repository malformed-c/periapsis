// Copyright (C) 2024-2026 Malformed C. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-only

package join

import (
	"context"
	"fmt"
	"log/slog"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// Runner executes the join steps in sequence.
type Runner struct {
	opts   *Options
	logger *slog.Logger
}

// New creates a Runner with the given options.
func New(opts *Options, logger *slog.Logger) *Runner {
	return &Runner{opts: opts, logger: logger}
}

// Run executes all join steps. If DryRun is set, read-only steps execute
// normally (prereq check, kubelet detection) and write steps are skipped
// with a log message.
func (r *Runner) Run(ctx context.Context) error {
	// Step 1: prerequisites.
	r.logger.Info("--- Step 1/7: check-prerequisites")
	if err := checkPrerequisites(r.logger); err != nil {
		return fmt.Errorf("prerequisites not met: %w", err)
	}

	// Step 2: generate or copy kubeconfig.
	r.logger.Info("--- Step 2/7: generate-kubeconfig")
	var (
		client         kubernetes.Interface
		kubeconfigPath string
	)
	if r.opts.DryRun {
		r.logger.Info("[dry-run] would write kubeconfig", "path", r.opts.ConfigDir+"/kubeconfig.yaml")
	} else {
		var err error
		client, kubeconfigPath, err = generateKubeconfig(ctx, r.opts, r.logger)
		if err != nil {
			return fmt.Errorf("generate-kubeconfig: %w", err)
		}
		_ = kubeconfigPath
	}

	// Step 3: detect existing kubelet.
	r.logger.Info("--- Step 3/7: detect-kubelet")
	var primary bool
	if r.opts.DryRun || client == nil {
		presence := detectKubelet(ctx, nil, r.logger)
		primary = !presence.Found
		if r.opts.DryRun {
			r.logger.Info("[dry-run] kubelet detection ran; would set primary", "primary", primary)
		}
	} else {
		presence := detectKubelet(ctx, client, r.logger)
		primary = !presence.Found
		r.logger.Info("Primary node decision", "primary", primary, "kubelet_found", presence.Found)
	}

	// Step 4: generate default config.
	r.logger.Info("--- Step 4/7: generate-config")
	constellationCNI := detectConstellationCNI(ctx, client, r.logger)
	if r.opts.DryRun {
		r.logger.Info("[dry-run] would write", "path", r.opts.ConfigDir+"/perigeos.toml")
	} else {
		if _, err := generateConfig(r.opts, primary, constellationCNI, r.logger); err != nil {
			return fmt.Errorf("generate-config: %w", err)
		}
	}

	// Step 5: install systemd service.
	r.logger.Info("--- Step 5/7: install-service")
	if r.opts.DryRun {
		r.logger.Info("[dry-run] would write", "path", unitPath)
		r.logger.Info("[dry-run] would run: systemctl daemon-reload && systemctl enable perigeos.service")
	} else {
		if err := installService(r.opts, r.logger); err != nil {
			return fmt.Errorf("install-service: %w", err)
		}
	}

	// Step 6: start service.
	r.logger.Info("--- Step 6/7: start-service")
	if r.opts.DryRun {
		r.logger.Info("[dry-run] would run: systemctl restart perigeos.service")
	} else {
		if err := startService(r.logger); err != nil {
			return fmt.Errorf("start-service: %w", err)
		}
	}

	// Step 7: verify registration.
	r.logger.Info("--- Step 7/7: verify-registration")
	if r.opts.DryRun {
		r.logger.Info("[dry-run] would wait for control socket and pawn node registration")
		r.logger.Info("[dry-run] join complete - review planned files above")
		return nil
	}

	if client != nil {
		if err := verifyRegistration(ctx, client, r.logger); err != nil {
			// Non-fatal: service may just be slow.
			r.logger.Warn("Verification incomplete (service may still be starting)", "err", err)
		}
	}

	r.logger.Info("Join complete - perigeos is running")
	r.logger.Info("Edit config to add more pawns:", "path", r.opts.ConfigDir+"/perigeos.toml")
	r.logger.Info("Then restart:                  systemctl restart perigeos.service")
	return nil
}

// detectConstellationCNI checks whether a Constellation (Cilium-based) agent
// DaemonSet is deployed in the cluster. Returns true if found.
func detectConstellationCNI(ctx context.Context, client kubernetes.Interface, logger *slog.Logger) bool {
	if client == nil {
		return false
	}
	// Look for the constellation-agent DaemonSet in kube-system.
	_, err := client.AppsV1().DaemonSets("kube-system").Get(ctx, "constellation-agent", metav1.GetOptions{})
	if err == nil {
		logger.Info("Detected Constellation CNI in cluster - enabling [global.cni]")
		return true
	}
	logger.Debug("Constellation CNI not detected in cluster", "err", err)
	return false
}
