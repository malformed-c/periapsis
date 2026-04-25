// Copyright (C) 2025-2026 Malformed C. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-only

package join

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

// generateKubeconfig builds or copies a kubeconfig and writes it to
// {configDir}/kubeconfig.yaml. It returns a ready kubernetes.Interface.
func generateKubeconfig(ctx context.Context, opts *Options, logger *slog.Logger) (kubernetes.Interface, string, error) {
	kubeconfigPath := filepath.Join(opts.ConfigDir, "kubeconfig.yaml")

	var cfg *clientcmdapi.Config

	if opts.Kubeconfig != "" {
		// Copy the provided kubeconfig.
		data, err := os.ReadFile(opts.Kubeconfig)
		if err != nil {
			return nil, "", fmt.Errorf("read kubeconfig %s: %w", opts.Kubeconfig, err)
		}
		cfg, err = clientcmd.Load(data)
		if err != nil {
			return nil, "", fmt.Errorf("parse kubeconfig %s: %w", opts.Kubeconfig, err)
		}
		logger.Info("Using provided kubeconfig", "src", opts.Kubeconfig)
	} else {
		// Discover via API server + token.
		var err error
		cfg, err = discoverKubeconfig(ctx, opts.APIServer, opts.Token, logger)
		if err != nil {
			return nil, "", fmt.Errorf("discover kubeconfig: %w", err)
		}
	}

	if err := os.MkdirAll(opts.ConfigDir, 0o700); err != nil {
		return nil, "", fmt.Errorf("create config dir %s: %w", opts.ConfigDir, err)
	}

	if err := clientcmd.WriteToFile(*cfg, kubeconfigPath); err != nil {
		return nil, "", fmt.Errorf("write kubeconfig %s: %w", kubeconfigPath, err)
	}
	if err := os.Chmod(kubeconfigPath, 0o600); err != nil {
		return nil, "", fmt.Errorf("chmod kubeconfig: %w", err)
	}
	logger.Info("Kubeconfig written", "path", kubeconfigPath)

	// Validate connectivity.
	restCfg, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	if err != nil {
		return nil, "", fmt.Errorf("load written kubeconfig: %w", err)
	}

	client, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return nil, "", fmt.Errorf("create Kubernetes client: %w", err)
	}

	version, err := client.Discovery().ServerVersion()
	if err != nil {
		return nil, "", fmt.Errorf("connect to API server: %w", err)
	}
	logger.Info("Connected to API server", "version", version.GitVersion, "kubeconfig", kubeconfigPath)

	return client, kubeconfigPath, nil
}
