// Copyright (C) 2025-2026 Malformed C. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-only

package join

import (
	"errors"
	"flag"
)

// Options holds the parsed flags for `perigeos join`.
type Options struct {
	// APIServer is the Kubernetes API server URL (e.g. https://192.168.1.10:6443).
	// Required when Kubeconfig is not provided.
	APIServer string

	// Token is the bootstrap/service-account token for initial API auth.
	// Required when Kubeconfig is not provided.
	Token string

	// Kubeconfig is an existing kubeconfig file to use instead of token discovery.
	Kubeconfig string

	// ConfigDir is where perigeos config and credentials are written.
	// Default: /etc/apsis/perigeos
	ConfigDir string

	// BaseDir is the state directory for perigeos.
	// Default: /var/lib/apsis/perigeos
	BaseDir string

	// DryRun prints what would be done without writing files or starting services.
	DryRun bool
}

// RegisterFlags registers join flags into the given FlagSet.
func (o *Options) RegisterFlags(fs *flag.FlagSet) {
	fs.StringVar(&o.APIServer, "api-server", "", "Kubernetes API server URL (required without --kubeconfig)")
	fs.StringVar(&o.Token, "token", "", "Bootstrap or service-account token (required without --kubeconfig)")
	fs.StringVar(&o.Kubeconfig, "kubeconfig", "", "Existing kubeconfig file (alternative to --api-server + --token)")
	fs.StringVar(&o.ConfigDir, "config-dir", "/etc/apsis/perigeos", "Directory for perigeos config and credentials")
	fs.StringVar(&o.BaseDir, "base-dir", "/var/lib/apsis/perigeos", "State directory for perigeos")
	fs.BoolVar(&o.DryRun, "dry-run", false, "Print planned actions without making changes")
}

// Validate checks that the options are consistent.
func (o *Options) Validate() error {
	if o.Kubeconfig == "" {
		if o.APIServer == "" {
			return errors.New("either --kubeconfig or --api-server is required")
		}
		if o.Token == "" {
			return errors.New("--token is required when using --api-server")
		}
	}
	return nil
}
