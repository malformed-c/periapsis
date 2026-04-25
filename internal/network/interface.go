// Copyright (C) 2025-2026 Malformed C. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-only

package network

import (
	"context"
)

// NetworkManager defines the contract for setting up Pod networking.
// In the future, this will wrap libcni
type NetworkManager interface {
	// Setup creates the network namespace and (eventually) invokes CNI ADD.
	// Returns the absolute path to the network namespace handle (e.g., /var/run/netns/uid)
	// and the pod IP string.
	// nodeName is the Kubernetes node (pawn) this pod is scheduled to; the CNI
	// plugin uses it to allocate from the correct per-node CIDR pool.
	Setup(ctx context.Context, podUID, namespace, name, nodeName string) (string, string, error)

	// Teardown invokes CNI DEL and removes the network namespace.
	// namespace and name must match the values passed to Setup so the CNI
	// plugin can locate its state (e.g. Cilium uses K8S_POD_NAMESPACE/NAME).
	Teardown(ctx context.Context, podUID, namespace, name string) error
}
