// Copyright (C) 2025-2026 Malformed C. All rights reserved.
// SPDX-License-Identifier: BUSL-1.1

package fixtures

import (
	"context"
	"sync"
)

// NetworkFixture implements internal/network.NetworkManager for testing.
type NetworkFixture struct {
	mu sync.Mutex

	SetupCalled    []string
	TeardownCalled []string

	DefaultIP string
}

func NewNetworkFixture() *NetworkFixture {
	return &NetworkFixture{
		DefaultIP: "10.0.0.1",
	}
}

func (n *NetworkFixture) Setup(_ context.Context, podUID, _, _, _ string) (string, string, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.SetupCalled = append(n.SetupCalled, podUID)
	return "/var/run/netns/" + podUID, n.DefaultIP, nil
}

func (n *NetworkFixture) Teardown(_ context.Context, podUID, _, _ string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.TeardownCalled = append(n.TeardownCalled, podUID)
	return nil
}
