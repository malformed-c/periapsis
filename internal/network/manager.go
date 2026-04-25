// Copyright (C) 2025-2026 Malformed C. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-only

package network

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

const (
	bridgeName = "perigeos0"
	podCIDR    = "10.88.0.0/16"
	podSubnet  = "/16"
)

// netnsName returns the network namespace name with the peri- prefix.
func netnsName(podUID string) string {
	return "peri-" + podUID
}

// Ensure compile-time interface compliance
var _ NetworkManager = (*LinuxNetworkManager)(nil)

type LinuxNetworkManager struct {
	logger  *slog.Logger
	baseDir string
	pool    *ipPool
}

func NewLinuxNetworkManager(logger *slog.Logger) *LinuxNetworkManager {
	pool, err := newIPPool(podCIDR)
	if err != nil {
		panic(fmt.Sprintf("failed to init IP pool: %v", err))
	}

	nm := &LinuxNetworkManager{
		logger:  logger,
		baseDir: "/var/run/netns",
		pool:    pool,
	}

	if err := nm.ensureBridge(); err != nil {
		logger.Warn("Bridge setup failed (non-fatal on re-init)", "err", err)
	}

	return nm
}

// Setup creates a network namespace, a veth pair connecting it to the host bridge,
// assigns an IP from the pool, and brings everything up.
func (n *LinuxNetworkManager) Setup(ctx context.Context, podUID, namespace, name, nodeName string) (string, string, error) {
	n.logger.Debug("Setting up network namespace", "pod", podUID)

	netnsPath := filepath.Join(n.baseDir, netnsName(podUID))

	// Idempotency: if netns already exists, recover the IP
	if _, err := os.Stat(netnsPath); err == nil {
		ip, err := n.recoverPodIP(ctx, podUID)
		if err != nil {
			n.logger.Warn("Could not recover pod IP, re-allocating", "err", err)
			ip, err = n.pool.Allocate(podUID)
			if err != nil {
				return "", "", err
			}
		}
		n.logger.Info("Network namespace already exists", "path", netnsPath, "ip", ip)
		return netnsPath, ip, nil
	}

	// Create netns (includes bringing up loopback)
	if err := createNetns(ctx, netnsName(podUID)); err != nil {
		return "", "", err
	}

	// Allocate IP
	podIP, err := n.pool.Allocate(podUID)
	if err != nil {
		_ = deleteNetns(ctx, podUID)
		return "", "", err
	}

	gateway := n.pool.Gateway()
	vethHost := vethName(podUID)
	nnsName := netnsName(podUID)

	cmds := [][]string{
		// Create veth pair, put pod side directly into netns
		{"ip", "link", "add", vethHost, "type", "veth", "peer", "name", "eth0", "netns", nnsName},
		// Host side: attach to bridge, bring up
		{"ip", "link", "set", vethHost, "master", bridgeName},
		{"ip", "link", "set", vethHost, "up"},
		// Pod side: assign IP, bring up loopback and eth0, add default route
		{"ip", "netns", "exec", nnsName, "ip", "addr", "add", podIP + podSubnet, "dev", "eth0"},
		{"ip", "netns", "exec", nnsName, "ip", "link", "set", "eth0", "up"},
		{"ip", "netns", "exec", nnsName, "ip", "route", "add", "default", "via", gateway},
	}

	for _, args := range cmds {
		if out, err := run(ctx, args[0], args[1:]...); err != nil {
			n.logger.Error("Network setup step failed", "cmd", args, "out", out, "err", err)
			n.pool.Release(podUID)
			_ = run2(ctx, "ip", "link", "delete", vethHost)
			_ = deleteNetns(ctx, nnsName)
			return "", "", fmt.Errorf("network setup %v: %s: %w", args, out, err)
		}
	}

	n.logger.Info("Pod network ready", "pod", podUID, "ip", podIP, "gateway", gateway)
	return netnsPath, podIP, nil
}

// Teardown removes the veth pair, releases the IP, and deletes the netns.
func (n *LinuxNetworkManager) Teardown(ctx context.Context, podUID, _, _ string) error {
	n.logger.Debug("Tearing down network namespace", "pod", podUID)

	netnsPath := filepath.Join(n.baseDir, netnsName(podUID))
	if _, err := os.Stat(netnsPath); os.IsNotExist(err) {
		n.pool.Release(podUID)
		return nil
	}

	vethHost := vethName(podUID)

	// Delete the host-side veth first - this also removes the peer inside
	// the netns. If it fails (e.g. already gone), proceed anyway.
	if out, err := run(ctx, "ip", "link", "delete", vethHost); err != nil {
		n.logger.Warn("Failed to delete veth (may already be gone)", "veth", vethHost, "out", out)
	}

	if err := deleteNetns(ctx, netnsName(podUID)); err != nil {
		n.logger.Error("Failed to delete netns", "pod", podUID, "err", err)
		n.pool.Release(podUID)
		return err
	}

	// Belt-and-suspenders: if the veth somehow survived the netns delete
	// (kernel moved the peer back to host ns), remove it now.
	_ = run2(ctx, "ip", "link", "delete", vethHost)

	n.pool.Release(podUID)
	return nil
}

// ensureBridge creates the perigeos0 bridge with gateway IP, enables forwarding + MASQUERADE.
func (n *LinuxNetworkManager) ensureBridge() error {
	ctx := context.Background()
	gateway := n.pool.Gateway()

	if _, err := run(ctx, "ip", "link", "show", bridgeName); err == nil {
		n.cleanOrphanedVeths(ctx)
		return nil // already exists
	}

	cmds := [][]string{
		{"ip", "link", "add", "name", bridgeName, "type", "bridge"},
		{"ip", "addr", "add", gateway + podSubnet, "dev", bridgeName},
		{"ip", "link", "set", bridgeName, "up"},
		{"sysctl", "-w", "net.ipv4.ip_forward=1"},
		{"iptables", "-t", "nat", "-A", "POSTROUTING", "-s", podCIDR, "!", "-o", bridgeName, "-j", "MASQUERADE"},
		{"iptables", "-A", "FORWARD", "-i", bridgeName, "-j", "ACCEPT"},
		{"iptables", "-A", "FORWARD", "-o", bridgeName, "-j", "ACCEPT"},
	}

	for _, args := range cmds {
		if out, err := run(ctx, args[0], args[1:]...); err != nil {
			n.logger.Warn("Bridge setup step failed (non-fatal)", "cmd", args, "out", out, "err", err)
		}
	}

	n.logger.Info("Bridge created", "bridge", bridgeName, "gateway", gateway)
	return nil
}

// recoverPodIP reads the IP assigned to eth0 in the pod's netns.
func (n *LinuxNetworkManager) recoverPodIP(ctx context.Context, podUID string) (string, error) {
	out, err := run(ctx, "ip", "netns", "exec", netnsName(podUID), "ip", "-4", "-o", "addr", "show", "dev", "eth0")
	if err != nil {
		return "", fmt.Errorf("ip addr show: %s: %w", out, err)
	}

	// "2: eth0    inet 10.88.0.2/16 scope global eth0\n"
	var idx, iface, inet, cidr string
	fmt.Sscanf(out, "%s %s %s %s", &idx, &iface, &inet, &cidr)
	if inet != "inet" || cidr == "" {
		return "", fmt.Errorf("could not parse IP from: %s", out)
	}

	ip, _, err := net.ParseCIDR(cidr)
	if err != nil {
		return "", err
	}
	return ip.String(), nil
}

// vethName produces a stable ≤15-char interface name for a pod UID.
func vethName(podUID string) string {
	// Take last 8 non-dash chars of the UID
	clean := ""
	for i := len(podUID) - 1; i >= 0 && len(clean) < 8; i-- {
		if podUID[i] != '-' {
			clean = string(podUID[i]) + clean
		}
	}
	return "veth" + clean
}

func run(ctx context.Context, name string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func run2(ctx context.Context, name string, args ...string) error {
	_, err := run(ctx, name, args...)
	return err
}

// createNetns creates a named network namespace and brings up loopback.
// Shared by all NetworkManager implementations.
func createNetns(ctx context.Context, podUID string) error {
	if out, err := run(ctx, "ip", "netns", "add", podUID); err != nil {
		return fmt.Errorf("ip netns add %s: %s: %w", podUID, out, err)
	}
	// Bring up lo - CNI plugins and systemd-nspawn don't do this.
	if out, err := run(ctx, "ip", "netns", "exec", podUID, "ip", "link", "set", "lo", "up"); err != nil {
		return fmt.Errorf("ip netns exec %s lo up: %s: %w", podUID, out, err)
	}
	return nil
}

// deleteNetns removes a named network namespace.
// Shared by all NetworkManager implementations.
func deleteNetns(ctx context.Context, podUID string) error {
	if out, err := run(ctx, "ip", "netns", "delete", podUID); err != nil {
		return fmt.Errorf("ip netns delete %s: %s: %w", podUID, out, err)
	}
	return nil
}

// cleanOrphanedVeths removes veths attached to the perigeos bridge whose
// peer netns no longer exists. Called on startup to clean up leaks from
// previous runs where Teardown failed mid-way.
func (n *LinuxNetworkManager) cleanOrphanedVeths(ctx context.Context) {
	// List all interfaces enslaved to the bridge.
	out, err := run(ctx, "ip", "link", "show", "master", bridgeName)
	if err != nil {
		return
	}
	for _, line := range strings.Split(out, "\n") {
		// Lines look like: "8: vethcaf8a1e5@if2: <...>"
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		ifName := strings.Split(strings.TrimRight(fields[1], ":"), "@")[0]
		if !strings.HasPrefix(ifName, "veth") {
			continue
		}
		// Extract the uid suffix and check if its netns still exists.
		// vethName() uses the last 8 non-dash chars of the UID as the suffix,
		// and netns files are named by full UID - so a suffix match is correct.
		suffix := strings.TrimPrefix(ifName, "veth")
		entries, err := os.ReadDir(n.baseDir)
		if err != nil {
			continue
		}
		found := false
		for _, e := range entries {
			if strings.HasSuffix(e.Name(), suffix) {
				found = true
				break
			}
		}
		if !found {
			n.logger.Info("Removing orphaned veth", "iface", ifName)
			_ = run2(ctx, "ip", "link", "delete", ifName)
		}
	}
}
