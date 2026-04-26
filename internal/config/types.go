// Copyright (C) 2025-2026 Malformed C. All rights reserved.
// SPDX-License-Identifier: BUSL-1.1

package config

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// What we read from TOML
type RawPerigeosConfig struct {
	Global   RawGlobalConfig             `toml:"global"`
	Pawns    map[string]RawPawnConfig    `toml:"pawns"`
	PawnSets map[string]RawPawnSetConfig `toml:"pawn_sets"`
}

type RawGlobalConfig struct {
	PerigeosPort  int    `toml:"PerigeosPort"`
	DefaultCPU    string `toml:"DefaultCPU"`
	DefaultMemory string `toml:"DefaultMemory"`

	// ServerCAPath is the path to the k8s CA certificate used to sign pawn
	// TLS serving certs. Defaults to the k3s CA location.
	ServerCAPath string `toml:"server_ca_path"`

	// ServerCAKeyPath is the path to the k8s CA private key.
	// Defaults to the k3s CA key location.
	ServerCAKeyPath string `toml:"server_ca_key_path"`

	// NodeIP overrides the IP advertised by the primary pawn in
	// Node.status.addresses. If empty, GetOutboundIP() is used.
	NodeIP string `toml:"NodeIP"`

	// Primary enables the primary node for this host.
	// The primary represents the physical host in the cluster - it's where
	// host-level DaemonSets (e.g. constellation-agent) schedule. For hosts
	// without a kubelet, the primary replaces it. For hosts with a kubelet
	// (e.g. k3s), the primary coexists and is labeled on the real node.
	Primary bool `toml:"primary"`

	// CNI configures Constellation CNI integration for the whole host.
	// When present, perigeos uses libcni to call ADD/DEL on the constellation agent.
	// When absent, built-in bridge networking is used per-pawn.
	CNI *RawCNIConfig `toml:"cni"`
}

type RawPawnConfig struct {
	Port   int               `toml:"port"`
	Labels map[string]string `toml:"labels"`
	Taints map[string]string `toml:"taints"`

	// NodeIP overrides the IP advertised in Node.status.addresses and used as
	// HostIP in pod status. Useful when the default outbound IP (determined by
	// routing to 8.8.8.8) is not reachable from the control plane - e.g. when
	// running the control plane inside a podman/kind container.
	NodeIP string `toml:"node_ip"`

	// CreateConcurrency limits how many pod creation sagas run in parallel
	// per pawn. 0 or unset uses DefaultCreateConcurrency (5).
	CreateConcurrency int `toml:"create_concurrency"`

	CPU                 string `toml:"cpu"`
	Memory              string `toml:"memory"`
	CPUWeight           uint64 `toml:"cpu_weight"`             // 1-10000
	IOReadBandwidthMax  string `toml:"io_read_bandwidth_max"`  // e.g. "10M"
	IOWriteBandwidthMax string `toml:"io_write_bandwidth_max"` // e.g. "10M"
}

// RawCNIConfig is the optional [global.cni] section.
type RawCNIConfig struct {
	// BinDir is the directory containing the constellation-cni binary.
	// Defaults to /opt/cni/bin.
	BinDir string `toml:"bin_dir"`

	// ConfDir is where the generated .conflist file is written.
	// Defaults to /etc/cni/net.d/constellation.
	ConfDir string `toml:"conf_dir"`

	// Debug enables verbose logging in the CNI plugin.
	Debug bool `toml:"debug"`
}

// RawPawnSetConfig embeds RawPawnConfig so it inherits all fields (cpu, memory, etc)
type RawPawnSetConfig struct {
	Count int `toml:"count"`
	RawPawnConfig
}

// What we want
type PerigeosConfig struct {
	Global GlobalConfig
	Pawns  []PawnConfig
}

type GlobalConfig struct {
	PerigeosPort  int
	DefaultCPU    resource.Quantity
	DefaultMemory resource.Quantity

	// BaseDir is the root of all Perigeos state on disk.
	// Default: /var/lib/apsis/perigeos. Override with --base-dir for dev/CI.
	BaseDir string

	// ServerCAPath / ServerCAKeyPath are the k8s CA cert and key used to sign
	// pawn TLS serving certificates. Defaults to the k3s CA paths.
	ServerCAPath    string
	ServerCAKeyPath string

	// Primary enables the primary node for this host.
	Primary bool

	// CNI is non-nil when Constellation CNI integration is enabled for this host.
	CNI *CNIConfig
}

type PawnConfig struct {
	Name string

	Port int

	// NodeIP is the IP advertised to the apiserver. If empty, GetOutboundIP() is used.
	NodeIP string

	// BaseDir is the root of all apsis state on disk. Copied from GlobalConfig.
	BaseDir string

	// IsPrimary marks this pawn as the host's primary node.
	// Primary nodes get periapsis.io/primary=true and
	// node-role.kubernetes.io/primary labels instead of the pawn role.
	IsPrimary bool

	Labels map[string]string
	Taints []corev1.Taint

	// CreateConcurrency limits parallel pod creation sagas per pawn. 0 = default (5).
	CreateConcurrency int

	// Let's trust Kubernetes
	CPU                 resource.Quantity
	Memory              resource.Quantity
	CPUWeight           uint64
	IOReadBandwidthMax  resource.Quantity
	IOWriteBandwidthMax resource.Quantity
}

// CNIConfig is the processed form of RawCNIConfig.
type CNIConfig struct {
	BinDir  string
	ConfDir string
	Debug   bool
}
