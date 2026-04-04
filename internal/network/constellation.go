package network

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"regexp"

	"github.com/containernetworking/cni/libcni"
	cniTypes "github.com/containernetworking/cni/pkg/types"
	types040 "github.com/containernetworking/cni/pkg/types/040"
)

// uuidRE matches a Kubernetes pod UID (UUID v4 format: 8-4-4-4-12 hex chars).
// Entries in /var/run/netns/ that don't match this pattern (e.g. podman's
// "netns-*" names) are foreign and must not be swept.
var uuidRE = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)


const (
	// constellationBinName is the CNI plugin binary that Constellation ships.
	constellationBinName = "cilium-cni"

	// cniVersion is the CNI spec version Constellation targets.
	cniVersion = "0.3.1"

	// netnsDir is where perigeos creates named network namespaces.
	// Must match the baseDir in LinuxNetworkManager so teardown is symmetric.
	netnsDir = "/var/run/netns"

	// agentSocketPath is the Cilium agent socket. One agent per host.
	agentSocketPath = "/var/run/cilium/cilium.sock"

)

// constellationNetConfig is the JSON structure written to the CNI conf dir.
type constellationNetConfig struct {
	CNIVersion  string `json:"cniVersion"`
	Name        string `json:"name"`
	Type        string `json:"type"`
	EnableDebug bool   `json:"enable-debug,omitempty"`
}

// ConstellationConfig holds the host-level CNI consumer settings.
// This configures perigeos as a CNI *client* — it does not configure the
// CNI implementation (constellation/Cilium). Agent deployment and
// configuration are handled separately.
type ConstellationConfig struct {
	// ConfDir is where the generated CNI conflist file is written.
	// Defaults to /etc/cni/net.d/constellation
	ConfDir string

	// BinDir is where the constellation-cni binary lives.
	// Defaults to /opt/cni/bin
	BinDir string

	// Debug enables CNI plugin debug logging.
	Debug bool
}

// ConstellationNetworkManager implements NetworkManager using libcni,
// delegating all datapath work to a running constellation-agent DaemonSet.
type ConstellationNetworkManager struct {
	logger   *slog.Logger
	cfg      ConstellationConfig
	cniConf  *libcni.NetworkConfigList
	cniAPI   libcni.CNI
	confFile string
	cniSem   chan struct{} // limits concurrent CNI ADD/DEL calls
}

// DefaultCNIConcurrency is the max parallel CNI calls to the agent.
// The agent's endpoint-queue-size defaults to 25; we stay under that
// to avoid overwhelming its local API socket.
const DefaultCNIConcurrency = 20

var _ NetworkManager = (*ConstellationNetworkManager)(nil)

// NewConstellationNetworkManager initialises the CNI consumer:
//  1. Writes the CNI conflist to ConfDir
//  2. Checks for the agent socket (informational — not a hard gate)
//  3. Loads the conflist via libcni
func NewConstellationNetworkManager(ctx context.Context, logger *slog.Logger, cfg ConstellationConfig) (*ConstellationNetworkManager, error) {
	if cfg.ConfDir == "" {
		cfg.ConfDir = "/etc/cni/net.d/constellation"
	}
	if cfg.BinDir == "" {
		cfg.BinDir = "/opt/cni/bin"
	}

	m := &ConstellationNetworkManager{
		logger: logger,
		cfg:    cfg,
		cniAPI: libcni.NewCNIConfigWithCacheDir([]string{cfg.BinDir}, "", nil),
		cniSem: make(chan struct{}, DefaultCNIConcurrency),
	}

	if _, err := os.Stat(agentSocketPath); err == nil {
		logger.Info("constellation-agent socket already present")
	} else {
		logger.Info("constellation-agent socket not yet present; agent must be deployed separately")
	}

	confFile, err := m.writeConfig()
	if err != nil {
		return nil, fmt.Errorf("writing CNI config: %w", err)
	}
	m.confFile = confFile

	netConf, err := libcni.ConfListFromFile(confFile)
	if err != nil {
		return nil, fmt.Errorf("loading CNI config from %s: %w", confFile, err)
	}
	m.cniConf = netConf

	logger.Info("Constellation CNI manager ready", "conf", confFile)
	return m, nil
}

// writeConfig generates and writes the CNI conflist JSON file.
func (m *ConstellationNetworkManager) writeConfig() (string, error) {
	if err := os.MkdirAll(m.cfg.ConfDir, 0755); err != nil {
		return "", fmt.Errorf("mkdir %s: %w", m.cfg.ConfDir, err)
	}
	netCfg := constellationNetConfig{
		CNIVersion:  cniVersion,
		Name:        "constellation",
		Type:        constellationBinName,
		EnableDebug: m.cfg.Debug,
	}
	confList := map[string]any{
		"cniVersion": cniVersion,
		"name":       netCfg.Name,
		"plugins":    []any{netCfg},
	}
	data, err := json.MarshalIndent(confList, "", "  ")
	if err != nil {
		return "", fmt.Errorf("marshal CNI config: %w", err)
	}
	confFile := m.cfg.ConfDir + "/constellation.conflist"
	if err := os.WriteFile(confFile, data, 0644); err != nil {
		return "", fmt.Errorf("write CNI config to %s: %w", confFile, err)
	}
	m.logger.Debug("Wrote CNI config", "path", confFile)
	return confFile, nil
}

// Setup creates a network namespace, calls CNI ADD, and returns the netns path
// and the pod IP assigned by constellation-agent.
func (m *ConstellationNetworkManager) Setup(ctx context.Context, podUID, namespace, name, nodeName string) (string, string, error) {
	netnsPath := netnsDir + "/" + podUID

	if _, err := os.Stat(netnsPath); err == nil {
		ip, err := m.recoverIP(ctx, podUID, namespace, name, netnsPath)
		if err != nil {
			m.logger.Warn("Could not recover pod IP via CNI cache",
				"pod", podUID, "err", err)
			return netnsPath, "", nil
		}
		m.logger.Info("Network namespace already exists, recovered IP", "pod", podUID, "ip", ip)
		return netnsPath, ip, nil
	}

	if err := createNetns(ctx, podUID); err != nil {
		return "", "", err
	}

	rt := m.runtimeConf(podUID, namespace, name, netnsPath, nodeName)

	select {
	case m.cniSem <- struct{}{}:
	case <-ctx.Done():
		_ = deleteNetns(context.Background(), podUID)
		return "", "", ctx.Err()
	}
	result, err := m.cniAPI.AddNetworkList(ctx, m.cniConf, rt)
	<-m.cniSem

	if err != nil {
		// CNI ADD may have partially created Cilium endpoint state (lxc interface)
		// before returning an error. Call DEL to clean up that state before removing
		// the netns, otherwise the lxc interface leaks until the next process restart.
		// Use a background context — the original ctx may already be cancelled.
		cleanCtx := context.Background()
		_ = m.cniAPI.DelNetworkList(cleanCtx, m.cniConf, rt)
		_ = deleteNetns(cleanCtx, podUID)
		return "", "", fmt.Errorf("CNI ADD for pod %s: %w", podUID, err)
	}

	ip := ipFromResult(result)
	m.logger.Info("Pod network ready via Constellation CNI",
		"pod", podUID, "ip", ip, "netns", netnsPath)
	return netnsPath, ip, nil
}

// Teardown calls CNI DEL and removes the network namespace.
// CNI DEL is always attempted even if the netns is already gone — Cilium
// maintains endpoint state (lxc interface) independently of the netns file,
// so skipping DEL when the netns is missing causes a permanent lxc leak.
func (m *ConstellationNetworkManager) Teardown(ctx context.Context, podUID, namespace, name string) error {
	netnsPath := netnsDir + "/" + podUID
	netnsExists := true
	if _, err := os.Stat(netnsPath); os.IsNotExist(err) {
		netnsExists = false
	}

	rt := m.runtimeConf(podUID, namespace, name, netnsPath, "")

	select {
	case m.cniSem <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	err := m.cniAPI.DelNetworkList(ctx, m.cniConf, rt)
	<-m.cniSem

	if err != nil {
		m.logger.Warn("CNI DEL failed (proceeding with netns cleanup)",
			"pod", podUID, "err", err)
	}

	if !netnsExists {
		return nil
	}
	if err := deleteNetns(ctx, podUID); err != nil {
		return err
	}
	return nil
}

func (m *ConstellationNetworkManager) recoverIP(ctx context.Context, podUID, namespace, name, netnsPath string) (string, error) {
	rt := m.runtimeConf(podUID, namespace, name, netnsPath, "")
	result, err := m.cniAPI.GetNetworkListCachedResult(m.cniConf, rt)
	if err != nil {
		return "", err
	}
	return ipFromResult(result), nil
}

func (m *ConstellationNetworkManager) runtimeConf(podUID, namespace, name, netnsPath, nodeName string) *libcni.RuntimeConf {
	args := [][2]string{
		{"K8S_POD_NAMESPACE", namespace},
		{"K8S_POD_NAME", name},
		{"K8S_POD_UID", podUID},
	}
	if nodeName != "" {
		args = append(args, [2]string{"K8S_POD_NODE_NAME", nodeName})
	}
	return &libcni.RuntimeConf{
		ContainerID: podUID,
		NetNS:       netnsPath,
		IfName:      "eth0",
		Args:        args,
	}
}

// SweepStaleNetns removes network namespaces that don't belong to any
// active pod. Called at startup after HydrateFromRuntime to clean up
// orphans left by ghost pods that were never properly torn down.
func (m *ConstellationNetworkManager) SweepStaleNetns(ctx context.Context, activeUIDs map[string]struct{}) {
	entries, err := os.ReadDir(netnsDir)
	if err != nil {
		return
	}

	var cleaned int
	for _, e := range entries {
		uid := e.Name()
		// Only sweep entries that look like pod UIDs. Skip foreign netns entries
		// (e.g. podman's "netns-*" names) to avoid destroying other consumers
		// of the shared /var/run/netns directory.
		if !uuidRE.MatchString(uid) {
			continue
		}
		if _, ok := activeUIDs[uid]; ok {
			continue
		}
		// Run CNI DEL first to clean up agent-side state, then delete netns.
		rt := m.runtimeConf(uid, "", "", netnsDir+"/"+uid, "")
		_ = m.cniAPI.DelNetworkList(ctx, m.cniConf, rt)
		if err := deleteNetns(ctx, uid); err != nil {
			m.logger.Warn("Failed to delete stale netns", "uid", uid, "err", err)
			continue
		}
		cleaned++
	}
	if cleaned > 0 {
		m.logger.Info("Swept stale network namespaces", "count", cleaned)
	}
}

func ipFromResult(result cniTypes.Result) string {
	if result == nil {
		return ""
	}
	r, err := types040.NewResultFromResult(result)
	if err != nil {
		return ""
	}
	for _, ip := range r.IPs {
		if ip.Address.IP.To4() != nil {
			return ip.Address.IP.String()
		}
	}
	return ""
}
