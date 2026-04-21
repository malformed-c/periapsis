package network

import (
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
)

// newTestConstellationManager creates a ConstellationNetworkManager with a
// temp ConfDir and a fake BinDir. It bypasses systemd and CNI binary checks
// so it can run anywhere.
func newTestConstellationManager(t *testing.T) *ConstellationNetworkManager {
	t.Helper()
	confDir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	m := &ConstellationNetworkManager{
		logger: logger,
		cfg: ConstellationConfig{
			ConfDir: confDir,
			BinDir:  "/opt/cni/bin",
		},
	}
	return m
}

// --- writeConfig ---

func TestWriteConfig_JSONShape(t *testing.T) {
	m := newTestConstellationManager(t)
	confFile, err := m.writeConfig()
	if err != nil {
		t.Fatalf("writeConfig: %v", err)
	}

	data, err := os.ReadFile(confFile)
	if err != nil {
		t.Fatalf("read confFile: %v", err)
	}

	var top map[string]any
	if err := json.Unmarshal(data, &top); err != nil {
		t.Fatalf("invalid JSON: %v\n%s", err, data)
	}

	if top["cniVersion"] != cniVersion {
		t.Errorf("cniVersion = %v, want %s", top["cniVersion"], cniVersion)
	}
	if top["name"] != "constellation" {
		t.Errorf("name = %v, want constellation", top["name"])
	}

	plugins, ok := top["plugins"].([]any)
	if !ok || len(plugins) != 1 {
		t.Fatalf("plugins: expected 1-element array, got %v", top["plugins"])
	}

	plugin := plugins[0].(map[string]any)
	if plugin["type"] != constellationBinName {
		t.Errorf("plugin.type = %v, want %s", plugin["type"], constellationBinName)
	}
	if _, hasDebug := plugin["enable-debug"]; hasDebug {
		t.Error("enable-debug should be omitted when false")
	}
}

func TestWriteConfig_Debug(t *testing.T) {
	m := newTestConstellationManager(t)
	m.cfg.Debug = true

	confFile, err := m.writeConfig()
	if err != nil {
		t.Fatalf("writeConfig: %v", err)
	}

	data, _ := os.ReadFile(confFile)
	var top map[string]any
	json.Unmarshal(data, &top)

	plugins := top["plugins"].([]any)
	plugin := plugins[0].(map[string]any)
	if plugin["enable-debug"] != true {
		t.Errorf("enable-debug = %v, want true", plugin["enable-debug"])
	}
}

func TestWriteConfig_FileLocation(t *testing.T) {
	m := newTestConstellationManager(t)
	confFile, err := m.writeConfig()
	if err != nil {
		t.Fatalf("writeConfig: %v", err)
	}

	expectedName := "constellation.conflist"
	if filepath.Base(confFile) != expectedName {
		t.Errorf("confFile base = %q, want %q", filepath.Base(confFile), expectedName)
	}
	if filepath.Dir(confFile) != m.cfg.ConfDir {
		t.Errorf("confFile dir = %q, want %q", filepath.Dir(confFile), m.cfg.ConfDir)
	}
}

func TestWriteConfig_CreatesConfDir(t *testing.T) {
	base := t.TempDir()
	nestedDir := filepath.Join(base, "does", "not", "exist", "yet")

	m := newTestConstellationManager(t)
	m.cfg.ConfDir = nestedDir

	if _, err := m.writeConfig(); err != nil {
		t.Fatalf("writeConfig should create missing dirs: %v", err)
	}
	if _, err := os.Stat(nestedDir); err != nil {
		t.Errorf("conf dir not created: %v", err)
	}
}

func TestWriteConfig_Idempotent(t *testing.T) {
	m := newTestConstellationManager(t)

	f1, err := m.writeConfig()
	if err != nil {
		t.Fatal(err)
	}
	f2, err := m.writeConfig()
	if err != nil {
		t.Fatal(err)
	}
	if f1 != f2 {
		t.Errorf("second writeConfig produced different path: %q vs %q", f1, f2)
	}

	data1, _ := os.ReadFile(f1)
	data2, _ := os.ReadFile(f2)
	if string(data1) != string(data2) {
		t.Error("second writeConfig produced different content")
	}
}

// --- runtimeConf ---

func TestRuntimeConf_Fields(t *testing.T) {
	m := newTestConstellationManager(t)
	rt := m.runtimeConf("pod-uid-abc", "kube-system", "coredns", "/var/run/netns/peri-pod-uid-abc", "")

	if rt.ContainerID != "pod-uid-abc" {
		t.Errorf("ContainerID = %q", rt.ContainerID)
	}
	if rt.NetNS != "/var/run/netns/peri-pod-uid-abc" {
		t.Errorf("NetNS = %q", rt.NetNS)
	}
	if rt.IfName != "eth0" {
		t.Errorf("IfName = %q", rt.IfName)
	}
}

func TestRuntimeConf_CNIArgs(t *testing.T) {
	m := newTestConstellationManager(t)
	rt := m.runtimeConf("uid-1", "my-ns", "my-pod", "/var/run/netns/peri-uid-1", "")

	argMap := make(map[string]string)
	for _, kv := range rt.Args {
		argMap[kv[0]] = kv[1]
	}

	if argMap["K8S_POD_NAMESPACE"] != "my-ns" {
		t.Errorf("K8S_POD_NAMESPACE = %q, want my-ns", argMap["K8S_POD_NAMESPACE"])
	}
	if argMap["K8S_POD_NAME"] != "my-pod" {
		t.Errorf("K8S_POD_NAME = %q, want my-pod", argMap["K8S_POD_NAME"])
	}
	if argMap["K8S_POD_UID"] != "uid-1" {
		t.Errorf("K8S_POD_UID = %q, want uid-1", argMap["K8S_POD_UID"])
	}

	// Without a nodeName, K8S_POD_NODE_NAME should be absent.
	if _, has := argMap["K8S_POD_NODE_NAME"]; has {
		t.Error("K8S_POD_NODE_NAME should be absent when nodeName is empty")
	}
}

func TestRuntimeConf_CNIArgs_WithNodeName(t *testing.T) {
	m := newTestConstellationManager(t)
	rt := m.runtimeConf("uid-2", "prod", "web", "/var/run/netns/peri-uid-2", "pawn-worker-03")

	argMap := make(map[string]string)
	for _, kv := range rt.Args {
		argMap[kv[0]] = kv[1]
	}

	// Standard args should still be present.
	if argMap["K8S_POD_NAMESPACE"] != "prod" {
		t.Errorf("K8S_POD_NAMESPACE = %q, want prod", argMap["K8S_POD_NAMESPACE"])
	}
	if argMap["K8S_POD_NAME"] != "web" {
		t.Errorf("K8S_POD_NAME = %q, want web", argMap["K8S_POD_NAME"])
	}
	if argMap["K8S_POD_UID"] != "uid-2" {
		t.Errorf("K8S_POD_UID = %q, want uid-2", argMap["K8S_POD_UID"])
	}

	// K8S_POD_NODE_NAME should be present with the correct value.
	if argMap["K8S_POD_NODE_NAME"] != "pawn-worker-03" {
		t.Errorf("K8S_POD_NODE_NAME = %q, want pawn-worker-03", argMap["K8S_POD_NODE_NAME"])
	}
}

func TestRuntimeConf_CNIArgs_NodeNameAffectsArgCount(t *testing.T) {
	m := newTestConstellationManager(t)

	// Without nodeName: 3 args (namespace, name, uid)
	rtNoNode := m.runtimeConf("uid-x", "ns", "pod", "/var/run/netns/peri-uid-x", "")
	if len(rtNoNode.Args) != 3 {
		t.Errorf("without nodeName: expected 3 args, got %d", len(rtNoNode.Args))
	}

	// With nodeName: 4 args (namespace, name, uid, node_name)
	rtWithNode := m.runtimeConf("uid-y", "ns", "pod", "/var/run/netns/peri-uid-y", "pawn-01")
	if len(rtWithNode.Args) != 4 {
		t.Errorf("with nodeName: expected 4 args, got %d", len(rtWithNode.Args))
	}
}

// --- ipFromResult ---

func TestIPFromResult_Nil(t *testing.T) {
	if ip := ipFromResult(nil); ip != "" {
		t.Errorf("ipFromResult(nil) = %q, want empty", ip)
	}
}
