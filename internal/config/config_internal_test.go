package config

import (
	"os"
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"
)

// ─── buildCNIConfig ───────────────────────────────────────────────────────────

func TestBuildCNIConfig_NilRaw_NoSocket_ReturnsNil(t *testing.T) {
	// Skip if a real constellation-agent socket exists on this host — the
	// auto-detect logic would return non-nil, which is correct behaviour but
	// defeats the purpose of this test.
	if _, err := os.Stat(constellationSocketPath()); err == nil {
		t.Skip("constellation-agent socket present on host; skipping auto-detect nil test")
	}
	got := buildCNIConfig(nil)
	if got != nil {
		t.Errorf("expected nil when no raw config and no socket, got %+v", got)
	}
}

func TestBuildCNIConfig_ExplicitBlock_Defaults(t *testing.T) {
	raw := &RawCNIConfig{} // all empty → should fill defaults
	got := buildCNIConfig(raw)
	if got == nil {
		t.Fatal("expected non-nil CNIConfig for explicit (empty) block")
	}
	if got.BinDir != "/opt/cni/bin" {
		t.Errorf("BinDir = %q, want /opt/cni/bin", got.BinDir)
	}
	if got.ConfDir != "/etc/cni/net.d/constellation" {
		t.Errorf("ConfDir = %q, want /etc/cni/net.d/constellation", got.ConfDir)
	}
	if got.Debug {
		t.Error("Debug should default to false")
	}
}

func TestBuildCNIConfig_ExplicitBlock_Overrides(t *testing.T) {
	raw := &RawCNIConfig{
		BinDir:  "/custom/cni/bin",
		ConfDir: "/custom/net.d",
		Debug:   true,
	}
	got := buildCNIConfig(raw)
	if got == nil {
		t.Fatal("expected non-nil CNIConfig")
	}
	if got.BinDir != "/custom/cni/bin" {
		t.Errorf("BinDir = %q", got.BinDir)
	}
	if got.ConfDir != "/custom/net.d" {
		t.Errorf("ConfDir = %q", got.ConfDir)
	}
	if !got.Debug {
		t.Error("Debug should be true")
	}
}

// ─── parseCPU ─────────────────────────────────────────────────────────────────

func TestParseCPU_KubernetesQuantity(t *testing.T) {
	def := resource.MustParse("100m")

	cases := []struct {
		input   string
		wantStr string
	}{
		{"500m", "500m"},
		{"1", "1"},
	}
	for _, c := range cases {
		q, err := parseCPU(c.input, def)
		if err != nil {
			t.Errorf("parseCPU(%q): unexpected error: %v", c.input, err)
			continue
		}
		if q.String() != c.wantStr {
			t.Errorf("parseCPU(%q) = %q, want %q", c.input, q.String(), c.wantStr)
		}
	}
}

func TestParseCPU_Percentage(t *testing.T) {
	def := resource.MustParse("100m")

	cases := []struct {
		input      string
		wantMillis int64
	}{
		{"100%", 1000},
		{"50%", 500},
		{"200%", 2000},
		{"10%", 100},
	}
	for _, c := range cases {
		q, err := parseCPU(c.input, def)
		if err != nil {
			t.Errorf("parseCPU(%q): unexpected error: %v", c.input, err)
			continue
		}
		if got := q.MilliValue(); got != c.wantMillis {
			t.Errorf("parseCPU(%q) = %dm, want %dm", c.input, got, c.wantMillis)
		}
	}
}

func TestParseCPU_Empty_ReturnsDefault(t *testing.T) {
	def := resource.MustParse("250m")
	q, err := parseCPU("", def)
	if err != nil {
		t.Fatalf("parseCPU(\"\") error: %v", err)
	}
	if q.Cmp(def) != 0 {
		t.Errorf("parseCPU(\"\") = %q, want default %q", q.String(), def.String())
	}
}

func TestParseCPU_InvalidFormat(t *testing.T) {
	def := resource.MustParse("100m")
	_, err := parseCPU("notcpu", def)
	if err == nil {
		t.Error("expected error for invalid CPU format")
	}
}

func TestDeriveCPUWeight(t *testing.T) {
	tests := []struct {
		name       string
		cpu        string
		configured uint64
		want       uint64
	}{
		{name: "configured weight wins", cpu: "500m", configured: 777, want: 777},
		{name: "derive from one core", cpu: "1000m", want: 39},
		{name: "derive from half core", cpu: "500m", want: 20},
		{name: "derive tiny cpu clamps to min share", cpu: "1m", want: 1},
		{name: "zero cpu yields zero", cpu: "0", want: 0},
		{name: "large cpu maps high weight", cpu: "200000m", want: 7812},
		{name: "huge cpu clamps to max weight", cpu: "1000000m", want: 10000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cpu := resource.MustParse(tt.cpu)
			if got := deriveCPUWeight(cpu, tt.configured); got != tt.want {
				t.Fatalf("deriveCPUWeight(%s, %d) = %d, want %d", tt.cpu, tt.configured, got, tt.want)
			}
		})
	}
}

// ─── Process – pawn set expansion ────────────────────────────────────────────

func defaultPawnCfg() RawPawnConfig {
	return RawPawnConfig{IOReadBandwidthMax: "10M", IOWriteBandwidthMax: "10M"}
}

func baseRaw(port int) RawPerigeosConfig {
	return RawPerigeosConfig{
		Global: RawGlobalConfig{
			PerigeosPort:  port,
			DefaultCPU:    "100m",
			DefaultMemory: "128Mi",
		},
	}
}

func pawnNames(cfg *PerigeosConfig) map[string]bool {
	m := make(map[string]bool, len(cfg.Pawns))
	for _, p := range cfg.Pawns {
		m[p.Name] = true
	}
	return m
}

func TestProcess_PawnSet_SingleDigitNaming(t *testing.T) {
	raw := baseRaw(10000)
	raw.PawnSets = map[string]RawPawnSetConfig{
		"worker": {Count: 3, RawPawnConfig: defaultPawnCfg()},
	}

	cfg, err := raw.Process("/tmp")
	if err != nil {
		t.Fatalf("Process: %v", err)
	}

	names := pawnNames(cfg)
	for _, want := range []string{"worker-0", "worker-1", "worker-2"} {
		if !names[want] {
			t.Errorf("missing pawn %q in %v", want, names)
		}
	}
}

func TestProcess_PawnSet_ZeroPaddedAt100(t *testing.T) {
	raw := baseRaw(10000)
	raw.PawnSets = map[string]RawPawnSetConfig{
		"rack": {Count: 100, RawPawnConfig: defaultPawnCfg()},
	}

	cfg, err := raw.Process("/tmp")
	if err != nil {
		t.Fatalf("Process: %v", err)
	}

	names := pawnNames(cfg)
	if !names["rack-00"] {
		t.Error("expected rack-00 for a 100-member set")
	}
	if !names["rack-99"] {
		t.Error("expected rack-99 for a 100-member set")
	}
	if names["rack-0"] {
		t.Error("rack-0 (unpadded) should not exist in a 100-member set")
	}
}

func TestProcess_PawnSet_CollisionError(t *testing.T) {
	raw := baseRaw(10000)
	raw.Pawns = map[string]RawPawnConfig{"worker-0": defaultPawnCfg()}
	raw.PawnSets = map[string]RawPawnSetConfig{
		"worker": {Count: 2, RawPawnConfig: defaultPawnCfg()},
	}

	_, err := raw.Process("/tmp")
	if err == nil {
		t.Error("expected error for pawn name collision between manual pawn and set")
	}
}

func TestProcess_PawnSet_ZeroCount_Ignored(t *testing.T) {
	raw := baseRaw(10000)
	raw.PawnSets = map[string]RawPawnSetConfig{
		"ghost": {Count: 0, RawPawnConfig: defaultPawnCfg()},
	}

	cfg, err := raw.Process("/tmp")
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(cfg.Pawns) != 0 {
		t.Errorf("expected 0 pawns, got %d", len(cfg.Pawns))
	}
}

// ─── Process – port assignment ────────────────────────────────────────────────

func TestProcess_Ports_NoDuplicates(t *testing.T) {
	raw := baseRaw(10000)
	raw.Pawns = map[string]RawPawnConfig{
		"alpha": defaultPawnCfg(),
		"beta":  defaultPawnCfg(),
		"gamma": defaultPawnCfg(),
	}

	cfg, err := raw.Process("/tmp")
	if err != nil {
		t.Fatalf("Process: %v", err)
	}

	seen := make(map[int]string)
	for _, p := range cfg.Pawns {
		if existing, dup := seen[p.Port]; dup {
			t.Errorf("port %d assigned to both %q and %q", p.Port, existing, p.Name)
		}
		seen[p.Port] = p.Name
		if p.Port <= 10000 {
			t.Errorf("pawn %q port %d ≤ PerigeosPort 10000", p.Name, p.Port)
		}
	}
}

func TestProcess_Ports_ManualPortIsReserved(t *testing.T) {
	raw := baseRaw(10000)
	raw.Pawns = map[string]RawPawnConfig{
		"alpha": {Port: 10002, IOReadBandwidthMax: "10M", IOWriteBandwidthMax: "10M"},
		"beta":  defaultPawnCfg(),
	}

	cfg, err := raw.Process("/tmp")
	if err != nil {
		t.Fatalf("Process: %v", err)
	}

	seen := make(map[int]string)
	for _, p := range cfg.Pawns {
		if existing, dup := seen[p.Port]; dup {
			t.Errorf("port collision: %d → %q and %q", p.Port, existing, p.Name)
		}
		seen[p.Port] = p.Name
	}
}

func TestProcess_Ports_DuplicateManual_Error(t *testing.T) {
	raw := baseRaw(10000)
	raw.Pawns = map[string]RawPawnConfig{
		"alpha": {Port: 10001, IOReadBandwidthMax: "10M", IOWriteBandwidthMax: "10M"},
		"beta":  {Port: 10001, IOReadBandwidthMax: "10M", IOWriteBandwidthMax: "10M"},
	}

	_, err := raw.Process("/tmp")
	if err == nil {
		t.Error("expected error for duplicate manual port")
	}
}
