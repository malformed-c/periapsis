package cgroup

import (
	"testing"

	"github.com/containerd/cgroups/v3/cgroup2"
	dbusv5 "github.com/godbus/dbus/v5"
)

func TestMilliCPUToCPUMax(t *testing.T) {
	if got := MilliCPUToCPUMax(0); got != cgroup2.NewCPUMax(nil, nil) {
		t.Fatalf("milli=0 got %q, want unlimited", got)
	}
	// 500 millicores → quota 50000us, default period 100000us → "50000 100000"
	if got := MilliCPUToCPUMax(500); string(got) != "50000 100000" {
		t.Fatalf("milli=500 got %q, want \"50000 100000\"", got)
	}
}

func TestBuildSystemdProperties_Empty(t *testing.T) {
	if got := BuildSystemdProperties(nil); len(got) != 0 {
		t.Fatalf("nil resources: got %d props, want 0", len(got))
	}
	if got := BuildSystemdProperties(&cgroup2.Resources{}); len(got) != 0 {
		t.Fatalf("empty resources: got %d props, want 0", len(got))
	}
}

func TestBuildSystemdProperties_CPUAndMemory(t *testing.T) {
	weight := uint64(39)
	memMax := int64(128 << 20)
	res := &cgroup2.Resources{
		CPU: &cgroup2.CPU{
			Weight: &weight,
			Max:    MilliCPUToCPUMax(2000), // 2 cores → quota 200000, period 100000
		},
		Memory: &cgroup2.Memory{Max: &memMax},
		Pids:   &cgroup2.Pids{Max: 4096},
	}

	props := BuildSystemdProperties(res)

	// Collect names for the assertion messages.
	names := make(map[string]dbusv5.Variant, len(props))
	for _, p := range props {
		names[p.Name] = p.Value
	}

	if v, ok := names["CPUWeight"]; !ok || v.Value().(uint64) != 39 {
		t.Errorf("CPUWeight: got %+v, want 39", v)
	}
	// 2 cores → 2,000,000 us per wall-clock second.
	if v, ok := names["CPUQuotaPerSecUSec"]; !ok || v.Value().(uint64) != 2_000_000 {
		t.Errorf("CPUQuotaPerSecUSec: got %+v, want 2_000_000", v)
	}
	if v, ok := names["MemoryMax"]; !ok || v.Value().(uint64) != uint64(memMax) {
		t.Errorf("MemoryMax: got %+v, want %d", v, memMax)
	}
	if v, ok := names["TasksMax"]; !ok || v.Value().(uint64) != 4096 {
		t.Errorf("TasksMax: got %+v, want 4096", v)
	}
}

func TestCPUMaxRoundTrip_MatchesLegacyFormula(t *testing.T) {
	// Legacy inline formula was: CPUQuotaPerSecUSec = milliCPU * 1000.
	// Verify the cgroup2 round-trip produces the same value for a range.
	for _, milli := range []int64{1, 100, 500, 1000, 2500, 10000} {
		max := MilliCPUToCPUMax(milli)
		got, ok := cpuMaxToQuotaPerSecUS(max)
		if !ok {
			t.Fatalf("milli=%d: cpuMaxToQuotaPerSecUS failed", milli)
		}
		want := uint64(milli * 1000)
		if got != want {
			t.Errorf("milli=%d: got %d, want %d", milli, got, want)
		}
	}
}
