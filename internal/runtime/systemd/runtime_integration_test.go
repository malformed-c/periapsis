package systemd_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/coreos/go-systemd/v22/dbus"
	dbusv5 "github.com/godbus/dbus/v5"
	"github.com/malformed-c/periapsis/internal/image"
	"github.com/malformed-c/periapsis/internal/runtime"
	rtsd "github.com/malformed-c/periapsis/internal/runtime/systemd"
	"github.com/malformed-c/periapsis/node/api"
)

// requireRoot skips the test if not running as root.
func requireRoot(t *testing.T) {
	t.Helper()
	if os.Getuid() != 0 {
		t.Skip("skipping: requires root (run with: sudo -E go test ./internal/runtime/systemd/...)")
	}
}

// requireSystemd skips if the system dbus is not reachable.
func requireSystemd(t *testing.T) {
	t.Helper()
	conn, err := dbus.NewSystemConnectionContext(context.Background())
	if err != nil {
		t.Skipf("skipping: system dbus unavailable: %v", err)
	}
	conn.Close()
}

// newTestRuntime returns a SystemdRuntime connected to the system bus.
// Test is skipped if not root or dbus is unavailable.
func newTestRuntime(t *testing.T) *rtsd.SystemdRuntime {
	t.Helper()
	requireRoot(t)
	requireSystemd(t)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	im := image.NewImageManager(t.TempDir(), logger)

	rt, err := rtsd.NewSystemdRuntime(context.Background(), pawnName(t), im, logger, runtime.ExecNsenter, nil)
	if err != nil {
		t.Fatalf("NewSystemdRuntime: %v", err)
	}
	t.Cleanup(func() { rt.Close() })
	return rt
}

// pawnName derives a safe systemd-compatible pawn name from the test name.
func pawnName(t *testing.T) string {
	t.Helper()
	r := strings.NewReplacer("/", "-", " ", "-", ".", "-", "_", "-")
	s := strings.ToLower(r.Replace(t.Name()))
	if len(s) > 32 {
		s = s[len(s)-32:]
	}
	return s
}

// spawnSleepUnit starts a transient sleep service with X-Perigeos* properties,
// simulating the unit shape that RunMachine produces (minus nspawn).
// The unit is stopped in t.Cleanup.
func spawnSleepUnit(t *testing.T, rt *rtsd.SystemdRuntime, pawn, podUID, container string, sleepSecs float64) {
	t.Helper()

	conn, err := dbus.NewSystemConnectionContext(context.Background())
	if err != nil {
		t.Fatalf("dbus connect: %v", err)
	}
	defer conn.Close()

	unitName := fmt.Sprintf("perigeos-%s-pod-%s-%s.service", pawn, podUID, container)
	sleepArg := fmt.Sprintf("%.1f", sleepSecs)

	props := []dbus.Property{
		dbus.PropDescription("Test pod " + podUID),
		dbus.PropExecStart([]string{"/usr/bin/sleep", sleepArg}, false),
		{Name: "CollectMode", Value: dbusv5.MakeVariant("inactive-or-failed")},
		{Name: "Environment", Value: dbusv5.MakeVariant([]string{
			"PERIGEOS_META_UID=" + podUID,
			"PERIGEOS_META_NAME=test-pod",
			"PERIGEOS_META_NAMESPACE=default",
			"PERIGEOS_META_NODENAME=" + pawn,
			"PERIGEOS_META_IP=10.88.0.99",
			"PERIGEOS_META_CONTAINER=" + container,
		})},
	}

	ch := make(chan string, 1)
	if _, err := conn.StartTransientUnitContext(context.Background(), unitName, "replace", props, ch); err != nil {
		t.Fatalf("StartTransientUnit(%s): %v", unitName, err)
	}
	if res := <-ch; res != "done" {
		t.Fatalf("start result for %s: %s", unitName, res)
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		rt.StopMachine(ctx, podUID, container) //nolint:errcheck
	})
}

// ─── Tests ────────────────────────────────────────────────────────────────────

func TestSystemd_MachineStatus_Running(t *testing.T) {
	rt := newTestRuntime(t)
	pawn := pawnName(t)
	spawnSleepUnit(t, rt, pawn, "uid-status-running", "main", 300)
	time.Sleep(100 * time.Millisecond)

	state, err := rt.MachineStatus(context.Background(), "uid-status-running", "main")
	if err != nil {
		t.Fatalf("MachineStatus: %v", err)
	}
	if state != runtime.StateRunning {
		t.Errorf("expected StateRunning, got %v", state)
	}
}

func TestSystemd_MachineStatus_Nonexistent(t *testing.T) {
	rt := newTestRuntime(t)

	state, err := rt.MachineStatus(context.Background(), "does-not-exist-uid", "main")
	if err != nil {
		t.Fatalf("MachineStatus returned unexpected error: %v", err)
	}
	if state == runtime.StateRunning {
		t.Errorf("nonexistent unit should not be StateRunning, got %v", state)
	}
}

func TestSystemd_StopMachine(t *testing.T) {
	rt := newTestRuntime(t)
	pawn := pawnName(t)
	spawnSleepUnit(t, rt, pawn, "uid-stop", "main", 300)
	time.Sleep(100 * time.Millisecond)

	if err := rt.StopMachine(context.Background(), "uid-stop", "main"); err != nil {
		t.Fatalf("StopMachine: %v", err)
	}

	time.Sleep(200 * time.Millisecond)
	state, _ := rt.MachineStatus(context.Background(), "uid-stop", "main")
	if state == runtime.StateRunning {
		t.Errorf("after StopMachine, unit should not be Running, got %v", state)
	}
}

func TestSystemd_StopMachine_Idempotent(t *testing.T) {
	rt := newTestRuntime(t)
	pawn := pawnName(t)
	spawnSleepUnit(t, rt, pawn, "uid-stop-idem", "main", 300)
	time.Sleep(100 * time.Millisecond)

	ctx := context.Background()
	_ = rt.StopMachine(ctx, "uid-stop-idem", "main")

	if err := rt.StopMachine(ctx, "uid-stop-idem", "main"); err != nil {
		t.Errorf("second StopMachine should be idempotent, got: %v", err)
	}
}

func TestSystemd_ListManagedMachines(t *testing.T) {
	rt := newTestRuntime(t)
	pawn := pawnName(t)

	pods := []struct{ uid, container string }{
		{"uid-list-1", "app"},
		{"uid-list-2", "sidecar"},
	}
	for _, p := range pods {
		spawnSleepUnit(t, rt, pawn, p.uid, p.container, 300)
	}
	time.Sleep(200 * time.Millisecond)

	machines, err := rt.ListManagedMachines(context.Background())
	if err != nil {
		t.Fatalf("ListManagedMachines: %v", err)
	}

	found := make(map[string]runtime.PodMetadata)
	for _, m := range machines {
		found[m.UID+"/"+m.ContainerName] = m
	}

	for _, p := range pods {
		key := p.uid + "/" + p.container
		m, ok := found[key]
		if !ok {
			t.Errorf("machine %q not in ListManagedMachines; got: %v", key, keys(machines))
			continue
		}
		if m.Namespace != "default" {
			t.Errorf("%s: namespace: got %q want \"default\"", key, m.Namespace)
		}
		if m.PodIP != "10.88.0.99" {
			t.Errorf("%s: podIP: got %q want \"10.88.0.99\"", key, m.PodIP)
		}
		if m.NodeName != pawn {
			t.Errorf("%s: nodeName: got %q want %q", key, m.NodeName, pawn)
		}
		if m.ContainerName != p.container {
			t.Errorf("%s: containerName: got %q want %q", key, m.ContainerName, p.container)
		}
	}
}

func TestSystemd_ListManagedMachines_DoesNotLeakAcrossPawns(t *testing.T) {
	requireRoot(t)
	requireSystemd(t)
	pawnA := pawnName(t) + "a"
	pawnB := pawnName(t) + "b"

	// Spawn unit under pawnA's naming scheme directly via dbus
	loggerA := slog.New(slog.NewTextHandler(io.Discard, nil))
	rtA, err := rtsd.NewSystemdRuntime(context.Background(), pawnA, image.NewImageManager(t.TempDir(), loggerA),
		loggerA, runtime.ExecNsenter, nil)
	if err != nil {
		t.Fatalf("NewSystemdRuntime for pawnA: %v", err)
	}
	defer rtA.Close()

	loggerB := slog.New(slog.NewTextHandler(io.Discard, nil))
	rtB, err := rtsd.NewSystemdRuntime(context.Background(), pawnB, image.NewImageManager(t.TempDir(), loggerB),
		loggerB, runtime.ExecNsenter, nil)
	if err != nil {
		t.Fatalf("NewSystemdRuntime for pawnB: %v", err)
	}
	defer rtB.Close()

	spawnSleepUnit(t, rtA, pawnA, "uid-leak-a", "main", 300)
	time.Sleep(200 * time.Millisecond)

	// rtB should not see pawnA's machines
	machinesB, err := rtB.ListManagedMachines(context.Background())
	if err != nil {
		t.Fatalf("ListManagedMachines (pawnB): %v", err)
	}
	for _, m := range machinesB {
		if m.UID == "uid-leak-a" {
			t.Errorf("pawnB incorrectly sees pawnA's machine uid-leak-a")
		}
	}
}

func TestSystemd_WaitForMachineExit_Completes(t *testing.T) {
	rt := newTestRuntime(t)
	pawn := pawnName(t)
	podUID := "uid-wait-exit"
	container := "main"

	// Sleep for 0.3s so it exits naturally during the test
	spawnSleepUnit(t, rt, pawn, podUID, container, 0.3)

	state, err := rt.WaitForMachineExit(context.Background(), podUID, container, 5*time.Second)
	if err != nil {
		t.Fatalf("WaitForMachineExit: %v", err)
	}
	if state != runtime.StateExited {
		t.Errorf("expected StateExited, got %v", state)
	}
}

func TestSystemd_WaitForMachineExit_Timeout(t *testing.T) {
	rt := newTestRuntime(t)
	pawn := pawnName(t)
	spawnSleepUnit(t, rt, pawn, "uid-wait-timeout", "main", 300)
	time.Sleep(100 * time.Millisecond)

	_, err := rt.WaitForMachineExit(context.Background(), "uid-wait-timeout", "main", 600*time.Millisecond)
	if err == nil {
		t.Error("expected timeout error, got nil")
	}
}

func TestSystemd_GetLogStream(t *testing.T) {
	rt := newTestRuntime(t)
	pawn := pawnName(t)
	spawnSleepUnit(t, rt, pawn, "uid-logs", "main", 300)
	time.Sleep(200 * time.Millisecond)

	rc, err := rt.GetLogStream(context.Background(), "uid-logs", "main", api.ContainerLogOpts{Tail: 10})
	if err != nil {
		t.Fatalf("GetLogStream: %v", err)
	}
	defer rc.Close()

	// Just verify it opens and is readable without error
	buf := make([]byte, 512)
	rc.Read(buf) //nolint:errcheck
}

func TestSystemd_InitPawnSlice(t *testing.T) {
	rt := newTestRuntime(t)

	cfg := runtime.PawnSliceConfig{
		Name:    pawnName(t),
		BaseDir: t.TempDir(),
	}

	if err := rt.InitPawnSlice(context.Background(), cfg); err != nil {
		t.Fatalf("InitPawnSlice: %v", err)
	}
	// Idempotency
	if err := rt.InitPawnSlice(context.Background(), cfg); err != nil {
		t.Errorf("InitPawnSlice (second call) should be idempotent: %v", err)
	}

	t.Cleanup(func() {
		conn, _ := dbus.NewSystemConnectionContext(context.Background())
		if conn == nil {
			return
		}
		defer conn.Close()
		ch := make(chan string, 1)
		conn.StopUnitContext(context.Background(), //nolint:errcheck
			"perigeos-"+pawnName(t)+".slice", "replace", ch)
	})
}

func keys(machines []runtime.PodMetadata) []string {
	out := make([]string, 0, len(machines))
	for _, m := range machines {
		out = append(out, m.UID+"/"+m.ContainerName)
	}
	return out
}
