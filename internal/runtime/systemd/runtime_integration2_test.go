package systemd_test

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/coreos/go-systemd/v22/dbus"
	dbusv5 "github.com/godbus/dbus/v5"
	"github.com/malformed-c/periapsis/internal/runtime"
)

// spawnExitUnit starts a transient unit that immediately exits with the given
// exit code. Unlike spawnSleepUnit it does NOT set CollectMode=inactive-or-failed,
// so the unit lingers in the dead/failed state until ResetFailedUnit is called.
// This is the shape perigeos units take in production.
func spawnExitUnit(t *testing.T, pawn, podUID, container string, exitCode int) {
	t.Helper()

	conn, err := dbus.NewSystemConnectionContext(context.Background())
	if err != nil {
		t.Fatalf("dbus connect: %v", err)
	}
	defer conn.Close()

	unitName := fmt.Sprintf("perigeos-%s-pod-%s-%s.service", pawn, podUID, container)
	script := fmt.Sprintf("exit %d", exitCode)

	props := []dbus.Property{
		dbus.PropDescription("Test exit pod " + podUID),
		dbus.PropExecStart([]string{"/bin/sh", "-c", script}, false),
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
	// The start result is "done" once systemd has activated the unit; the unit
	// itself may have already exited by the time the channel fires.
	if res := <-ch; res != "done" {
		t.Fatalf("start result for %s: %s", unitName, res)
	}
}

// waitForState polls MachineStatus until it matches want or the deadline passes.
func waitForState(t *testing.T, rt runtime.Runtime, podUID, container string, want runtime.MachineState, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		state, err := rt.MachineStatus(context.Background(), podUID, container)
		if err != nil {
			t.Fatalf("MachineStatus: %v", err)
		}
		if state == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	state, _ := rt.MachineStatus(context.Background(), podUID, container)
	t.Fatalf("timed out waiting for %v, last state: %v", want, state)
}

// --- ResetUnit ---

func TestSystemd_ResetUnit_ClearsFailedUnit(t *testing.T) {
	rt := newTestRuntime(t)
	pawn := pawnName(t)
	podUID := "uid-reset-failed"
	container := "main"

	spawnExitUnit(t, pawn, podUID, container, 1)
	waitForState(t, rt, podUID, container, runtime.StateFailed, 3*time.Second)

	if err := rt.ResetUnit(context.Background(), podUID, container); err != nil {
		t.Fatalf("ResetUnit: %v", err)
	}

	// After reset the unit should no longer appear as failed.
	time.Sleep(100 * time.Millisecond)
	state, _ := rt.MachineStatus(context.Background(), podUID, container)
	if state == runtime.StateFailed {
		t.Errorf("unit still in StateFailed after ResetUnit")
	}
}

func TestSystemd_ResetUnit_Idempotent(t *testing.T) {
	rt := newTestRuntime(t)
	pawn := pawnName(t)
	podUID := "uid-reset-idem"
	container := "main"

	spawnExitUnit(t, pawn, podUID, container, 1)
	waitForState(t, rt, podUID, container, runtime.StateFailed, 3*time.Second)

	ctx := context.Background()
	if err := rt.ResetUnit(ctx, podUID, container); err != nil {
		t.Fatalf("first ResetUnit: %v", err)
	}
	// Second call on an already-reset unit should not error.
	if err := rt.ResetUnit(ctx, podUID, container); err != nil {
		t.Errorf("second ResetUnit should be idempotent, got: %v", err)
	}
}

// --- GetContainerExitInfo ---

func TestSystemd_GetContainerExitInfo_Failure(t *testing.T) {
	rt := newTestRuntime(t)
	pawn := pawnName(t)
	podUID := "uid-exitinfo-fail"
	container := "main"

	spawnExitUnit(t, pawn, podUID, container, 42)
	waitForState(t, rt, podUID, container, runtime.StateFailed, 3*time.Second)
	t.Cleanup(func() { rt.ResetUnit(context.Background(), podUID, container) }) //nolint:errcheck

	info := rt.GetContainerExitInfo(context.Background(), podUID, container)

	if info.ExitCode != 42 {
		t.Errorf("ExitCode: got %d, want 42", info.ExitCode)
	}
	if info.Result == "" {
		t.Error("Result should be non-empty for a failed unit")
	}
	// systemd reports this as "exit-code" when the process itself exited non-zero.
	if info.Result != "exit-code" {
		t.Logf("Result=%q (expected \"exit-code\", may differ by systemd version)", info.Result)
	}
}

func TestSystemd_GetContainerExitInfo_Success(t *testing.T) {
	rt := newTestRuntime(t)
	pawn := pawnName(t)
	podUID := "uid-exitinfo-ok"
	container := "main"

	// exit 0 — unit ends in inactive/dead, not failed.
	spawnExitUnit(t, pawn, podUID, container, 0)
	waitForState(t, rt, podUID, container, runtime.StateExited, 3*time.Second)

	info := rt.GetContainerExitInfo(context.Background(), podUID, container)

	if info.ExitCode != 0 {
		t.Errorf("ExitCode: got %d, want 0", info.ExitCode)
	}
	// "success" is the standard systemd Result value for a clean exit.
	if info.Result != "" && info.Result != "success" {
		t.Errorf("Result: got %q, want \"success\" or empty", info.Result)
	}
}

// --- CleanupStaleUnits ---

func TestSystemd_CleanupStaleUnits_RemovesUnknownFailed(t *testing.T) {
	rt := newTestRuntime(t)
	pawn := pawnName(t)

	// Spawn two failing units with distinct UIDs.
	spawnExitUnit(t, pawn, "uid-stale-keep", "main", 1)
	spawnExitUnit(t, pawn, "uid-stale-remove", "main", 1)

	waitForState(t, rt, "uid-stale-keep", "main", runtime.StateFailed, 3*time.Second)
	waitForState(t, rt, "uid-stale-remove", "main", runtime.StateFailed, 3*time.Second)

	t.Cleanup(func() {
		rt.ResetUnit(context.Background(), "uid-stale-keep", "main")   //nolint:errcheck
		rt.ResetUnit(context.Background(), "uid-stale-remove", "main") //nolint:errcheck
	})

	// Tell the runtime that only "uid-stale-keep" is active; the other is stale.
	activeUIDs := map[string]bool{"uid-stale-keep": true}
	cleaned, err := rt.CleanupStaleUnits(context.Background(), activeUIDs)
	if err != nil {
		t.Fatalf("CleanupStaleUnits: %v", err)
	}
	if cleaned != 1 {
		t.Errorf("cleaned: got %d, want 1", cleaned)
	}

	// The kept unit should still be failed; the removed one should be gone.
	state, _ := rt.MachineStatus(context.Background(), "uid-stale-keep", "main")
	if state != runtime.StateFailed {
		t.Errorf("kept unit: expected StateFailed, got %v", state)
	}
	state, _ = rt.MachineStatus(context.Background(), "uid-stale-remove", "main")
	if state == runtime.StateFailed {
		t.Errorf("removed unit: still in StateFailed after CleanupStaleUnits")
	}
}

func TestSystemd_CleanupStaleUnits_SkipsRunningUnits(t *testing.T) {
	rt := newTestRuntime(t)
	pawn := pawnName(t)
	podUID := "uid-cleanup-skip-running"
	container := "main"

	spawnSleepUnit(t, rt, pawn, podUID, container, 300)
	time.Sleep(150 * time.Millisecond)

	// Even with an empty active set, a running unit must not be reset.
	cleaned, err := rt.CleanupStaleUnits(context.Background(), map[string]bool{})
	if err != nil {
		t.Fatalf("CleanupStaleUnits: %v", err)
	}

	// No running unit should have been cleaned.
	state, _ := rt.MachineStatus(context.Background(), podUID, container)
	if state != runtime.StateRunning {
		t.Errorf("running unit was affected by CleanupStaleUnits (state=%v, cleaned=%d)", state, cleaned)
	}
}

// --- SliceActive ---

func TestSystemd_SliceActive_Lifecycle(t *testing.T) {
	requireRoot(t)
	requireSystemd(t)

	rt := newTestRuntime(t)

	// Before InitPawnSlice the slice should not exist.
	if rt.SliceActive(context.Background()) {
		t.Log("slice already active before InitPawnSlice (leftover from previous run — acceptable)")
	}

	cfg := runtime.PawnSliceConfig{
		Name:    pawnName(t),
		BaseDir: t.TempDir(),
	}
	if err := rt.InitPawnSlice(context.Background(), cfg); err != nil {
		t.Fatalf("InitPawnSlice: %v", err)
	}

	if !rt.SliceActive(context.Background()) {
		t.Error("SliceActive should return true after InitPawnSlice")
	}
}

// --- SubscribeEvents ---

func TestSystemd_SubscribeEvents_ReceivesUnitStop(t *testing.T) {
	rt := newTestRuntime(t)
	pawn := pawnName(t)
	podUID := "uid-events-stop"
	container := "main"

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	events := rt.SubscribeEvents(ctx)
	if events == nil {
		t.Skip("SubscribeEvents returned nil (no signal connection available)")
	}

	spawnSleepUnit(t, rt, pawn, podUID, container, 300)
	time.Sleep(150 * time.Millisecond)

	expectedUnit := fmt.Sprintf("perigeos-%s-pod-%s-%s.service", pawn, podUID, container)

	if err := rt.StopMachine(context.Background(), podUID, container); err != nil {
		t.Fatalf("StopMachine: %v", err)
	}

	// Collect events until we see the expected unit or the context expires.
	for {
		select {
		case ev, ok := <-events:
			if !ok {
				t.Fatal("events channel closed before receiving expected event")
			}
			if ev.UnitName == expectedUnit {
				// Received at least one event for our unit. Done.
				return
			}
		case <-ctx.Done():
			t.Fatalf("timed out waiting for event for unit %q", expectedUnit)
		}
	}
}

func TestSystemd_SubscribeEvents_ReceivesUnitExit(t *testing.T) {
	rt := newTestRuntime(t)
	pawn := pawnName(t)
	podUID := "uid-events-exit"
	container := "main"

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	events := rt.SubscribeEvents(ctx)
	if events == nil {
		t.Skip("SubscribeEvents returned nil (no signal connection available)")
	}

	// A unit that exits on its own (not via StopMachine) should also produce events.
	spawnExitUnit(t, pawn, podUID, container, 0)
	t.Cleanup(func() { rt.ResetUnit(context.Background(), podUID, container) }) //nolint:errcheck

	expectedUnit := fmt.Sprintf("perigeos-%s-pod-%s-%s.service", pawn, podUID, container)

	for {
		select {
		case ev, ok := <-events:
			if !ok {
				t.Fatal("events channel closed before receiving expected event")
			}
			if ev.UnitName == expectedUnit {
				return
			}
		case <-ctx.Done():
			t.Fatalf("timed out waiting for event for unit %q", expectedUnit)
		}
	}
}

// --- CheckMachined ---

func TestSystemd_CheckMachined(t *testing.T) {
	rt := newTestRuntime(t)

	if err := rt.CheckMachined(context.Background()); err != nil {
		t.Errorf("CheckMachined: %v", err)
	}
}

// --- MachineStatus edge cases ---

func TestSystemd_MachineStatus_AfterNaturalExit(t *testing.T) {
	rt := newTestRuntime(t)
	pawn := pawnName(t)
	podUID := "uid-status-exit"
	container := "main"

	// exit 0: the unit should end up in StateExited, not StateFailed.
	spawnExitUnit(t, pawn, podUID, container, 0)
	waitForState(t, rt, podUID, container, runtime.StateExited, 3*time.Second)
}

func TestSystemd_MachineStatus_AfterNonZeroExit(t *testing.T) {
	rt := newTestRuntime(t)
	pawn := pawnName(t)
	podUID := "uid-status-fail"
	container := "main"

	spawnExitUnit(t, pawn, podUID, container, 1)
	waitForState(t, rt, podUID, container, runtime.StateFailed, 3*time.Second)
	t.Cleanup(func() { rt.ResetUnit(context.Background(), podUID, container) }) //nolint:errcheck
}

// --- ListManagedMachines with mixed states ---

func TestSystemd_ListManagedMachines_IncludesFailedUnits(t *testing.T) {
	rt := newTestRuntime(t)
	pawn := pawnName(t)

	spawnSleepUnit(t, rt, pawn, "uid-list-running", "app", 300)
	spawnExitUnit(t, pawn, "uid-list-failed", "app", 1)

	waitForState(t, rt, "uid-list-failed", "app", runtime.StateFailed, 3*time.Second)
	t.Cleanup(func() { rt.ResetUnit(context.Background(), "uid-list-failed", "app") }) //nolint:errcheck

	time.Sleep(100 * time.Millisecond)

	machines, err := rt.ListManagedMachines(context.Background())
	if err != nil {
		t.Fatalf("ListManagedMachines: %v", err)
	}

	uids := make([]string, 0, len(machines))
	for _, m := range machines {
		uids = append(uids, m.UID)
	}

	if !slices.Contains(uids, "uid-list-running") {
		t.Errorf("running unit not in ListManagedMachines; got UIDs: %v", uids)
	}
	if !slices.Contains(uids, "uid-list-failed") {
		t.Errorf("failed unit not in ListManagedMachines; got UIDs: %v", uids)
	}

	// Verify the State field is populated correctly per machine.
	for _, m := range machines {
		switch m.UID {
		case "uid-list-running":
			if m.State != runtime.StateRunning {
				t.Errorf("uid-list-running: State=%v, want StateRunning", m.State)
			}
		case "uid-list-failed":
			if m.State != runtime.StateFailed {
				t.Errorf("uid-list-failed: State=%v, want StateFailed", m.State)
			}
			if m.ExitCode != 1 {
				t.Errorf("uid-list-failed: ExitCode=%d, want 1", m.ExitCode)
			}
		}
	}
}
