package systemd_test

// Additional integration tests focused on journald log streaming,
// follow-mode cancellation, and unit metadata correctness.
//
// Run with:
//   sudo -E go test ./internal/runtime/systemd/... -v -count=1

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
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

// --- Helpers ---

// spawnEchoUnit starts a transient service that writes a known message to its
// journal and then sleeps, so GetLogStream has real content to read.
// SyslogIdentifier is set so the journal SYSLOG_IDENTIFIER match works.
func spawnEchoUnit(t *testing.T, rt *rtsd.SystemdRuntime, pawn, podUID, container, msg string) {
	t.Helper()

	conn, err := dbus.NewSystemConnectionContext(context.Background())
	if err != nil {
		t.Fatalf("dbus connect: %v", err)
	}
	defer conn.Close()

	unitName := fmt.Sprintf("perigeos-%s-pod-%s-%s.service", pawn, podUID, container)
	cmd := fmt.Sprintf("echo '%s' && sleep 300", msg)

	props := []dbus.Property{
		dbus.PropDescription("Echo test pod " + podUID),
		dbus.PropExecStart([]string{"/bin/sh", "-c", cmd}, false),
		{Name: "SyslogIdentifier", Value: dbusv5.MakeVariant(container)},
		{Name: "CollectMode", Value: dbusv5.MakeVariant("inactive-or-failed")},
		{Name: "Environment", Value: dbusv5.MakeVariant([]string{
			"PERIGEOS_META_UID=" + podUID,
			"PERIGEOS_META_NAME=test-echo-pod",
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

// readLines reads up to n lines from rc with a per-line timeout.
func readLines(t *testing.T, rc io.ReadCloser, n int, timeout time.Duration) []string {
	t.Helper()
	lines := make(chan string, n)
	go func() {
		scanner := bufio.NewScanner(rc)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
		close(lines)
	}()

	var result []string
	deadline := time.After(timeout)
	for len(result) < n {
		select {
		case line, ok := <-lines:
			if !ok {
				return result
			}
			result = append(result, line)
		case <-deadline:
			return result
		}
	}
	return result
}

// --- Journal content ---

func TestSystemd_GetLogStream_HasContent(t *testing.T) {
	rt := newTestRuntime(t)
	pawn := pawnName(t)
	podUID := "uid-log-content"
	container := "main"
	marker := "hello-perigeos-journal-test"

	spawnEchoUnit(t, rt, pawn, podUID, container, marker)
	// Give journald time to ingest the message.
	time.Sleep(500 * time.Millisecond)

	rc, err := rt.GetLogStream(context.Background(), podUID, container, api.ContainerLogOpts{})
	if err != nil {
		t.Fatalf("GetLogStream: %v", err)
	}
	defer rc.Close()

	lines := readLines(t, rc, 20, 3*time.Second)

	found := false
	for _, l := range lines {
		if strings.Contains(l, marker) {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("marker %q not found in log output; got lines: %v", marker, lines)
	}
}

func TestSystemd_GetLogStream_TailLimitsEntries(t *testing.T) {
	rt := newTestRuntime(t)
	pawn := pawnName(t)
	podUID := "uid-log-tail"
	container := "main"

	// Write several distinct lines then sleep.
	msgs := []string{"line-alpha", "line-beta", "line-gamma", "line-delta", "line-epsilon"}
	cmd := strings.Join(msgs, "\necho ") // echo each line
	fullCmd := "echo " + cmd + " && sleep 300"

	conn, err := dbus.NewSystemConnectionContext(context.Background())
	if err != nil {
		t.Fatalf("dbus connect: %v", err)
	}
	defer conn.Close()

	unitName := fmt.Sprintf("perigeos-%s-pod-%s-%s.service", pawn, podUID, container)
	props := []dbus.Property{
		dbus.PropExecStart([]string{"/bin/sh", "-c", fullCmd}, false),
		{Name: "SyslogIdentifier", Value: dbusv5.MakeVariant(container)},
		{Name: "CollectMode", Value: dbusv5.MakeVariant("inactive-or-failed")},
		{Name: "Environment", Value: dbusv5.MakeVariant([]string{
			"PERIGEOS_META_UID=" + podUID,
			"PERIGEOS_META_NAME=test-tail",
			"PERIGEOS_META_NAMESPACE=default",
			"PERIGEOS_META_NODENAME=" + pawn,
			"PERIGEOS_META_IP=10.88.0.99",
			"PERIGEOS_META_CONTAINER=" + container,
		})},
	}
	ch := make(chan string, 1)
	if _, err := conn.StartTransientUnitContext(context.Background(), unitName, "replace", props, ch); err != nil {
		t.Fatalf("StartTransientUnit: %v", err)
	}
	<-ch
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		rt.StopMachine(ctx, podUID, container) //nolint:errcheck
	})

	time.Sleep(600 * time.Millisecond)

	rc, err := rt.GetLogStream(context.Background(), podUID, container, api.ContainerLogOpts{Tail: 2})
	if err != nil {
		t.Fatalf("GetLogStream(Tail=2): %v", err)
	}
	defer rc.Close()

	lines := readLines(t, rc, 10, 2*time.Second)
	if len(lines) > 2 {
		t.Errorf("Tail=2 should return at most 2 lines, got %d: %v", len(lines), lines)
	}
}

func TestSystemd_GetLogStream_SinceSeconds(t *testing.T) {
	rt := newTestRuntime(t)
	pawn := pawnName(t)
	podUID := "uid-log-since"
	container := "main"
	marker := "since-seconds-marker"

	spawnEchoUnit(t, rt, pawn, podUID, container, marker)
	time.Sleep(500 * time.Millisecond)

	// SinceSeconds=10 should include the message written <1s ago.
	rc, err := rt.GetLogStream(context.Background(), podUID, container, api.ContainerLogOpts{SinceSeconds: 10})
	if err != nil {
		t.Fatalf("GetLogStream(SinceSeconds=10): %v", err)
	}
	defer rc.Close()

	lines := readLines(t, rc, 20, 3*time.Second)

	found := false
	for _, l := range lines {
		if strings.Contains(l, marker) {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("marker %q not found with SinceSeconds=10; lines: %v", marker, lines)
	}
}

func TestSystemd_GetLogStream_SinceSeconds_ExcludesOld(t *testing.T) {
	rt := newTestRuntime(t)
	pawn := pawnName(t)
	podUID := "uid-log-since-old"
	container := "main"
	marker := "old-entry-should-be-excluded"

	spawnEchoUnit(t, rt, pawn, podUID, container, marker)
	time.Sleep(500 * time.Millisecond)

	// SinceSeconds=1 with a cutoff well before our message.
	// Sleep to push the message >2s into the past then use SinceSeconds=1.
	time.Sleep(2 * time.Second)

	rc, err := rt.GetLogStream(context.Background(), podUID, container, api.ContainerLogOpts{SinceSeconds: 1})
	if err != nil {
		t.Fatalf("GetLogStream: %v", err)
	}
	defer rc.Close()

	lines := readLines(t, rc, 20, 2*time.Second)

	for _, l := range lines {
		if strings.Contains(l, marker) {
			t.Errorf("old entry should be excluded by SinceSeconds=1, but appeared in: %v", lines)
			break
		}
	}
}

// --- Follow mode ---

func TestSystemd_GetLogStream_Follow_CancelContext(t *testing.T) {
	rt := newTestRuntime(t)
	pawn := pawnName(t)
	podUID := "uid-log-follow-cancel"
	container := "main"

	spawnEchoUnit(t, rt, pawn, podUID, container, "follow-test-marker")
	time.Sleep(300 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())

	rc, err := rt.GetLogStream(ctx, podUID, container, api.ContainerLogOpts{Follow: true})
	if err != nil {
		t.Fatalf("GetLogStream(Follow): %v", err)
	}
	defer rc.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 512)
		for {
			_, err := rc.Read(buf)
			if err != nil {
				return
			}
		}
	}()

	// Cancel context after a short delay - the reader goroutine must unblock.
	time.Sleep(300 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// good
	case <-time.After(3 * time.Second):
		t.Error("follow mode Read did not unblock after context cancellation")
	}
}

// --- StartedAt timestamp ---

func TestSystemd_ListManagedMachines_StartedAt_Populated(t *testing.T) {
	rt := newTestRuntime(t)
	pawn := pawnName(t)

	spawnSleepUnit(t, rt, pawn, "uid-startedat", "main", 300)
	time.Sleep(300 * time.Millisecond)

	machines, err := rt.ListManagedMachines(context.Background())
	if err != nil {
		t.Fatalf("ListManagedMachines: %v", err)
	}

	var found *runtime.PodMetadata
	for i := range machines {
		if machines[i].UID == "uid-startedat" {
			found = &machines[i]
			break
		}
	}
	if found == nil {
		t.Fatal("machine uid-startedat not found")
	}
	if found.StartedAt.IsZero() {
		t.Error("StartedAt is zero - ActiveEnterTimestamp not being read correctly")
	}
	// Sanity: started within the last minute
	if time.Since(found.StartedAt) > time.Minute {
		t.Errorf("StartedAt %v looks too old (>1m ago)", found.StartedAt)
	}
}

// --- MachineStatus - failed state ---

func TestSystemd_MachineStatus_Failed(t *testing.T) {
	requireRoot(t)
	requireSystemd(t)

	conn, err := dbus.NewSystemConnectionContext(context.Background())
	if err != nil {
		t.Fatalf("dbus: %v", err)
	}
	defer conn.Close()

	pawn := pawnName(t)
	podUID := "uid-failed-state"
	container := "main"

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	rt, err := rtsd.NewSystemdRuntime(context.Background(), pawn,
		image.NewImageManager(t.TempDir(), logger), logger, runtime.ExecNsenter, nil)
	if err != nil {
		t.Fatalf("NewSystemdRuntime: %v", err)
	}
	defer rt.Close()

	unitName := fmt.Sprintf("perigeos-%s-pod-%s-%s.service", pawn, podUID, container)

	// Start a unit that exits immediately with failure.
	props := []dbus.Property{
		dbus.PropExecStart([]string{"/bin/sh", "-c", "exit 1"}, false),
		{Name: "CollectMode", Value: dbusv5.MakeVariant("inactive")}, // keep failed state visible
		{Name: "Environment", Value: dbusv5.MakeVariant([]string{
			"PERIGEOS_META_UID=" + podUID,
			"PERIGEOS_META_CONTAINER=" + container,
		})},
	}
	ch := make(chan string, 1)
	if _, err := conn.StartTransientUnitContext(context.Background(), unitName, "replace", props, ch); err != nil {
		t.Fatalf("StartTransientUnit: %v", err)
	}
	<-ch

	t.Cleanup(func() {
		conn2, _ := dbus.NewSystemConnectionContext(context.Background())
		if conn2 != nil {
			defer conn2.Close()
			conn2.ResetFailedUnitContext(context.Background(), unitName) //nolint:errcheck
		}
	})

	// Wait up to 2s for the unit to settle into failed state.
	var state runtime.MachineState
	for i := 0; i < 20; i++ {
		time.Sleep(100 * time.Millisecond)
		state, err = rt.MachineStatus(context.Background(), podUID, container)
		if err != nil {
			t.Fatalf("MachineStatus: %v", err)
		}
		if state == runtime.StateFailed {
			break
		}
	}
	if state != runtime.StateFailed {
		t.Errorf("expected StateFailed for exit-1 unit, got %v", state)
	}
}

// --- GetLogStream - nonexistent unit opens cleanly ---

func TestSystemd_GetLogStream_NonexistentUnit_OpensCleanly(t *testing.T) {
	rt := newTestRuntime(t)

	// A unit that never ran - GetLogStream should open successfully and
	// return EOF immediately (no entries match).
	rc, err := rt.GetLogStream(context.Background(), "no-such-uid-xyz", "main", api.ContainerLogOpts{})
	if err != nil {
		t.Fatalf("GetLogStream for nonexistent unit should not error, got: %v", err)
	}
	defer rc.Close()

	buf := make([]byte, 128)
	n, err := rc.Read(buf)
	if n != 0 || err != io.EOF {
		t.Errorf("expected (0, EOF) for empty journal match, got (%d, %v)", n, err)
	}
}
