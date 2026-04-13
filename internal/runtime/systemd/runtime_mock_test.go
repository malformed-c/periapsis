package systemd

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/coreos/go-systemd/v22/dbus"
	dbusv5 "github.com/godbus/dbus/v5"
	"github.com/malformed-c/periapsis/internal/image"
	"github.com/malformed-c/periapsis/internal/runtime"
	corev1 "k8s.io/api/core/v1"
)

func TestRunMachine_Issue485Workaround(t *testing.T) {
	mockDBus := newMockSystemdDBus()
	mockMachine := newMockMachineDBus()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	im := image.NewImageManager(t.TempDir(), logger)

	rt := NewSystemdRuntimeWithConns("test-pawn", im, logger, runtime.ExecNsenter, mockDBus, mockMachine, nil)

	podUID := "test-uid"
	cfg := runtime.PodConfig{
		ContainerName: "test-container",
		Container: &corev1.Container{
			Name: "test-container",
		},
		RootFS:    "/tmp/rootfs",
		NetNSPath: "/proc/1/ns/net",
	}

	err := rt.RunMachine(context.Background(), podUID, cfg)
	if err != nil {
		t.Fatalf("RunMachine failed: %v", err)
	}

	// Verify that the call was made
	select {
	case name := <-mockDBus.startTransientCalled:
		expectedName := wrapperUnitName("test-pawn", podUID, "test-container")
		if name != expectedName {
			t.Errorf("expected unit name %s, got %s", expectedName, name)
		}
	default:
		t.Fatal("StartTransientUnit was not called")
	}
}

func TestWaitForMachineExit_SuccessPath(t *testing.T) {
	mockDBus := newMockSystemdDBus()
	mockMachine := newMockMachineDBus()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	im := image.NewImageManager(t.TempDir(), logger)

	rt := NewSystemdRuntimeWithConns("test-pawn", im, logger, runtime.ExecNsenter, mockDBus, mockMachine, nil)

	podUID := "test-uid"
	containerName := "test-container"
	serviceName := wrapperUnitName("test-pawn", podUID, containerName)

	// Step 1: Unit is "active"
	mockDBus.units[serviceName] = dbus.UnitStatus{Name: serviceName, ActiveState: "active"}
	mockDBus.properties[serviceName] = map[string]*dbus.Property{
		"ActiveState": {Name: "ActiveState", Value: dbusv5.MakeVariant("active")},
		"SubState":    {Name: "SubState", Value: dbusv5.MakeVariant("running")},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Run WaitForMachineExit in a goroutine
	done := make(chan struct{})
	var state runtime.MachineState
	var waitErr error
	go func() {
		state, waitErr = rt.WaitForMachineExit(ctx, podUID, containerName, 10*time.Second)
		close(done)
	}()

	// Simulate event indicating it has started
	mockDBus.notifyWaiters(serviceName, "running")

	// Step 2: Unit becomes "inactive" (exited)
	time.Sleep(100 * time.Millisecond)
	mockDBus.mu.Lock()
	mockDBus.units[serviceName] = dbus.UnitStatus{Name: serviceName, ActiveState: "inactive"}
	mockDBus.properties[serviceName] = map[string]*dbus.Property{
		"ActiveState":    {Name: "ActiveState", Value: dbusv5.MakeVariant("inactive")},
		"ExecMainStatus": {Name: "ExecMainStatus", Value: dbusv5.MakeVariant(int32(0))},
	}
	mockDBus.mu.Unlock()
	mockDBus.notifyWaiters(serviceName, "dead")

	<-done
	if waitErr != nil {
		t.Fatalf("WaitForMachineExit failed: %v", waitErr)
	}
	if state != runtime.StateExited {
		t.Errorf("expected StateExited, got %v", state)
	}
}

func TestWaitForMachineExit_Timeout(t *testing.T) {
	mockDBus := newMockSystemdDBus()
	mockMachine := newMockMachineDBus()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	im := image.NewImageManager(t.TempDir(), logger)

	rt := NewSystemdRuntimeWithConns("test-pawn", im, logger, runtime.ExecNsenter, mockDBus, mockMachine, nil)

	podUID := "test-uid"
	containerName := "test-container"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := rt.WaitForMachineExit(ctx, podUID, containerName, 100*time.Millisecond)
	if err == nil {
		t.Error("expected timeout error, got nil")
	}
}

func TestBatchWatcher_FastExit(t *testing.T) {
	// This test requires a full Gambit setup, but we can verify the core logic
	// by simulating how it interacts with the Runtime.
	mockDBus := newMockSystemdDBus()
	mockMachine := newMockMachineDBus()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	im := image.NewImageManager(t.TempDir(), logger)

	rt := NewSystemdRuntimeWithConns("test-pawn", im, logger, runtime.ExecNsenter, mockDBus, mockMachine, nil)

	podUID := "fast-exit-uid"
	containerName := "main"
	serviceName := wrapperUnitName("test-pawn", podUID, containerName)

	// Simulate unit that ran and exited with 0 before any poll.
	mockDBus.mu.Lock()
	mockDBus.units[serviceName] = dbus.UnitStatus{
		Name:        serviceName,
		ActiveState: "inactive",
	}
	mockDBus.properties[serviceName] = map[string]*dbus.Property{
		"ActiveState":    {Name: "ActiveState", Value: dbusv5.MakeVariant("inactive")},
		"ExecMainStatus": {Name: "ExecMainStatus", Value: dbusv5.MakeVariant(int32(0))},
		"Environment": {Name: "Environment", Value: dbusv5.MakeVariant([]string{
			"PERIGEOS_META_UID=" + podUID,
			"PERIGEOS_META_CONTAINER=" + containerName,
		})},
	}
	mockDBus.mu.Unlock()

	// Verify ListManagedMachines returns it with ExitCode 0
	machines, err := rt.ListManagedMachines(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(machines) != 1 {
		t.Fatalf("expected 1 machine, got %d", len(machines))
	}
	m := machines[0]
	if m.State != runtime.StateExited || m.ExitCode != 0 {
		t.Errorf("expected Exited(0), got %v(%d)", m.State, m.ExitCode)
	}
}
