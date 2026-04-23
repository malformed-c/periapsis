package node

import (
        "context"
        "io"
        "log/slog"
        "os"
        "testing"
        "time"

        perigeos "github.com/malformed-c/periapsis/internal/runtime"
        "github.com/malformed-c/periapsis/internal/types"
        "github.com/malformed-c/periapsis/node/api"
        corev1 "k8s.io/api/core/v1"
        metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
        k8stypes "k8s.io/apimachinery/pkg/types"
)

// --- helpers ---

func bwLogger() *slog.Logger {
        return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func newRunningPod(uid, name string, containers ...string) *corev1.Pod {
        specs := make([]corev1.Container, len(containers))
        for i, c := range containers {
                specs[i] = corev1.Container{Name: c, Image: "nginx:alpine"}
        }
        return &corev1.Pod{
                ObjectMeta: metav1.ObjectMeta{
                        UID:       k8stypes.UID(uid),
                        Namespace: "default",
                        Name:      name,
                },
                Spec: corev1.PodSpec{
                        Containers:    specs,
                        RestartPolicy: corev1.RestartPolicyAlways,
                },
        }
}

// factCollector collects facts sent through SendFact.
type factCollector struct {
        facts []types.Fact
}

func (fc *factCollector) Send(fact types.Fact) bool {
        fc.facts = append(fc.facts, fact)
        return true
}

// stubRuntime implements perigeos.Runtime for BatchWatcher tests.
// Only SubscribeEvents is functional; all other methods are stubs.
type stubRuntime struct {
        events chan perigeos.UnitEvent
}

func newStubRuntime() *stubRuntime {
        return &stubRuntime{
                events: make(chan perigeos.UnitEvent, 16),
        }
}

func (s *stubRuntime) send(ev perigeos.UnitEvent) { s.events <- ev }

func (s *stubRuntime) RunMachine(_ context.Context, _ string, _ perigeos.PodConfig) error {
        return nil
}
func (s *stubRuntime) StopMachine(_ context.Context, _, _ string) error {
        return nil
}
func (s *stubRuntime) MachineStatus(_ context.Context, _, _ string) (perigeos.MachineState, error) {
        return perigeos.StateUnknown, nil
}
func (s *stubRuntime) MachineExitCode(_ context.Context, _, _ string) int32 {
        return 0
}
func (s *stubRuntime) WaitForMachineExit(_ context.Context, _, _ string, _ time.Duration) (perigeos.MachineState, error) {
        return perigeos.StateUnknown, nil
}
func (s *stubRuntime) ListManagedMachines(_ context.Context) ([]perigeos.PodMetadata, error) {
        return nil, nil
}
func (s *stubRuntime) GetLogStream(_ context.Context, _, _ string, _ api.ContainerLogOpts) (io.ReadCloser, error) {
        return io.NopCloser(nil), nil
}
func (s *stubRuntime) RunInContainer(_ context.Context, _, _ string, _ []string, _ api.AttachIO) error {
        return nil
}
func (s *stubRuntime) AttachContainer(_ context.Context, _, _ string, _ api.AttachIO) error {
        return nil
}
func (s *stubRuntime) InitPawnSlice(_ context.Context, _ perigeos.PawnSliceConfig) error {
        return nil
}
func (s *stubRuntime) CheckMachined(_ context.Context) error {
        return nil
}
func (s *stubRuntime) SubscribeEvents(_ context.Context) <-chan perigeos.UnitEvent {
        return s.events
}
func (s *stubRuntime) MakeSharedMounts(_ context.Context, _, _ string, _ []perigeos.BindMount) error {
        return nil
}
func (s *stubRuntime) ResetUnit(_ context.Context, _, _ string) error {
        return nil
}
func (s *stubRuntime) CleanupStaleUnits(_ context.Context, _ map[string]bool) (int, error) {
        return 0, nil
}
func (s *stubRuntime) SliceActive(_ context.Context) bool {
        return true
}
func (s *stubRuntime) PortForward(_ context.Context, _, _ string, _ int32, _ io.ReadWriteCloser) error {
        return nil
}

// --- Tests ---

// TestBatchWatcher_EventBasedEmitsUnitFact verifies that the event-based
// BatchWatcher emits UnitFacts when D-Bus signals arrive. This replaces
// the old poll-based tests.
func TestBatchWatcher_EventBasedEmitsUnitFact(t *testing.T) {
        store := setupTestStore()
        t.Cleanup(store.Close)

        rt := newStubRuntime()
        fc := &factCollector{}

        bw := StartBatchWatcher(BatchWatcherDeps{
                Runtime:  rt,
                Logger:   bwLogger(),
                PawnName: "test-pawn",
                Store:    store,
                ParseUnitName: func(unitName string) (string, string) {
                        return ParseUnitName("test-pawn", unitName)
                },
                SendFact: fc.Send,
        })
        defer bw.Stop()

        // Send a D-Bus event for a running container.
        rt.send(perigeos.UnitEvent{
                UnitName: "perigeos-test-pawn-pod-abc12345-6789-0123-4567-89abcdef0123-nginx.service",
                SubState: "running",
        })

        // Wait briefly for the event to be processed.
        waitFor(t, func() bool { return len(fc.facts) > 0 }, 2*time.Second, "UnitFact to be emitted")

        if len(fc.facts) == 0 {
                t.Fatal("expected at least one UnitFact to be emitted")
        }

        uf, ok := fc.facts[0].(*types.UnitFact)
        if !ok {
                t.Fatalf("expected UnitFact, got %T", fc.facts[0])
        }
        if uf.SubState != "running" {
                t.Errorf("expected SubState=running, got %q", uf.SubState)
        }
}

// TestBatchWatcher_IgnoresOtherPawnEvents verifies that the BatchWatcher
// filters out events for other pawns.
func TestBatchWatcher_IgnoresOtherPawnEvents(t *testing.T) {
        store := setupTestStore()
        t.Cleanup(store.Close)

        rt := newStubRuntime()
        fc := &factCollector{}

        bw := StartBatchWatcher(BatchWatcherDeps{
                Runtime:  rt,
                Logger:   bwLogger(),
                PawnName: "test-pawn",
                Store:    store,
                ParseUnitName: func(unitName string) (string, string) {
                        return ParseUnitName("test-pawn", unitName)
                },
                SendFact: fc.Send,
        })
        defer bw.Stop()

        // Send an event for a different pawn.
        rt.send(perigeos.UnitEvent{
                UnitName: "perigeos-other-pawn-pod-abc12345-6789-0123-4567-89abcdef0123-nginx.service",
                SubState: "running",
        })

        // Wait briefly.
        time.Sleep(100 * time.Millisecond)

        if len(fc.facts) > 0 {
                t.Errorf("expected no UnitFacts for other pawn, got %d", len(fc.facts))
        }
}

// TestBatchWatcher_NoPanicWithoutSendFact verifies that the BatchWatcher
// handles a nil SendFact gracefully.
func TestBatchWatcher_NoPanicWithoutSendFact(t *testing.T) {
        store := setupTestStore()
        t.Cleanup(store.Close)

        rt := newStubRuntime()

        bw := StartBatchWatcher(BatchWatcherDeps{
                Runtime:  rt,
                Logger:   bwLogger(),
                PawnName: "test-pawn",
                Store:    store,
                ParseUnitName: func(unitName string) (string, string) {
                        return ParseUnitName("test-pawn", unitName)
                },
                SendFact: nil, // nil SendFact - should not panic
        })
        defer bw.Stop()

        // Send an event - should be silently dropped.
        rt.send(perigeos.UnitEvent{
                UnitName: "perigeos-test-pawn-pod-abc12345-6789-0123-4567-89abcdef0123-nginx.service",
                SubState: "running",
        })

        // Wait briefly - no panic should occur.
        time.Sleep(100 * time.Millisecond)
}

// waitFor polls the condition until it returns true or the timeout expires.
func waitFor(t *testing.T, cond func() bool, timeout time.Duration, msg string) {
        t.Helper()
        deadline := time.Now().Add(timeout)
        for !cond() {
                if time.Now().After(deadline) {
                        t.Fatalf("timeout waiting for %s", msg)
                }
                time.Sleep(10 * time.Millisecond)
        }
}
