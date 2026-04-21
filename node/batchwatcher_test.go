package node

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	perigeos "github.com/malformed-c/periapsis/internal/runtime"
	"github.com/malformed-c/periapsis/node/api"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
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
			UID:       types.UID(uid),
			Namespace: "default",
			Name:      name,
		},
		Spec: corev1.PodSpec{
			Containers:    specs,
			RestartPolicy: corev1.RestartPolicyAlways,
		},
	}
}

// stubRuntime implements perigeos.Runtime. All methods are no-ops except
// ListManagedMachines, which returns whatever machines is set to.
type stubRuntime struct {
	mu       sync.Mutex
	machines []perigeos.PodMetadata
	eventsCh chan perigeos.UnitEvent
}

func newStubRuntime() *stubRuntime {
	return &stubRuntime{
		eventsCh: make(chan perigeos.UnitEvent, 64),
	}
}

func (r *stubRuntime) setMachines(m []perigeos.PodMetadata) {
	r.mu.Lock()
	r.machines = m
	r.mu.Unlock()
}

func (r *stubRuntime) ListManagedMachines(_ context.Context) ([]perigeos.PodMetadata, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]perigeos.PodMetadata, len(r.machines))
	copy(out, r.machines)
	return out, nil
}

func (r *stubRuntime) SubscribeEvents(_ context.Context) <-chan perigeos.UnitEvent {
	return r.eventsCh
}

func (r *stubRuntime) send(ev perigeos.UnitEvent) { r.eventsCh <- ev }

// satisfy the full Runtime interface with no-ops
func (r *stubRuntime) RunMachine(_ context.Context, _ string, _ perigeos.PodConfig) error {
	return nil
}
func (r *stubRuntime) StopMachine(_ context.Context, _, _ string) error { return nil }
func (r *stubRuntime) ResetUnit(_ context.Context, _, _ string) error   { return nil }
func (r *stubRuntime) MachineStatus(_ context.Context, _, _ string) (perigeos.MachineState, error) {
	return perigeos.StateUnknown, nil
}
func (r *stubRuntime) WaitForMachineExit(_ context.Context, _, _ string, _ time.Duration) (perigeos.MachineState, error) {
	return perigeos.StateExited, nil
}
func (r *stubRuntime) GetLogStream(_ context.Context, _, _ string, _ api.ContainerLogOpts) (io.ReadCloser, error) {
	return nil, nil
}
func (r *stubRuntime) RunInContainer(_ context.Context, _, _ string, _ []string, _ api.AttachIO) error {
	return nil
}
func (r *stubRuntime) AttachContainer(_ context.Context, _, _ string, _ api.AttachIO) error {
	return nil
}
func (r *stubRuntime) InitPawnSlice(_ context.Context, _ perigeos.PawnSliceConfig) error {
	return nil
}
func (r *stubRuntime) CheckMachined(_ context.Context) error { return nil }
func (r *stubRuntime) MakeSharedMounts(_ context.Context, _, _ string, _ []perigeos.BindMount) error {
	return nil
}
func (r *stubRuntime) CleanupStaleUnits(_ context.Context, _ map[string]bool) (int, error) {
	return 0, nil
}
func (r *stubRuntime) SliceActive(_ context.Context) bool { return true }

func (r *stubRuntime) PortForward(ctx context.Context, podUID, containerName string, port int32, stream io.ReadWriteCloser) error {
	return nil
}

// startBW creates a BatchWatcher with the given store and runtime. Returned
// notified channel receives the latest pushed pod on every NotifyStatus call.
// Caller must call bw.Stop() to clean up.
func startBW(t *testing.T, store *PodStore, rt *stubRuntime) (*BatchWatcher, chan *corev1.Pod) {
	t.Helper()
	notified := make(chan *corev1.Pod, 32)
	bw := StartBatchWatcher(BatchWatcherDeps{
		Store:   store,
		Runtime: rt,
		Logger:  bwLogger(),
		NotifyStatus: func(p *corev1.Pod) {
			notified <- p
		},
		BuildPodStatus: func(pod *corev1.Pod, stateLookup func(string, string) perigeos.MachineState) *corev1.PodStatus {
			g := &Gambit{Logger: bwLogger(), store: store, node: &PawnNode{startTime: time.Now()}}
			return g.buildPodStatus(pod, stateLookup)
		},
		RestartContainer: func(_ context.Context, _ string, _ *corev1.Pod, _ string, _ int32, _ time.Duration) {},
		ParseUnitName: func(unitName string) (string, string) {
			return ParseUnitName("test-pawn", unitName)
		},
		PawnName: "test-pawn",
	})
	return bw, notified
}

// waitNotify drains until a pod matching pred arrives, or t.Fatal on timeout.
func waitNotify(t *testing.T, ch chan *corev1.Pod, pred func(*corev1.Pod) bool, msg string) *corev1.Pod {
	t.Helper()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	for {
		t.Log("wait loop")
		select {
		case p := <-ch:
			if pred(p) {
				return p
			}
		case <-deadline.C:
			t.Fatalf("timeout waiting for status update: %s", msg)
		}
	}
}

// --- Tests ---

// TestBatchWatcher_PokeTriggersStatusPush verifies that Poke() causes an
// immediate poll and pushes a status update for a Running pod without
// injecting any status object directly. This is the core property of the
// fix replacing pushContainerCreatingStatus.
func TestBatchWatcher_PokeTriggersStatusPush(t *testing.T) {
	store := setupTestStore()
	t.Cleanup(store.Close)

	pod := newRunningPod("uid-poke", "poke-pod", "nginx")
	uid := string(pod.UID)
	store.RegisterPending(uid, pod, &creationHandle{cancel: func() {}, done: make(chan struct{})})
	store.PromoteRunning(uid, pod, "10.0.0.1")
	store.InitRestartState(pod)

	rt := newStubRuntime()
	rt.setMachines([]perigeos.PodMetadata{
		{UID: uid, ContainerName: "nginx", State: perigeos.StateRunning},
	})

	bw, notified := startBW(t, store, rt)
	defer bw.Stop()

	bw.Poke()

	got := waitNotify(t, notified, func(p *corev1.Pod) bool {
		return string(p.UID) == uid
	}, "status push after Poke()")

	if got.Status.Phase != corev1.PodRunning {
		t.Errorf("expected Running, got %q", got.Status.Phase)
	}
}

// TestBatchWatcher_PokeDoesNotOverwriteReadyTrue verifies the race that the
// fix was designed to address: if the BatchWatcher has already pushed
// Running/ready=true, a subsequent Poke() must not overwrite that with
// ready=false. The status pushed after Poke() should reflect the real
// container state (Running/ready) because it reads from the probe state,
// not from a pre-baked stale object.
func TestBatchWatcher_PokeDoesNotOverwriteReadyTrue(t *testing.T) {
	store := setupTestStore()
	t.Cleanup(store.Close)

	pod := newRunningPod("uid-ready", "ready-pod", "nginx")
	uid := string(pod.UID)
	store.RegisterPending(uid, pod, &creationHandle{cancel: func() {}, done: make(chan struct{})})
	store.PromoteRunning(uid, pod, "10.0.0.2")
	store.InitRestartState(pod)

	// Mark the container ready via probe state (simulates a passed readiness probe).
	store.UpdateProbeState(uid, "nginx", func(ps *ContainerProbeState) {
		ps.Ready = true
	})

	rt := newStubRuntime()
	rt.setMachines([]perigeos.PodMetadata{
		{UID: uid, ContainerName: "nginx", State: perigeos.StateRunning},
	})

	bw, notified := startBW(t, store, rt)
	defer bw.Stop()

	// First Poke: should push Running/ready=true.
	bw.Poke()
	first := waitNotify(t, notified, func(p *corev1.Pod) bool {
		return string(p.UID) == uid && len(p.Status.ContainerStatuses) > 0 && p.Status.ContainerStatuses[0].Ready
	}, "first push: ready=true")

	if !first.Status.ContainerStatuses[0].Ready {
		t.Fatal("first push: expected ready=true")
	}

	// Second Poke: simulates the creation goroutine calling Poke() after
	// launchContainer completes. Must not overwrite the ready=true status.
	bw.Poke()

	// Give the watcher time to process. Any push here should still be ready=true.
	time.Sleep(100 * time.Millisecond)

	// Drain any pending pushes - none should have ready=false.
	done := false
	for !done {
		select {
		case p := <-notified:
			if string(p.UID) == uid && len(p.Status.ContainerStatuses) > 0 {
				if !p.Status.ContainerStatuses[0].Ready {
					t.Errorf("second Poke() overwrote ready=true with ready=false")
				}
			}
		default:
			done = true
		}
	}
}

// TestBatchWatcher_MarkRunningUnblocksTerminalDecision verifies that after
// MarkRunning is called for a container that exits before the first poll,
// checkPod correctly classifies it as Exited rather than deferring forever.
// This is the fast-exit container bug.
// TODO: Doesn't exit.
func TestBatchWatcher_MarkRunningUnblocksTerminalDecision(t *testing.T) {
	fmt.Println("Creating store")
	store := setupTestStore()
	t.Cleanup(store.Close)

	fmt.Println("Store success")

	pod := newRunningPod("uid-fast", "fast-exit", "job")
	pod.Spec.RestartPolicy = corev1.RestartPolicyNever
	uid := string(pod.UID)
	store.RegisterPending(uid, pod, &creationHandle{cancel: func() {}, done: make(chan struct{})})
	store.PromoteRunning(uid, pod, "10.0.0.3")
	store.InitRestartState(pod)

	t.Log("Restart success")

	// Container exited successfully before BatchWatcher ever saw it Running.
	rt := newStubRuntime()
	rt.setMachines([]perigeos.PodMetadata{
		{UID: uid, ContainerName: "job", State: perigeos.StateExited, ExitCode: 0},
	})

	bw, notified := startBW(t, store, rt)
	defer bw.Stop()

	// Tell the watcher that the container has officially started.
	// This mimics the behavior of the pod creation logic.
	bw.MarkRunning(uid, "job")

	// Without MarkRunning: poll would defer terminal decision forever because
	// seenRunning["uid-fast/job"] is false. The fast-exit path only fires
	// when exists=true AND exitCode=0, which it is here, so it should work.
	// This test verifies that path actually produces a terminal push.
	bw.Poke()

	waitNotify(t, notified, func(p *corev1.Pod) bool {
		return string(p.UID) == uid &&
			(p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed)
	}, "terminal phase push for fast-exit container (RestartPolicy=Never)")
}

// TestBatchWatcher_MarkRunningRaceDetector exercises MarkRunning and poll()
// concurrently to confirm there is no data race on seenRunning. Run with
// -race to catch any regression on the dual-mutex fix.
func TestBatchWatcher_MarkRunningRaceDetector(t *testing.T) {
	store := setupTestStore()
	t.Cleanup(store.Close)

	pod := newRunningPod("uid-race", "race-pod", "c1", "c2", "c3")
	uid := string(pod.UID)
	store.RegisterPending(uid, pod, &creationHandle{cancel: func() {}, done: make(chan struct{})})
	store.PromoteRunning(uid, pod, "10.0.0.4")
	store.InitRestartState(pod)

	rt := newStubRuntime()
	rt.setMachines([]perigeos.PodMetadata{
		{UID: uid, ContainerName: "c1", State: perigeos.StateRunning},
		{UID: uid, ContainerName: "c2", State: perigeos.StateRunning},
		{UID: uid, ContainerName: "c3", State: perigeos.StateRunning},
	})

	bw, _ := startBW(t, store, rt)
	defer bw.Stop()

	// Hammer MarkRunning from multiple goroutines while the BatchWatcher
	// is polling. The race detector will catch any unsynchronized access.
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Go(func() {
			bw.MarkRunning(uid, "c1")
			bw.MarkRunning(uid, "c2")
			bw.MarkRunning(uid, "c3")
		})
	}
	for range 10 {
		wg.Go(func() {
			bw.Poke()
		})
	}
	wg.Wait()
}

// TestBatchWatcher_ReadinessTransitionPushed verifies that a readiness
// probe transition (false -> true) causes a status push even when the
// container's machine state doesn't change. This covers the prevReady
// tracking path.
func TestBatchWatcher_ReadinessTransitionPushed(t *testing.T) {
	store := setupTestStore()
	t.Cleanup(store.Close)

	pod := newRunningPod("uid-probe", "probe-pod", "nginx")
	uid := string(pod.UID)
	store.RegisterPending(uid, pod, &creationHandle{cancel: func() {}, done: make(chan struct{})})
	store.PromoteRunning(uid, pod, "10.0.0.5")
	store.InitRestartState(pod)
	// InitRestartState defaults Ready=true when there's no readiness probe.
	// Force it to false so the first poll seeds prevReady[key]=false and
	// the subsequent false->true transition is detectable.
	store.UpdateProbeState(uid, "nginx", func(ps *ContainerProbeState) {
		ps.Ready = false
	})

	rt := newStubRuntime()
	rt.setMachines([]perigeos.PodMetadata{
		{UID: uid, ContainerName: "nginx", State: perigeos.StateRunning},
	})

	bw, notified := startBW(t, store, rt)
	defer bw.Stop()
	bw.seenRunning[uid+"/nginx"] = true // prime seenRunning

	// First poll: container Running, not ready. Seeds prevReady[key]=false.
	bw.Poke()
	waitNotify(t, notified, func(p *corev1.Pod) bool {
		return string(p.UID) == uid
	}, "initial poll")

	// Simulate readiness probe passing.
	store.UpdateProbeState(uid, "nginx", func(ps *ContainerProbeState) {
		ps.Ready = true
	})

	// Second poll: readiness changed false->true, must trigger a push.
	bw.Poke()
	got := waitNotify(t, notified, func(p *corev1.Pod) bool {
		return string(p.UID) == uid &&
			len(p.Status.ContainerStatuses) > 0 &&
			p.Status.ContainerStatuses[0].Ready
	}, "readiness transition push")

	if !got.Status.ContainerStatuses[0].Ready {
		t.Error("expected ready=true after probe transition")
	}
	if got.Status.Conditions[0].Status != corev1.ConditionTrue {
		t.Errorf("expected pod Ready condition True, got %s", got.Status.Conditions[0].Status)
	}
}

// TestBatchWatcher_NoSpuriousPushWhenNothingChanges verifies the coalescer
// doesn't push status updates when state is stable. Reduces API server noise.
func TestBatchWatcher_NoSpuriousPushWhenNothingChanges(t *testing.T) {
	store := setupTestStore()
	t.Cleanup(store.Close)

	pod := newRunningPod("uid-stable", "stable-pod", "nginx")
	uid := string(pod.UID)
	store.RegisterPending(uid, pod, &creationHandle{cancel: func() {}, done: make(chan struct{})})
	store.PromoteRunning(uid, pod, "10.0.0.6")
	store.InitRestartState(pod)
	store.UpdateProbeState(uid, "nginx", func(ps *ContainerProbeState) { ps.Ready = true })

	rt := newStubRuntime()
	rt.setMachines([]perigeos.PodMetadata{
		{UID: uid, ContainerName: "nginx", State: perigeos.StateRunning},
	})

	bw, notified := startBW(t, store, rt)
	defer bw.Stop()
	bw.seenRunning[uid+"/nginx"] = true

	// First Poke: establishes prevStateMap and prevReady.
	bw.Poke()
	waitNotify(t, notified, func(p *corev1.Pod) bool {
		return string(p.UID) == uid
	}, "first poll")

	// Drain channel.
	time.Sleep(50 * time.Millisecond)
	for len(notified) > 0 {
		<-notified
	}

	// Second Poke with identical state: coalescer must not push.
	bw.Poke()
	time.Sleep(150 * time.Millisecond)

	if len(notified) > 0 {
		p := <-notified
		t.Errorf("spurious status push when nothing changed: phase=%s ready=%v",
			p.Status.Phase,
			len(p.Status.ContainerStatuses) > 0 && p.Status.ContainerStatuses[0].Ready)
	}
}
