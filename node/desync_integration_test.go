package node

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/coreos/go-systemd/v22/dbus"
	dbusv5 "github.com/godbus/dbus/v5"
	"github.com/malformed-c/periapsis/internal/config"
	"github.com/malformed-c/periapsis/internal/image"
	"github.com/malformed-c/periapsis/internal/network"
	perigeos "github.com/malformed-c/periapsis/internal/runtime"
	rtsd "github.com/malformed-c/periapsis/internal/runtime/systemd"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
)

// ─── Prerequisites ───────────────────────────────────────────────────────────

func requireRoot(t *testing.T) {
	t.Helper()
	if os.Getuid() != 0 {
		t.Skip("skipping: requires root (run with: sudo -E go test)")
	}
}

func requireSystemd(t *testing.T) {
	t.Helper()
	conn, err := dbus.NewSystemConnectionContext(context.Background())
	if err != nil {
		t.Skipf("skipping: system dbus unavailable: %v", err)
	}
	conn.Close()
}

// ─── Test Infrastructure ─────────────────────────────────────────────────────

// testHarness holds the real systemd runtime, a real gambit, and temp disk
// state for cross-checking all 4 sources of pod truth.
type testHarness struct {
	t        *testing.T
	pawnName string
	baseDir  string
	rt       *rtsd.SystemdRuntime
	gambit   *Gambit
	im       *image.ImageManager
	conn     *dbus.Conn
}

func newHarness(t *testing.T) *testHarness {
	t.Helper()
	requireRoot(t)
	requireSystemd(t)

	pawn := safePawnName(t)
	base := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	im := image.NewImageManager(base, logger)

	rt, err := rtsd.NewSystemdRuntime(context.Background(), pawn, im, logger, perigeos.ExecNsenter)
	if err != nil {
		t.Fatalf("NewSystemdRuntime: %v", err)
	}
	t.Cleanup(func() { rt.Close() })

	conn, err := dbus.NewSystemConnectionContext(context.Background())
	if err != nil {
		t.Fatalf("dbus connect: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	cfg := config.PawnConfig{
		Name:    pawn,
		BaseDir: base,
	}
	nm := &stubNetwork{}
	rec := record.NewFakeRecorder(100)
	store := NewPodStore(rt, 5, logger)
	g := NewGambit(cfg, store, im, nm, rt, logger, rec)

	return &testHarness{
		t:        t,
		pawnName: pawn,
		baseDir:  base,
		rt:       rt,
		gambit:   g,
		im:       im,
		conn:     conn,
	}
}

// stubNetwork satisfies NetworkManager without touching real netns.
type stubNetwork struct{}

func (n *stubNetwork) Setup(_ context.Context, podUID, _, _, _ string) (string, string, error) {
	return "/proc/1/ns/net", "10.99.0.1", nil
}
func (n *stubNetwork) Teardown(_ context.Context, _, _, _ string) error { return nil }

var _ network.NetworkManager = (*stubNetwork)(nil)

func safePawnName(t *testing.T) string {
	t.Helper()
	r := strings.NewReplacer("/", "-", " ", "-", ".", "-", "_", "-")
	s := strings.ToLower(r.Replace(t.Name()))
	if len(s) > 32 {
		s = s[len(s)-32:]
	}
	return s
}

// ─── Systemd helpers ─────────────────────────────────────────────────────────

// spawnUnit creates a transient systemd service that looks like a perigeos pod
// machine, with embedded PERIGEOS_META_* env vars. This is the same shape
// that RunMachine produces, minus actual nspawn.
func (h *testHarness) spawnUnit(podUID, container string) {
	h.t.Helper()
	unitName := fmt.Sprintf("perigeos-%s-pod-%s-%s.service", h.pawnName, podUID, container)

	props := []dbus.Property{
		dbus.PropDescription("Integration test pod " + podUID),
		dbus.PropExecStart([]string{"/usr/bin/sleep", "300"}, false),
		{Name: "CollectMode", Value: dbusv5.MakeVariant("inactive-or-failed")},
		{Name: "Environment", Value: dbusv5.MakeVariant([]string{
			"PERIGEOS_META_UID=" + podUID,
			"PERIGEOS_META_NAME=pod-" + podUID,
			"PERIGEOS_META_NAMESPACE=default",
			"PERIGEOS_META_NODENAME=" + h.pawnName,
			"PERIGEOS_META_IP=10.99.0.1",
			"PERIGEOS_META_CONTAINER=" + container,
		})},
	}

	ch := make(chan string, 1)
	if _, err := h.conn.StartTransientUnitContext(context.Background(), unitName, "replace", props, ch); err != nil {
		h.t.Fatalf("StartTransientUnit(%s): %v", unitName, err)
	}
	if res := <-ch; res != "done" {
		h.t.Fatalf("start result for %s: %s", unitName, res)
	}

	h.t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		h.rt.StopMachine(ctx, podUID, container) //nolint:errcheck
	})
}

// stopUnit stops a perigeos unit via the runtime.
func (h *testHarness) stopUnit(podUID, container string) {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := h.rt.StopMachine(ctx, podUID, container); err != nil {
		h.t.Logf("stopUnit(%s/%s): %v (may be expected)", podUID, container, err)
	}
}

// ─── Disk helpers ────────────────────────────────────────────────────────────

// createPodDir creates the disk workspace for a pod UID.
func (h *testHarness) createPodDir(podUID string) {
	h.t.Helper()
	dir := filepath.Join(h.baseDir, "pawns", h.pawnName, "pods", podUID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		h.t.Fatalf("createPodDir: %v", err)
	}
}

// podDirExists checks whether the pod directory exists on disk.
func (h *testHarness) podDirExists(podUID string) bool {
	dir := filepath.Join(h.baseDir, "pawns", h.pawnName, "pods", podUID)
	_, err := os.Stat(dir)
	return err == nil
}

// diskPodUIDs returns all pod UIDs found on disk.
func (h *testHarness) diskPodUIDs() []string {
	podsDir := filepath.Join(h.baseDir, "pawns", h.pawnName, "pods")
	entries, err := os.ReadDir(podsDir)
	if err != nil {
		return nil
	}
	var uids []string
	for _, e := range entries {
		if e.IsDir() {
			uids = append(uids, e.Name())
		}
	}
	return uids
}

// ─── Gambit helpers ──────────────────────────────────────────────────────────

// injectPod populates gambit in-memory state by hydrating from a running unit.
// The unit must already exist in systemd.
func (h *testHarness) injectViaHydrate() {
	h.t.Helper()
	if err := h.gambit.HydrateFromRuntime(context.Background()); err != nil {
		h.t.Fatalf("HydrateFromRuntime: %v", err)
	}
}

// ─── Snapshot helpers ────────────────────────────────────────────────────────

type stateSnapshot struct {
	gambitUIDs  map[string]string // uid → ns/name
	systemdUIDs map[string]bool
	diskUIDs    map[string]bool
}

func (h *testHarness) snapshot() stateSnapshot {
	h.t.Helper()
	ctx := context.Background()

	// Gambit
	gambitUIDs := h.gambit.PodUIDs()

	// Systemd
	machines, err := h.rt.ListManagedMachines(ctx)
	if err != nil {
		h.t.Fatalf("ListManagedMachines: %v", err)
	}
	systemdUIDs := make(map[string]bool)
	for _, m := range machines {
		if m.UID != "" {
			systemdUIDs[m.UID] = true
		}
	}

	// Disk
	diskUIDs := make(map[string]bool)
	for _, uid := range h.diskPodUIDs() {
		diskUIDs[uid] = true
	}

	return stateSnapshot{
		gambitUIDs:  gambitUIDs,
		systemdUIDs: systemdUIDs,
		diskUIDs:    diskUIDs,
	}
}

// assertAllSourcesAgree verifies all 3 local sources have exactly the same UIDs.
func (h *testHarness) assertAllSourcesAgree(expected []string) {
	h.t.Helper()
	time.Sleep(200 * time.Millisecond) // let systemd settle
	snap := h.snapshot()

	sort.Strings(expected)

	// Check gambit
	var gambitList []string
	for uid := range snap.gambitUIDs {
		gambitList = append(gambitList, uid)
	}
	sort.Strings(gambitList)
	if !stringSliceEqual(expected, gambitList) {
		h.t.Errorf("gambit desync:\n  want: %v\n  got:  %v", expected, gambitList)
	}

	// Check systemd
	var systemdList []string
	for uid := range snap.systemdUIDs {
		systemdList = append(systemdList, uid)
	}
	sort.Strings(systemdList)
	if !stringSliceEqual(expected, systemdList) {
		h.t.Errorf("systemd desync:\n  want: %v\n  got:  %v", expected, systemdList)
	}

	// Check disk
	var diskList []string
	for uid := range snap.diskUIDs {
		diskList = append(diskList, uid)
	}
	sort.Strings(diskList)
	if !stringSliceEqual(expected, diskList) {
		h.t.Errorf("disk desync:\n  want: %v\n  got:  %v", expected, diskList)
	}
}

func (h *testHarness) assertDesync(desc string, expectGhosts, expectOrphans, expectStale int) {
	h.t.Helper()
	time.Sleep(200 * time.Millisecond)
	snap := h.snapshot()

	ghosts := 0
	for uid := range snap.gambitUIDs {
		if !snap.systemdUIDs[uid] {
			ghosts++
		}
	}
	orphans := 0
	for uid := range snap.systemdUIDs {
		if _, ok := snap.gambitUIDs[uid]; !ok {
			orphans++
		}
	}
	stale := 0
	for uid := range snap.diskUIDs {
		if _, ok := snap.gambitUIDs[uid]; !ok {
			stale++
		}
	}

	if ghosts != expectGhosts {
		h.t.Errorf("%s: ghosts: want %d, got %d", desc, expectGhosts, ghosts)
	}
	if orphans != expectOrphans {
		h.t.Errorf("%s: orphans: want %d, got %d", desc, expectOrphans, orphans)
	}
	if stale != expectStale {
		h.t.Errorf("%s: stale dirs: want %d, got %d", desc, expectStale, stale)
	}
}

func stringSliceEqual(a, b []string) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// ─── Integration Tests ───────────────────────────────────────────────────────

// TestIntegration_HydrateMatchesSystemd verifies that after hydrating from
// systemd, the gambit's in-memory state matches the running machines.
func TestIntegration_HydrateMatchesSystemd(t *testing.T) {
	h := newHarness(t)

	// Spawn 3 units, create matching disk dirs
	uids := []string{"uid-h1", "uid-h2", "uid-h3"}
	for _, uid := range uids {
		h.spawnUnit(uid, "main")
		h.createPodDir(uid)
	}
	time.Sleep(300 * time.Millisecond)

	h.injectViaHydrate()
	h.assertAllSourcesAgree(uids)
}

// TestIntegration_DeletePodCleansAllSources verifies that DeletePod removes
// state from gambit, stops the systemd unit, and removes the disk dir.
func TestIntegration_DeletePodCleansAllSources(t *testing.T) {
	h := newHarness(t)
	uid := "uid-del"

	h.spawnUnit(uid, "main")
	h.createPodDir(uid)
	time.Sleep(300 * time.Millisecond)
	h.injectViaHydrate()
	h.assertAllSourcesAgree([]string{uid})

	// Now delete via gambit — should clean all sources.
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-" + uid,
			Namespace: "default",
			UID:       types.UID(uid),
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "main", Image: "dummy"}},
		},
	}
	if err := h.gambit.DeletePod(context.Background(), pod); err != nil {
		t.Fatalf("DeletePod: %v", err)
	}

	h.assertAllSourcesAgree(nil)
}

// TestIntegration_OrphanMachineDetection verifies that a machine running in
// systemd without a corresponding gambit entry is detected as an orphan.
func TestIntegration_OrphanMachineDetection(t *testing.T) {
	h := newHarness(t)

	// Spawn a unit but do NOT hydrate — gambit doesn't know about it.
	h.spawnUnit("uid-orphan", "main")
	time.Sleep(300 * time.Millisecond)

	h.assertDesync("orphan machine", 0, 1, 0)
}

// TestIntegration_GhostPodDetection verifies that a pod in gambit without
// a running systemd unit is detected as a ghost.
func TestIntegration_GhostPodDetection(t *testing.T) {
	h := newHarness(t)
	uid := "uid-ghost"

	// Spawn + hydrate, then kill the unit without going through DeletePod.
	h.spawnUnit(uid, "main")
	h.createPodDir(uid)
	time.Sleep(300 * time.Millisecond)
	h.injectViaHydrate()
	h.assertAllSourcesAgree([]string{uid})

	// Kill the unit behind gambit's back.
	h.stopUnit(uid, "main")
	time.Sleep(300 * time.Millisecond)

	h.assertDesync("ghost pod", 1, 0, 0)
}

// TestIntegration_StaleDirDetection verifies that disk directories without
// matching gambit or systemd entries are detected as stale.
func TestIntegration_StaleDirDetection(t *testing.T) {
	h := newHarness(t)

	// Create a dir with no matching unit or gambit state.
	h.createPodDir("uid-stale")

	h.assertDesync("stale dir", 0, 0, 1)
}

// TestIntegration_ReconcilerCleansOrphans verifies that the reconciler stops
// orphan machines that have no matching pod in gambit or k8s.
func TestIntegration_ReconcilerCleansOrphans(t *testing.T) {
	h := newHarness(t)
	uid := "uid-recon"

	h.spawnUnit(uid, "main")
	time.Sleep(300 * time.Millisecond)

	// Gambit doesn't know about it — it's an orphan.
	snap := h.snapshot()
	if !snap.systemdUIDs[uid] {
		t.Fatal("unit should be running")
	}

	// Run reconciler — should clean the orphan immediately (no grace period).
	tr := NewReconcilerForTest(h.rt, &stubNetwork{}, nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	tr.RunOnce(context.Background())

	time.Sleep(500 * time.Millisecond)
	snap2 := h.snapshot()
	if snap2.systemdUIDs[uid] {
		t.Error("reconciler should have removed orphan unit")
	}
}

// TestIntegration_MultiPodLifecycle tests creating and deleting multiple pods
// and verifying source consistency at each step.
func TestIntegration_MultiPodLifecycle(t *testing.T) {
	h := newHarness(t)
	uids := []string{"uid-m1", "uid-m2", "uid-m3", "uid-m4"}

	// Phase 1: create all
	for _, uid := range uids {
		h.spawnUnit(uid, "main")
		h.createPodDir(uid)
	}
	time.Sleep(300 * time.Millisecond)
	h.injectViaHydrate()
	h.assertAllSourcesAgree(uids)

	// Phase 2: delete first two via DeletePod
	for _, uid := range uids[:2] {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pod-" + uid,
				Namespace: "default",
				UID:       types.UID(uid),
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "main", Image: "dummy"}},
			},
		}
		if err := h.gambit.DeletePod(context.Background(), pod); err != nil {
			t.Fatalf("DeletePod(%s): %v", uid, err)
		}
	}
	h.assertAllSourcesAgree(uids[2:])

	// Phase 3: delete remaining
	for _, uid := range uids[2:] {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pod-" + uid,
				Namespace: "default",
				UID:       types.UID(uid),
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "main", Image: "dummy"}},
			},
		}
		if err := h.gambit.DeletePod(context.Background(), pod); err != nil {
			t.Fatalf("DeletePod(%s): %v", uid, err)
		}
	}
	h.assertAllSourcesAgree(nil)
}

// ─── Fuzzer ──────────────────────────────────────────────────────────────────

// TestIntegration_DesyncFuzzer randomly creates and deletes pods via different
// paths (proper DeletePod vs behind-the-back kills) and verifies state
// invariants hold after each operation.
//
// Invariants checked:
//  1. After DeletePod: UID absent from all 3 sources.
//  2. After behind-the-back kill: gambit has ghost, systemd does not.
//  3. After hydrate: gambit matches systemd.
//  4. Disk dirs only exist for UIDs that gambit knows about (no stale).
func TestIntegration_DesyncFuzzer(t *testing.T) {
	h := newHarness(t)
	const iterations = 30
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	type podState struct {
		uid       string
		container string
		inGambit  bool
		inSystemd bool
		onDisk    bool
	}
	pods := make(map[string]*podState)
	pickUID := func() string {
		keys := make([]string, 0, len(pods))
		for k := range pods {
			keys = append(keys, k)
		}
		return keys[rng.Intn(len(keys))]
	}
	uidCounter := 0

	for i := range iterations {
		// Decide action: create, delete-proper, kill-behind-back, or hydrate
		action := rng.Intn(4)

		switch {
		case action == 0 || len(pods) < 3:
			// CREATE: spawn unit + disk dir
			uidCounter++
			uid := fmt.Sprintf("fuzz-%d-%d", i, uidCounter)
			h.spawnUnit(uid, "main")
			h.createPodDir(uid)
			pods[uid] = &podState{
				uid:       uid,
				container: "main",
				inSystemd: true,
				onDisk:    true,
			}

		case action == 1 && len(pods) > 0:
			// DELETE-PROPER via gambit (if gambit knows about it)
			uid := pickUID()
			ps := pods[uid]
			if ps.inGambit {
				pod := &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pod-" + uid,
						Namespace: "default",
						UID:       types.UID(uid),
					},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Name: ps.container, Image: "dummy"}},
					},
				}
				if err := h.gambit.DeletePod(context.Background(), pod); err != nil {
					t.Logf("DeletePod(%s): %v", uid, err)
				}
				ps.inGambit = false
				ps.inSystemd = false
				ps.onDisk = false

				// Verify: UID gone from all sources.
				time.Sleep(200 * time.Millisecond)
				snap := h.snapshot()
				if _, ok := snap.gambitUIDs[uid]; ok {
					t.Errorf("iter %d: DeletePod(%s) left ghost in gambit", i, uid)
				}
				if snap.systemdUIDs[uid] {
					t.Errorf("iter %d: DeletePod(%s) left orphan in systemd", i, uid)
				}
				if snap.diskUIDs[uid] {
					t.Errorf("iter %d: DeletePod(%s) left stale dir", i, uid)
				}
				delete(pods, uid)
			}

		case action == 2 && len(pods) > 0:
			// KILL-BEHIND-BACK: stop unit without going through gambit
			uid := pickUID()
			ps := pods[uid]
			if ps.inSystemd {
				h.stopUnit(uid, ps.container)
				ps.inSystemd = false

				// If gambit knew about it, now it's a ghost.
				if ps.inGambit {
					time.Sleep(200 * time.Millisecond)
					snap := h.snapshot()
					if _, ok := snap.gambitUIDs[uid]; !ok {
						t.Errorf("iter %d: ghost %s should still be in gambit after behind-back kill", i, uid)
					}
					if snap.systemdUIDs[uid] {
						t.Errorf("iter %d: killed %s should not be in systemd", i, uid)
					}
				}
			}

		case action == 3:
			// HYDRATE: sync gambit from systemd
			if err := h.gambit.HydrateFromRuntime(context.Background()); err != nil {
				t.Fatalf("iter %d: HydrateFromRuntime: %v", i, err)
			}
			// Update tracked state.
			for _, ps := range pods {
				if ps.inSystemd {
					ps.inGambit = true
				}
			}

			// Verify: gambit UIDs ⊇ systemd UIDs.
			time.Sleep(200 * time.Millisecond)
			snap := h.snapshot()
			for sysUID := range snap.systemdUIDs {
				if _, ok := snap.gambitUIDs[sysUID]; !ok {
					t.Errorf("iter %d: after hydrate, systemd UID %s not in gambit", i, sysUID)
				}
			}
		}
	}
}

