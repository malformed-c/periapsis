package control

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/malformed-c/periapsis/internal/config"
	"github.com/malformed-c/periapsis/internal/image"
	perigeos "github.com/malformed-c/periapsis/internal/runtime"
	"github.com/malformed-c/periapsis/node"
	"github.com/malformed-c/periapsis/node/api"
	"k8s.io/client-go/tools/record"
)

// ─── Mock Runtime ─────────────────────────────────────────────────────────────

// doctorMockRuntime implements perigeos.Runtime for doctor tests.
// Only ListManagedMachines is exercised by doctor; all other methods are stubs.
type doctorMockRuntime struct {
	machines []perigeos.PodMetadata
}

func (r *doctorMockRuntime) RunMachine(_ context.Context, _ string, _ perigeos.PodConfig) error {
	return nil
}
func (r *doctorMockRuntime) StopMachine(_ context.Context, _, _ string) error {
	return nil
}
func (r *doctorMockRuntime) MachineStatus(_ context.Context, _, _ string) (perigeos.MachineState, error) {
	return perigeos.StateUnknown, nil
}
func (r *doctorMockRuntime) WaitForMachineExit(_ context.Context, _, _ string, _ time.Duration) (perigeos.MachineState, error) {
	return perigeos.StateUnknown, nil
}
func (r *doctorMockRuntime) ListManagedMachines(_ context.Context) ([]perigeos.PodMetadata, error) {
	return r.machines, nil
}
func (r *doctorMockRuntime) GetLogStream(_ context.Context, _, _ string, _ api.ContainerLogOpts) (io.ReadCloser, error) {
	return io.NopCloser(nil), nil
}
func (r *doctorMockRuntime) RunInContainer(_ context.Context, _, _ string, _ []string, _ api.AttachIO) error {
	return nil
}
func (r *doctorMockRuntime) AttachToContainer(_ context.Context, _, _ string, _ api.AttachIO) error {
	return nil
}
func (r *doctorMockRuntime) InitPawnSlice(_ context.Context, _ perigeos.PawnSliceConfig) error {
	return nil
}
func (r *doctorMockRuntime) CheckMachined(_ context.Context) error {
	return nil
}
func (r *doctorMockRuntime) SubscribeEvents(_ context.Context) <-chan perigeos.UnitEvent {
	return nil
}
func (r *doctorMockRuntime) MakeSharedMounts(_ context.Context, _, _ string, _ []perigeos.BindMount) error {
	return nil
}
func (r *doctorMockRuntime) ResetUnit(_ context.Context, _, _ string) error {
	return nil
}
func (r *doctorMockRuntime) CleanupStaleUnits(_ context.Context, _ map[string]bool) (int, error) {
	return 0, nil
}
func (r *doctorMockRuntime) SliceActive(ctx context.Context) bool {
	return true
}

// ─── Mock Network ─────────────────────────────────────────────────────────────

// doctorMockNetwork implements network.NetworkManager. All calls are no-ops.
type doctorMockNetwork struct{}

func (n *doctorMockNetwork) Setup(_ context.Context, podUID, _, _, _ string) (string, string, error) {
	return "/var/run/netns/" + podUID, "10.0.0.1", nil
}
func (n *doctorMockNetwork) Teardown(_ context.Context, _, _, _ string) error {
	return nil
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

// newDoctorTestGambit creates a Gambit with a mock runtime and a temp BaseDir.
// machines is the initial list of systemd machines. The gambit's pods map is
// populated by calling HydrateFromRuntime — use this to seed in-memory state.
func newDoctorTestGambit(t *testing.T, pawnName string, machines []perigeos.PodMetadata) (*node.Gambit, *doctorMockRuntime) {
	t.Helper()
	baseDir := t.TempDir()
	cfg := config.PawnConfig{
		Name:    pawnName,
		BaseDir: baseDir,
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	rt := &doctorMockRuntime{machines: machines}
	nm := &doctorMockNetwork{}
	im := image.NewImageManager(baseDir, logger)
	rec := record.NewFakeRecorder(100)
	store := node.NewPodStore(rt, 5, logger)
	volumes := node.NewVolumeTracker(cfg.BaseDir, cfg.Name, logger)
	pawnNode := node.NewPawnNode(cfg, store, im, logger)
	g := node.NewGambit(node.GambitDeps{
		Config:         cfg,
		Store:          store,
		Volumes:        volumes,
		Node:           pawnNode,
		ImageManager:   im,
		NetworkManager: nm,
		Runtime:        rt,
		Logger:         logger,
		EventRecorder:  rec,
	})
	pawnNode.SetDeletePod(g.DeletePod)
	return g, rt
}

// makeDiskPodDir creates the on-disk directory for a pod UID under the gambit's BaseDir.
func makeDiskPodDir(t *testing.T, g *node.Gambit, uid string) {
	t.Helper()
	dir := filepath.Join(g.Config.BaseDir, "pawns", g.Config.Name, "pods", uid)
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatalf("makeDiskPodDir: %v", err)
	}
}

// newDoctorServer creates a control Server wired up to a single gambit.
func newDoctorServer(g *node.Gambit) *Server {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	s := &Server{logger: logger}
	s.gambits = []*node.Gambit{g}
	return s
}

// callDoctor calls buildDoctor on the server and unmarshals the response.
func callDoctor(t *testing.T, s *Server) DoctorResponse {
	t.Helper()
	result := s.buildDoctor(context.Background())
	b, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("marshal doctor result: %v", err)
	}
	var resp DoctorResponse
	if err := json.Unmarshal(b, &resp); err != nil {
		t.Fatalf("decode DoctorResponse: %v", err)
	}
	return resp
}

// ─── scanDiskPods tests ────────────────────────────────────────────────────────

func TestScanDiskPods(t *testing.T) {
	baseDir := t.TempDir()
	pawnName := "test-pawn"
	podsDir := filepath.Join(baseDir, "pawns", pawnName, "pods")

	uids := []string{"uid-aaa", "uid-bbb", "uid-ccc"}
	for _, uid := range uids {
		if err := os.MkdirAll(filepath.Join(podsDir, uid), 0755); err != nil {
			t.Fatal(err)
		}
	}
	// Also put a file (not a dir) — should be ignored.
	if err := os.WriteFile(filepath.Join(podsDir, "not-a-uid.txt"), []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}

	got := scanDiskPods(baseDir, pawnName)
	sort.Strings(got)
	sort.Strings(uids)

	if len(got) != len(uids) {
		t.Fatalf("scanDiskPods returned %d entries, want %d: %v", len(got), len(uids), got)
	}
	for i, want := range uids {
		if got[i] != want {
			t.Errorf("scanDiskPods[%d] = %q, want %q", i, got[i], want)
		}
	}
}

func TestScanDiskPods_Empty(t *testing.T) {
	// Non-existent pods directory must return nil (not an error).
	got := scanDiskPods(t.TempDir(), "no-such-pawn")
	if got != nil {
		t.Errorf("expected nil for missing dir, got %v", got)
	}
}

func TestScanDiskPods_OnlyDirs(t *testing.T) {
	baseDir := t.TempDir()
	pawnName := "pawn"
	podsDir := filepath.Join(baseDir, "pawns", pawnName, "pods")
	if err := os.MkdirAll(podsDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Create only regular files — result must be empty.
	for _, name := range []string{"file1", "file2"} {
		if err := os.WriteFile(filepath.Join(podsDir, name), nil, 0644); err != nil {
			t.Fatal(err)
		}
	}

	got := scanDiskPods(baseDir, pawnName)
	if len(got) != 0 {
		t.Errorf("expected 0 UIDs when only files present, got %v", got)
	}
}

// ─── diagnosePawn / doctor handler tests ──────────────────────────────────────

func TestDoctorHealthy(t *testing.T) {
	// gambit, systemd and disk all agree: one pod, uid-1.
	machines := []perigeos.PodMetadata{
		{UID: "uid-1", Name: "mypod", Namespace: "default"},
	}
	g, _ := newDoctorTestGambit(t, "pawn0", machines)
	if err := g.HydrateFromRuntime(context.Background()); err != nil {
		t.Fatal(err)
	}
	makeDiskPodDir(t, g, "uid-1")

	s := newDoctorServer(g)
	resp := callDoctor(t, s)

	if !resp.Healthy {
		t.Errorf("expected Healthy=true, got false; pawns=%+v", resp.Pawns)
	}
	if len(resp.Pawns) != 1 {
		t.Fatalf("expected 1 pawn diagnosis, got %d", len(resp.Pawns))
	}
	d := resp.Pawns[0]
	if d.GambitPods != 1 || d.SystemdUnits != 1 || d.DiskDirs != 1 {
		t.Errorf("unexpected counts: gambit=%d systemd=%d disk=%d", d.GambitPods, d.SystemdUnits, d.DiskDirs)
	}
	if len(d.GhostPods)+len(d.OrphanMachines)+len(d.StaleDirs)+len(d.MissingDirs) != 0 {
		t.Errorf("expected no discrepancies, got %+v", d)
	}
}

func TestDoctorGhostPods(t *testing.T) {
	// uid-ghost: in gambit (hydrated), NOT in systemd after the runtime is mutated.
	machines := []perigeos.PodMetadata{
		{UID: "uid-ghost", Name: "ghost", Namespace: "default"},
		{UID: "uid-ok", Name: "ok", Namespace: "default"},
	}
	g, rt := newDoctorTestGambit(t, "pawn0", machines)
	if err := g.HydrateFromRuntime(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Gambit now has uid-ghost + uid-ok in memory.
	// Disk: both present.
	makeDiskPodDir(t, g, "uid-ghost")
	makeDiskPodDir(t, g, "uid-ok")

	// Simulate systemd drift: uid-ghost is gone.
	rt.machines = []perigeos.PodMetadata{
		{UID: "uid-ok", Name: "ok", Namespace: "default"},
	}

	s := newDoctorServer(g)
	resp := callDoctor(t, s)

	if resp.Healthy {
		t.Error("expected Healthy=false due to ghost pod")
	}
	d := resp.Pawns[0]
	if len(d.GhostPods) != 1 {
		t.Fatalf("expected 1 ghost pod, got %d: %v", len(d.GhostPods), d.GhostPods)
	}
	if d.GhostPods[0].UID != "uid-ghost" {
		t.Errorf("wrong ghost UID: %q", d.GhostPods[0].UID)
	}
	if len(d.OrphanMachines) != 0 {
		t.Errorf("unexpected orphans: %v", d.OrphanMachines)
	}
}

func TestDoctorOrphanMachines(t *testing.T) {
	// uid-orphan: in systemd, NOT in gambit.
	machines := []perigeos.PodMetadata{
		{UID: "uid-ok", Name: "ok", Namespace: "default"},
	}
	g, rt := newDoctorTestGambit(t, "pawn0", machines)
	if err := g.HydrateFromRuntime(context.Background()); err != nil {
		t.Fatal(err)
	}
	makeDiskPodDir(t, g, "uid-ok")

	// Systemd now has an extra machine gambit doesn't know about.
	rt.machines = []perigeos.PodMetadata{
		{UID: "uid-ok", Name: "ok", Namespace: "default"},
		{UID: "uid-orphan", Name: "orphan", Namespace: "kube-system"},
	}

	s := newDoctorServer(g)
	resp := callDoctor(t, s)

	if resp.Healthy {
		t.Error("expected Healthy=false due to orphan machine")
	}
	d := resp.Pawns[0]
	if len(d.OrphanMachines) != 1 {
		t.Fatalf("expected 1 orphan machine, got %d: %v", len(d.OrphanMachines), d.OrphanMachines)
	}
	if d.OrphanMachines[0].UID != "uid-orphan" {
		t.Errorf("wrong orphan UID: %q", d.OrphanMachines[0].UID)
	}
	if len(d.GhostPods) != 0 {
		t.Errorf("unexpected ghosts: %v", d.GhostPods)
	}
}

func TestDoctorStaleDirs(t *testing.T) {
	// uid-stale: dir exists on disk, NOT in gambit.
	machines := []perigeos.PodMetadata{
		{UID: "uid-ok", Name: "ok", Namespace: "default"},
	}
	g, _ := newDoctorTestGambit(t, "pawn0", machines)
	if err := g.HydrateFromRuntime(context.Background()); err != nil {
		t.Fatal(err)
	}
	makeDiskPodDir(t, g, "uid-ok")
	makeDiskPodDir(t, g, "uid-stale") // not in gambit or systemd

	s := newDoctorServer(g)
	resp := callDoctor(t, s)

	if resp.Healthy {
		t.Error("expected Healthy=false due to stale dir")
	}
	d := resp.Pawns[0]
	if len(d.StaleDirs) != 1 {
		t.Fatalf("expected 1 stale dir, got %d: %v", len(d.StaleDirs), d.StaleDirs)
	}
	if d.StaleDirs[0] != "uid-stale" {
		t.Errorf("wrong stale UID: %q", d.StaleDirs[0])
	}
}

func TestDoctorMissingDirs(t *testing.T) {
	// uid-missing: in gambit, NOT on disk.
	machines := []perigeos.PodMetadata{
		{UID: "uid-ok", Name: "ok", Namespace: "default"},
		{UID: "uid-missing", Name: "missing", Namespace: "default"},
	}
	g, _ := newDoctorTestGambit(t, "pawn0", machines)
	if err := g.HydrateFromRuntime(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Only create disk dir for uid-ok; uid-missing has no disk dir.
	makeDiskPodDir(t, g, "uid-ok")

	s := newDoctorServer(g)
	resp := callDoctor(t, s)

	if resp.Healthy {
		t.Error("expected Healthy=false due to missing dir")
	}
	d := resp.Pawns[0]
	if len(d.MissingDirs) != 1 {
		t.Fatalf("expected 1 missing dir, got %d: %v", len(d.MissingDirs), d.MissingDirs)
	}
	if d.MissingDirs[0].UID != "uid-missing" {
		t.Errorf("wrong missing UID: %q", d.MissingDirs[0].UID)
	}
}

func TestDoctorMultipleDesyncTypes(t *testing.T) {
	// Set up a scenario with all four discrepancy types at once.
	//
	// After HydrateFromRuntime (initial machines):
	//   gambit in-memory: uid-a, uid-b, uid-c, uid-d
	//
	// Disk:
	//   uid-a ✓  uid-b ✓  uid-c missing  uid-d ✓  uid-stale (not in gambit)
	//
	// Systemd (after runtime mutation):
	//   uid-a ✓  uid-b ✓  uid-d missing (ghost)  uid-orphan (not in gambit)
	//
	// Expected:
	//   ghost:   uid-c  (in gambit, not in systemd)
	//   orphan:  uid-orphan (in systemd, not in gambit)
	//   stale:   uid-stale  (on disk, not in gambit)
	//   missing: uid-d  (in gambit, not on disk) — wait, uid-d has a disk dir
	//
	// Revised plan:
	//   gambit:  uid-a, uid-b, uid-c, uid-d
	//   systemd: uid-a, uid-b, uid-orphan          → ghost=uid-c,uid-d; orphan=uid-orphan
	//   disk:    uid-a, uid-b, uid-stale            → stale=uid-stale;   missing=uid-c,uid-d

	initial := []perigeos.PodMetadata{
		{UID: "uid-a", Name: "pod-a", Namespace: "default"},
		{UID: "uid-b", Name: "pod-b", Namespace: "default"},
		{UID: "uid-c", Name: "pod-c", Namespace: "default"},
		{UID: "uid-d", Name: "pod-d", Namespace: "default"},
	}
	g, rt := newDoctorTestGambit(t, "pawn0", initial)
	if err := g.HydrateFromRuntime(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Disk: uid-a, uid-b, uid-stale
	makeDiskPodDir(t, g, "uid-a")
	makeDiskPodDir(t, g, "uid-b")
	makeDiskPodDir(t, g, "uid-stale")

	// Systemd drifts: uid-a, uid-b, uid-orphan
	rt.machines = []perigeos.PodMetadata{
		{UID: "uid-a", Name: "pod-a", Namespace: "default"},
		{UID: "uid-b", Name: "pod-b", Namespace: "default"},
		{UID: "uid-orphan", Name: "pod-orphan", Namespace: "kube-system"},
	}

	s := newDoctorServer(g)
	resp := callDoctor(t, s)

	if resp.Healthy {
		t.Error("expected Healthy=false")
	}
	d := resp.Pawns[0]

	// Ghost pods: uid-c and uid-d (in gambit, not in systemd)
	if len(d.GhostPods) != 2 {
		t.Errorf("expected 2 ghost pods, got %d: %v", len(d.GhostPods), d.GhostPods)
	}

	// Orphan machines: uid-orphan
	if len(d.OrphanMachines) != 1 || d.OrphanMachines[0].UID != "uid-orphan" {
		t.Errorf("expected [uid-orphan] orphan, got %v", d.OrphanMachines)
	}

	// Stale dirs: uid-stale
	if len(d.StaleDirs) != 1 || d.StaleDirs[0] != "uid-stale" {
		t.Errorf("expected [uid-stale] stale, got %v", d.StaleDirs)
	}

	// Missing dirs: uid-c and uid-d
	if len(d.MissingDirs) != 2 {
		t.Errorf("expected 2 missing dirs, got %d: %v", len(d.MissingDirs), d.MissingDirs)
	}

	// Summary correctness
	if resp.Summary.TotalGhosts != 2 {
		t.Errorf("summary TotalGhosts want 2, got %d", resp.Summary.TotalGhosts)
	}
	if resp.Summary.TotalOrphans != 1 {
		t.Errorf("summary TotalOrphans want 1, got %d", resp.Summary.TotalOrphans)
	}
	if resp.Summary.TotalStaleDirs != 1 {
		t.Errorf("summary TotalStaleDirs want 1, got %d", resp.Summary.TotalStaleDirs)
	}
}

func TestDoctorEmptyGambit(t *testing.T) {
	// No pods anywhere — should be healthy with all-zero counts.
	g, _ := newDoctorTestGambit(t, "pawn0", nil)

	s := newDoctorServer(g)
	resp := callDoctor(t, s)

	if !resp.Healthy {
		t.Error("expected Healthy=true for empty gambit")
	}
	if len(resp.Pawns) != 1 {
		t.Fatalf("expected 1 pawn, got %d", len(resp.Pawns))
	}
	d := resp.Pawns[0]
	if d.GambitPods != 0 || d.SystemdUnits != 0 || d.DiskDirs != 0 {
		t.Errorf("expected all zeros, got %+v", d)
	}
}

func TestDoctorMultipleContainersSameUID(t *testing.T) {
	// A multi-container pod produces multiple machines with the same UID.
	// The doctor should deduplicate — systemdUIDs counts unique UIDs.
	machines := []perigeos.PodMetadata{
		{UID: "uid-multi", Name: "mypod", Namespace: "default", ContainerName: "app"},
		{UID: "uid-multi", Name: "mypod", Namespace: "default", ContainerName: "sidecar"},
	}
	g, _ := newDoctorTestGambit(t, "pawn0", machines)
	if err := g.HydrateFromRuntime(context.Background()); err != nil {
		t.Fatal(err)
	}
	makeDiskPodDir(t, g, "uid-multi")

	s := newDoctorServer(g)
	resp := callDoctor(t, s)

	if !resp.Healthy {
		t.Errorf("expected Healthy=true; diag=%+v", resp.Pawns)
	}
	d := resp.Pawns[0]
	if d.SystemdUnits != 1 {
		t.Errorf("expected 1 unique UID from 2 machines, got SystemdUnits=%d", d.SystemdUnits)
	}
}

func TestDoctorNoPawns(t *testing.T) {
	// Server with no gambits — should return healthy with empty slice.
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	s := &Server{logger: logger}

	resp := callDoctor(t, s)
	if !resp.Healthy {
		t.Error("expected Healthy=true with no pawns")
	}
	if len(resp.Pawns) != 0 {
		t.Errorf("expected empty Pawns, got %v", resp.Pawns)
	}
}

// ─── Fuzz / property-based test ───────────────────────────────────────────────

// TestDoctorFuzz runs 100 random scenarios and verifies the cross-reference
// logic is consistent (no UID can appear in both ghost and orphan, counts
// match slices, etc.).
func TestDoctorFuzz(t *testing.T) {
	const iterations = 100
	rng := rand.New(rand.NewSource(42))

	for iter := 0; iter < iterations; iter++ {
		// Choose sizes randomly.
		nTotal := rng.Intn(12) + 1 // 1..12 UIDs in the universe

		// Assign each UID to sets randomly.
		inGambit := make(map[string]bool)
		inSystemd := make([]perigeos.PodMetadata, 0)
		inDisk := make(map[string]bool)

		allUIDs := make([]string, nTotal)
		for i := 0; i < nTotal; i++ {
			allUIDs[i] = fmt.Sprintf("uid-%d-%d", iter, i)
		}

		var hydrateUIDs []perigeos.PodMetadata
		for _, uid := range allUIDs {
			g := rng.Intn(2) == 1
			s := rng.Intn(2) == 1
			d := rng.Intn(2) == 1

			if g {
				// Add to gambit by also adding to initial hydration list.
				hydrateUIDs = append(hydrateUIDs, perigeos.PodMetadata{
					UID:       uid,
					Name:      "pod-" + uid,
					Namespace: "default",
				})
				inGambit[uid] = true
			}
			if s {
				inSystemd = append(inSystemd, perigeos.PodMetadata{
					UID:       uid,
					Name:      "pod-" + uid,
					Namespace: "default",
				})
			}
			if d {
				inDisk[uid] = true
			}
		}

		// Build gambit.
		pawnName := fmt.Sprintf("pawn-fuzz-%d", iter)
		baseDir := t.TempDir()
		cfg := config.PawnConfig{Name: pawnName, BaseDir: baseDir}
		rt := &doctorMockRuntime{machines: hydrateUIDs}
		nm := &doctorMockNetwork{}
		logger := slog.New(slog.NewTextHandler(io.Discard, nil))
		im := image.NewImageManager(baseDir, logger)
		rec := record.NewFakeRecorder(100)
		store := node.NewPodStore(rt, 5, logger)
		volumes := node.NewVolumeTracker(cfg.BaseDir, cfg.Name, logger)
		pawnNode := node.NewPawnNode(cfg, store, im, logger)
		g := node.NewGambit(node.GambitDeps{
			Config:         cfg,
			Store:          store,
			Volumes:        volumes,
			Node:           pawnNode,
			ImageManager:   im,
			NetworkManager: nm,
			Runtime:        rt,
			Logger:         logger,
			EventRecorder:  rec,
		})
		pawnNode.SetDeletePod(g.DeletePod)

		if err := g.HydrateFromRuntime(context.Background()); err != nil {
			t.Fatalf("iter %d: HydrateFromRuntime: %v", iter, err)
		}

		// Now set systemd to the (potentially different) inSystemd slice.
		rt.machines = inSystemd

		// Create disk dirs.
		for uid := range inDisk {
			dir := filepath.Join(baseDir, "pawns", pawnName, "pods", uid)
			if err := os.MkdirAll(dir, 0755); err != nil {
				t.Fatalf("iter %d: mkdir: %v", iter, err)
			}
		}

		// Build a throwaway server and run doctor.
		s := &Server{
			logger:  logger,
			gambits: []*node.Gambit{g},
		}
		resp := callDoctor(t, s)

		if len(resp.Pawns) != 1 {
			t.Fatalf("iter %d: expected 1 pawn, got %d", iter, len(resp.Pawns))
		}
		diag := resp.Pawns[0]

		// Build the deduplicated systemd UID set (mirrors diagnosePawn logic).
		systemdUIDs := make(map[string]bool)
		for _, m := range inSystemd {
			if m.UID != "" {
				systemdUIDs[m.UID] = true
			}
		}

		// Property 1: every ghost UID is in gambit but NOT in systemd.
		for _, e := range diag.GhostPods {
			if !inGambit[e.UID] {
				t.Errorf("iter %d: ghost %q not in gambit", iter, e.UID)
			}
			if systemdUIDs[e.UID] {
				t.Errorf("iter %d: ghost %q is also in systemd", iter, e.UID)
			}
		}

		// Property 2: every orphan UID is in systemd but NOT in gambit.
		for _, e := range diag.OrphanMachines {
			if !systemdUIDs[e.UID] {
				t.Errorf("iter %d: orphan %q not in systemd", iter, e.UID)
			}
			if inGambit[e.UID] {
				t.Errorf("iter %d: orphan %q is also in gambit", iter, e.UID)
			}
		}

		// Property 3: every stale UID is on disk but NOT in gambit.
		for _, uid := range diag.StaleDirs {
			if !inDisk[uid] {
				t.Errorf("iter %d: stale %q not on disk", iter, uid)
			}
			if inGambit[uid] {
				t.Errorf("iter %d: stale %q is also in gambit", iter, uid)
			}
		}

		// Property 4: every missing UID is in gambit but NOT on disk.
		for _, e := range diag.MissingDirs {
			if !inGambit[e.UID] {
				t.Errorf("iter %d: missing %q not in gambit", iter, e.UID)
			}
			if inDisk[e.UID] {
				t.Errorf("iter %d: missing %q is also on disk", iter, e.UID)
			}
		}

		// Property 5: no UID appears in both ghost and orphan.
		ghostSet := make(map[string]bool)
		for _, e := range diag.GhostPods {
			ghostSet[e.UID] = true
		}
		for _, e := range diag.OrphanMachines {
			if ghostSet[e.UID] {
				t.Errorf("iter %d: UID %q appears in both ghost and orphan", iter, e.UID)
			}
		}

		// Property 6: Healthy flag is false iff any discrepancy list is non-empty.
		hasIssues := len(diag.GhostPods)+len(diag.OrphanMachines)+len(diag.StaleDirs)+len(diag.MissingDirs) > 0
		if hasIssues == resp.Healthy {
			t.Errorf("iter %d: Healthy=%v but hasIssues=%v", iter, resp.Healthy, hasIssues)
		}

		// Property 7: summary counts match sums across pawns.
		if resp.Summary.TotalGhosts != len(diag.GhostPods) {
			t.Errorf("iter %d: TotalGhosts %d != len(GhostPods) %d",
				iter, resp.Summary.TotalGhosts, len(diag.GhostPods))
		}
		if resp.Summary.TotalOrphans != len(diag.OrphanMachines) {
			t.Errorf("iter %d: TotalOrphans %d != len(OrphanMachines) %d",
				iter, resp.Summary.TotalOrphans, len(diag.OrphanMachines))
		}
		if resp.Summary.TotalStaleDirs != len(diag.StaleDirs) {
			t.Errorf("iter %d: TotalStaleDirs %d != len(StaleDirs) %d",
				iter, resp.Summary.TotalStaleDirs, len(diag.StaleDirs))
		}
	}
}
