package provider

import (
	"log/slog"

	"github.com/malformed-c/periapsis/internal/image"
	"github.com/malformed-c/periapsis/internal/network"
	pruntime "github.com/malformed-c/periapsis/internal/runtime"
	v1 "k8s.io/client-go/listers/core/v1"
)

// mockTracker is a simple PodTracker for use in tests.
type mockTracker struct {
	inFlight map[string]bool
	hasPod   map[string]bool
}

func newMockTracker() *mockTracker {
	return &mockTracker{
		inFlight: make(map[string]bool),
		hasPod:   make(map[string]bool),
	}
}

func (m *mockTracker) IsInFlight(uid string) bool { return m.inFlight[uid] }
func (m *mockTracker) HasPod(uid string) bool     { return m.hasPod[uid] }
func (m *mockTracker) PodUIDs() map[string]string {
	uids := make(map[string]string, len(m.hasPod))
	for uid := range m.hasPod {
		uids[uid] = "default/mock-pod"
	}
	return uids
}
func (m *mockTracker) EvictGhost(uid string) { delete(m.hasPod, uid) }

// TestReconciler wraps Reconciler with state-manipulation helpers.
// Exported so package provider_test can use it.
type TestReconciler struct {
	*Reconciler
	tracker *mockTracker
}

func (t *TestReconciler) MarkInFlight(uid string) { t.tracker.inFlight[uid] = true }
func (t *TestReconciler) MarkHasPod(uid string)   { t.tracker.hasPod[uid] = true }

// NewReconcilerForTest creates a Reconciler with a mock PodTracker instead of
// a real Gambit. The image.ImageManager uses /tmp so no special dirs are needed.
func NewReconcilerForTest(
	rt pruntime.Runtime,
	nm network.NetworkManager,
	podLister v1.PodNamespaceLister,
	logger *slog.Logger,
) *TestReconciler {
	tracker := newMockTracker()
	im := image.NewImageManager("/tmp/apsis-test", "test-pawn", slog.Default())
	return &TestReconciler{
		Reconciler: &Reconciler{
			tracker:   tracker,
			runtime:   rt,
			network:   nm,
			image:     im,
			podLister: podLister,
			logger:    logger,
			baseDir:   "/tmp/apsis-test",
			pawnName:  "test-pawn",
		},
		tracker: tracker,
	}
}
