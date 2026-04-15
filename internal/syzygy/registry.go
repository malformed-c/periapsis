package syzygy

// FocusRegistry manages per-pod Focus actors.
//
// The registry is the single place where Foci are created, looked up,
// and destroyed. Syzygy routes Facts through the registry to the
// correct Focus based on the Fact's UID.

import (
        "context"
        "fmt"
        "log/slog"
        "sync"

        "github.com/malformed-c/periapsis/internal/foci"
        "github.com/malformed-c/periapsis/internal/types"
)

// FocusRegistry manages the lifecycle of Focus actors.
type FocusRegistry struct {
        mu     sync.RWMutex
        foci   map[string]*foci.Focus // uid -> Focus
        logger *slog.Logger
}

// NewFocusRegistry creates a new FocusRegistry.
func NewFocusRegistry(logger *slog.Logger) *FocusRegistry {
        if logger == nil {
                logger = slog.Default()
        }
        return &FocusRegistry{
                foci:   make(map[string]*foci.Focus),
                logger: logger.With("component", "focus-registry"),
        }
}

// Get returns the Focus for a pod UID, or nil if no Focus exists.
func (r *FocusRegistry) Get(uid string) *foci.Focus {
        r.mu.RLock()
        defer r.mu.RUnlock()
        return r.foci[uid]
}

// Create creates a new Focus for a pod and starts its event loop.
// Returns an error if a Focus already exists for this UID.
func (r *FocusRegistry) Create(ctx context.Context, cfg foci.FocusConfig) (*foci.Focus, error) {
        r.mu.Lock()
        defer r.mu.Unlock()

        if _, exists := r.foci[cfg.UID]; exists {
                return nil, fmt.Errorf("focus already exists for uid %s", cfg.UID)
        }

        focus := foci.NewFocus(cfg)
        r.foci[cfg.UID] = focus

        go focus.Run(ctx)

        r.logger.Info("focus created", "pod", cfg.PodName, "uid", cfg.UID)
        return focus, nil
}

// Remove stops and removes a Focus. The Focus's event loop will
// drain remaining facts before exiting.
func (r *FocusRegistry) Remove(uid string) {
        r.mu.Lock()
        defer r.mu.Unlock()

        focus, ok := r.foci[uid]
        if !ok {
                return
        }

        delete(r.foci, uid)
        r.logger.Info("focus removed", "uid", uid)
        _ = focus // caller should cancel the context to stop the Focus
}

// UIDs returns all tracked pod UIDs.
func (r *FocusRegistry) UIDs() []string {
        r.mu.RLock()
        defer r.mu.RUnlock()

        uids := make([]string, 0, len(r.foci))
        for uid := range r.foci {
                uids = append(uids, uid)
        }
        return uids
}

// SnapshotAll returns the computed FocusSnapshot for all tracked pods.
// Used by the anti-entropy loop to detect drift.
func (r *FocusRegistry) SnapshotAll() map[string]foci.FocusSnapshot {
        r.mu.RLock()
        defer r.mu.RUnlock()

        snapshots := make(map[string]foci.FocusSnapshot, len(r.foci))
        for uid, focus := range r.foci {
                snapshots[uid] = focus.Snapshot()
        }
        return snapshots
}

// Route sends a Fact to the appropriate Focus based on the Fact's UID.
// Returns false if no Focus exists for the UID or the inbox is full.
func (r *FocusRegistry) Route(fact *types.Fact) bool {
        uid := factUID(fact)
        if uid == "" {
                return false
        }

        r.mu.RLock()
        focus, ok := r.foci[uid]
        r.mu.RUnlock()

        if !ok {
                return false
        }

        return focus.Send(fact)
}

// RouteAll broadcasts a Fact to all Foci.
// Used for global events like node shutdown.
func (r *FocusRegistry) RouteAll(fact *types.Fact) {
        r.mu.RLock()
        defer r.mu.RUnlock()

        for _, focus := range r.foci {
                focus.Send(fact)
        }
}

// Count returns the number of active Foci.
func (r *FocusRegistry) Count() int {
        r.mu.RLock()
        defer r.mu.RUnlock()
        return len(r.foci)
}

// factUID extracts the UID from a Fact.
// All fact types carry the UID directly; for UnitFact, the caller
// (D-Bus event handler) is responsible for parsing the UID from the
// unit name and populating the UID field before sending to Syzygy.
func factUID(fact *types.Fact) string {
        switch f := (*fact).(type) {
        case *types.UnitFact:
                return f.UID
        case *types.ContainerFact:
                return f.UID
        case *types.PodStatusFact:
                return f.UID
        case *types.ProbeFact:
                return f.UID
        case *types.SpecFact:
                return f.UID
        default:
                return ""
        }
}

// ─── Status Writer Adapter ─────────────────────────────────────────────

// statusAdapter adapts a send function to the foci.StatusWriter interface.
// This decouples Focus from the horizon package.
type statusAdapter struct {
        send func(foci.StatusIntent)
}

func (a *statusAdapter) WriteStatus(intent foci.StatusIntent) {
        if a.send != nil {
                a.send(intent)
        }
}
