package node

// BatchWatcher — fully event-based D-Bus signal adapter.
//
// Phase 4 of ADR-0011: the BatchWatcher is now a thin adapter whose only
// responsibility is to subscribe to systemd D-Bus PropertiesChanged signals,
// parse them into types.UnitFact values, and forward them to Syzygy.
//
// All state management (coalescing, restart policy, probes, terminal phase
// decisions) has moved to the pure foci.Reduce function, driven by Syzygy's
// single-threaded event loop. Poll-based reconciliation is handled by
// Syzygy's anti-entropy loop, which calls ListManagedMachines and emits
// ContainerStateFacts. Probes are owned by ProbeScheduler.
//
// What remains:
//   - Startup: cleanup stale units from a previous crash
//   - Runtime: D-Bus event subscription → UnitFact → SendFact
//   - Cleanup: stale unit removal on startup

import (
        "context"
        "log/slog"
        "strings"

        perigeos "github.com/malformed-c/periapsis/internal/runtime"
        "github.com/malformed-c/periapsis/internal/types"
)

// BatchWatcherDeps holds all external dependencies for the BatchWatcher.
type BatchWatcherDeps struct {
        Runtime    perigeos.Runtime
        Logger     *slog.Logger
        PawnName   string
        Store      *PodStore // only used for startup stale-unit cleanup

        // ParseUnitName extracts (uid, containerName) from a systemd unit name.
        ParseUnitName func(unitName string) (uid, containerName string)

        // SendFact forwards a Fact to the Syzygy event loop.
        // Required — the event-based BW cannot function without Syzygy.
        SendFact func(fact types.Fact) bool
}

// BatchWatcher subscribes to D-Bus unit state signals and forwards them
// as UnitFacts to the Syzygy event loop. It holds no pod state, runs no
// probes, performs no restart logic, and never pushes status directly.
type BatchWatcher struct {
        deps   BatchWatcherDeps
        logger *slog.Logger
        cancel context.CancelFunc
        done   chan struct{}
}

// StartBatchWatcher creates and starts the event-based batch watcher.
func StartBatchWatcher(deps BatchWatcherDeps) *BatchWatcher {
        if deps.SendFact == nil {
                deps.Logger.Warn("BatchWatcher started without SendFact — events will be dropped")
                deps.SendFact = func(types.Fact) bool { return false }
        }

        ctx, cancel := context.WithCancel(context.Background())
        bw := &BatchWatcher{
                deps:   deps,
                logger: deps.Logger.With("component", "batchwatcher"),
                cancel: cancel,
                done:   make(chan struct{}),
        }

        go bw.run(ctx)
        return bw
}

// Stop cancels the batch watcher and waits for it to exit.
func (bw *BatchWatcher) Stop() {
        bw.cancel()
        <-bw.done
}

func (bw *BatchWatcher) run(ctx context.Context) {
        defer close(bw.done)

        // On startup, clean up stale units left by a previous crash/restart.
        bw.cleanupStaleUnits(ctx)

        // Subscribe to D-Bus unit state events.
        eventCh := bw.deps.Runtime.SubscribeEvents(ctx)

        for {
                select {
                case <-ctx.Done():
                        return

                case ev, ok := <-eventCh:
                        if !ok {
                                return
                        }
                        bw.handleUnitEvent(ev)
                }
        }
}

// handleUnitEvent reacts to a D-Bus unit state change by emitting a
// UnitFact to Syzygy. Syzygy's Reduce function handles all downstream
// logic: phase transitions, restart decisions, event recording, and
// status updates via the Effect pipeline.
func (bw *BatchWatcher) handleUnitEvent(ev perigeos.UnitEvent) {
        // Quick filter: does this unit belong to this pawn?
        if !strings.HasPrefix(ev.UnitName, "perigeos-"+bw.deps.PawnName+"-") {
                return
        }

        uid, containerName := bw.deps.ParseUnitName(ev.UnitName)
        if uid == "" {
                return
        }

        // Fetch exit code for "failed" substate so the reducer can decide
        // whether to restart or terminate.
        var exitCode int32
        if ev.SubState == "failed" {
                exitCode = bw.deps.Runtime.MachineExitCode(context.Background(), uid, containerName)
        }

        bw.logger.Debug("handleUnitEvent: emitting UnitFact",
                "uid", uid, "container", containerName,
                "unit", ev.UnitName, "subState", ev.SubState, "exitCode", exitCode)

        bw.deps.SendFact(types.NewUnitFact(uid, ev.UnitName, ev.SubState, exitCode))
}

// cleanupStaleUnits removes dead/failed systemd units from a previous
// perigeos lifetime that never got cleaned up (e.g. after a crash).
func (bw *BatchWatcher) cleanupStaleUnits(ctx context.Context) {
        if bw.deps.Store == nil {
                return
        }

        activeUIDs := bw.deps.Store.ActiveUIDs()
        cleaned, err := bw.deps.Runtime.CleanupStaleUnits(ctx, activeUIDs)
        if err != nil {
                bw.logger.Error("Startup stale unit cleanup failed", "err", err)
                return
        }
        if cleaned > 0 {
                bw.logger.Info("Cleaned up stale units from previous run", "count", cleaned)
        }
}
