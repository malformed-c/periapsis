# ADR-0009: Change KillMode from control-group to process

**Status:** Accepted
**Date:** 2026-03-30

## Context

Perigeos manages pods as systemd-nspawn containers. The perigeos systemd unit
currently uses `KillMode=control-group`, which kills all processes in the
service's cgroup when perigeos stops - including every nspawn container it
spawned.

This causes a cascade of problems on `systemctl restart perigeos`:

1. **All containers die.** Every running pod is killed and must be recreated
   from scratch (image re-extract, CNI ADD, nspawn start). On a host with
   dozens of pods this takes minutes.

2. **Constellation agent dies too.** The constellation-agent pod runs on the
   same host, managed by perigeos. When it's killed, the
   `/var/run/cilium/cilium.sock` socket disappears. Perigeos's CNI
   auto-detection (`buildCNIConfig`) checks for that socket at startup -
   if the socket is gone, it falls back to built-in veth networking. Pods
   then get podman bridge IPs (`10.88.0.x`) instead of Constellation-managed
   CIDRs, breaking cross-host connectivity entirely.

3. **BatchWatcher `seenRunning` state is lost.** The in-memory
   `seenRunning` map tracks which containers have been observed in Running
   state. After restart, containers killed by `control-group` appear as
   `StateExited` with `seenRunning=false`. The BatchWatcher defers the
   terminal decision ("container never seen running"), preventing restart
   logic from firing. Pods stay stuck in `ContainerCreating` / `Pending`.

4. **Pod Ready condition goes stale.** The initial status push after pod
   recreation hardcodes `Ready: false`. If the BatchWatcher doesn't detect
   a readiness transition (due to bug #3), the pod stays `Ready: False`
   indefinitely, preventing endpoint population and service connectivity.

Bugs #3 and #4 have been fixed (seeding `seenRunning` from hydrated pods,
using `isContainerReady()` in the initial status push). But the fundamental
issue - killing every container on a routine restart - remains costly and
fragile.

## Decision

Change the perigeos systemd unit to `KillMode=process`. Only the main
perigeos process is killed on stop; nspawn containers survive in their own
scopes/slices.

### Startup: rediscovery

`HydrateFromRuntime` already handles this path:

1. Reads `pod-state.json` from disk for each pod.
2. Calls `ListManagedMachines()` to discover running systemd units.
3. Re-populates `g.pods`, `g.podIPs`, `g.podPhases`.
4. `StartBatchWatcher` seeds `seenRunning` from hydrated `PodRunning` pods.
5. First poll detects containers as `StateRunning` and pushes correct status.

No container recreation needed - pods continue running uninterrupted.

### Shutdown: explicit cleanup

On `systemctl stop perigeos` (not restart), containers should be cleaned up.
Perigeos already handles SIGTERM gracefully via `DrainPods`, which:

1. Iterates all tracked pods.
2. Calls `StopMachine` for each container.
3. Tears down network namespaces.
4. Removes overlay mounts.

This runs within `TimeoutStopSec=90s`. If perigeos is killed before
completing drain (SIGKILL after timeout), orphan containers remain but are
harmless - they'll be cleaned up on next start by the stale-pod sweep in
`HydrateFromRuntime`.

### Service file changes

```ini
# Before
Type=simple
KillMode=control-group
TimeoutStopSec=90s

# After
Type=notify
NotifyAccess=main
WatchdogSec=120s
KillMode=process
TimeoutStopSec=30s
```

- `Type=notify`: systemd waits for `READY=1` before reporting active.
- `WatchdogSec=120s`: perigeos sends `WATCHDOG=1` pings every 30s;
  systemd auto-restarts if pings stop.
- `TimeoutStopSec=30s`: shorter - no drain needed, just close servers.

### Shutdown behavior

SIGTERM handler:
1. Sends `STOPPING=1` to systemd.
2. Marks all pawns as shutting down (node → NotReady).
3. Waits up to 15s for any in-progress apiserver-initiated deletions.
4. Closes control socket and HTTPS servers.
5. Exits - containers survive via `KillMode=process`.

No `DrainPods` on SIGTERM. For explicit node decommission, run
`perigeos drain` before stopping.

### Alternatives considered

**Option A: Keep `control-group`, fix all edge cases** (current state)
- Seeding `seenRunning` ✓ (done)
- Fixing `Ready: false` initial push ✓ (done)
- Explicit `[global.cni]` to bypass socket auto-detect ✓ (done)
- Still kills every container on restart - slow, wasteful.
- Every new edge case from container death requires another fix.

**Option B: `KillMode=process`** ✓
- Containers survive restarts - zero disruption.
- Hydration path already works.
- Eliminates the entire class of restart-related bugs.
- Graceful shutdown still cleans up via `DrainPods`.

**Option C: `KillMode=mixed`**
- Sends SIGTERM to main process, SIGKILL to remaining after timeout.
- Containers would get SIGKILL after `TimeoutStopSec` if drain doesn't
  finish. This is harsher than Option B where orphans are swept on next
  start.

## Consequences

- **Zero-downtime restarts**: `systemctl restart perigeos` no longer kills
  pods. Binary upgrades become non-disruptive.
- **Orphan risk on crash**: If perigeos crashes (SIGSEGV, OOM-kill) without
  running drain, containers remain. Mitigated by stale-pod sweep on next
  start and systemd's transient unit garbage collection.
- **Explicit stop still cleans up**: `systemctl stop perigeos` triggers
  `DrainPods` via SIGTERM, which stops all containers before exit.
- **Simpler mental model**: Perigeos owns container lifecycle explicitly
  via its own code, not implicitly via cgroup membership.
