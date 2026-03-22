# ADR-0007: Dedicated mount-thread for systemd-nspawn overlays

**Status:** Proposed
**Date:** 2026-03-21

## Context

Perigeos runs Kubernetes pods as systemd-nspawn containers, mounting overlayfs for container rootfs via `unix.Mount()` in Go. These overlay mounts must be visible to nspawn processes, which systemd starts via D-Bus `StartTransientUnit` in PID 1's mount namespace.

When the perigeos systemd unit runs with `ProtectHome=yes` (or other directives that create a private mount namespace), the perigeos service gets a private namespace separate from PID 1's. Any overlay mounts performed in that private namespace are invisible to transient nspawn units spawned by D-Bus in PID 1's mount namespace. This breaks pod startup.

**Current workaround:** Disable `ProtectHome=yes` in the systemd service file. This removes useful hardening and is not a sustainable solution as the service gains more filesystem operations.

## Decision

Implement a **dedicated mount-thread pattern**: lock a single OS thread to PID 1's mount namespace at startup, and route all mount/unmount operations through that thread via a Go channel.

### Implementation approach

1. At perigeos startup (before spawning worker goroutines), call `runtime.LockOSThread()` on the main thread, then `unix.Setns()` to enter PID 1's mount namespace.

2. Spawn a goroutine that remains bound to that OS thread. This goroutine reads mount/unmount requests from a buffered channel, executes them in PID 1's namespace, and sends responses back through a reply channel.

3. Whenever perigeos needs to mount or unmount an overlay, send a request to the mount-thread via the channel. Wait synchronously for the response.

4. All other perigeos goroutines run in the perigeos service's private namespace (or any namespace they inherit), unaffected by the mount-thread's namespace pinning.

### Why this approach

**Option 1: Shell out to `nsenter --mount=/proc/1/ns/mnt mount ...`**
- Simplest to implement, but forks a process per mount operation.
- High overhead for frequent container startups.
- Harder to pass structured errors back to Go.

**Option 2: Call `unix.Setns()` per mount request**
- Avoids forks, but fragile with Go's goroutine scheduler.
- The calling goroutine's thread affinity is not guaranteed across scheduler yield points.
- Switching namespaces on an arbitrary goroutine can unpredictably affect other goroutines on the same OS thread.
- Requires careful use of `runtime.LockOSThread()` per operation, which is error-prone.

**Option 3: Dedicated mount thread** ✓
- Single OS thread bound to PID 1's mount namespace at startup.
- All mount operations routed through that thread via channel — clean, thread-safe, no forks.
- Goroutine scheduler never moves that OS thread; namespace stays pinned.
- Mount operations become async (channel send + reply wait), but latency is negligible (< 1ms for typical overlays).

## Consequences

- Enables re-enabling `ProtectHome=yes` and other systemd hardening directives, improving service security posture.
- Adds approximately 50 lines of infrastructure code (mount channel, request/response types, worker loop).
- Mount operations become async in the perigeos codebase, but the caller always waits for the reply synchronously, so from the caller's perspective they appear synchronous.
- If the mount-thread goroutine panics, the entire perigeos process exits (like any uncaught panic), but normal errors are handled gracefully through the reply channel.
- Testing mount failures requires either mocking the channel or running integration tests with real mount operations (already required for `internal/runtime/systemd`).
- The dedicated thread will show up in debuggers and profiling tools; document its purpose in comments.
