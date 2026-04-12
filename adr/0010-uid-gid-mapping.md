# ADR-0010: UID/GID Mapping and User Namespace Isolation

**Status:** Accepted  
**Date:** 2026-04-12  

---

## Summary

Implement user namespace isolation for nspawn-based containers using a post-netns
userns shim, and sandbox hardening for chroot-based host-PID workloads. The nspawn
path gains real userns isolation via a static C shim that calls `unshare(CLONE_NEWUSER)`
**inside** the container (after nspawn has joined the CNI network namespace), with
perigeos writing `uid_map`/`gid_map` from the host side. The chroot path relies on
capability dropping since user namespaces are unavailable with `RootDirectory=`
transient units.

---

## Context

Periapsis runs containers via two runtime paths:

1. **nspawn path** (`internal/runtime/systemd/runtime.go` → `RunMachine`): Full container via `systemd-nspawn`
2. **chroot path** (`internal/runtime/systemd/program.go` → `runProgram`): Transient systemd service with `RootDirectory=` for host-PID workloads (CNI agents, kubelet plugins)

### The userns + netns ordering problem

nspawn's `--private-users` calls `unshare(CLONE_NEWUSER)` **before** `setns(CLONE_NEWNET)`.
The userns child process lacks `CAP_SYS_ADMIN` in the **init** user namespace, so the
subsequent `setns()` into an external (CNI-allocated) network namespace fails with
`EPERM`. This is a fundamental incompatibility — all perigeos pods require an external
netns, making `--private-users` unusable.

### Why not use Go for the shim?

Go's `unshare(CLONE_NEWUSER)` only affects the calling OS thread. Other goroutine
threads remain in the old user namespace, making the process state inconsistent and
unsafe. A static C binary avoids this entirely.

---

## Decision

### Path 1: nspawn (Full Containers) — userns-shim

Create the user namespace **after** nspawn has joined the CNI netns, using a static
C shim (`cmd/userns-shim/main.c`) as the container entrypoint:

#### Protocol

```
nspawn starts → shim is PID 1 (already in CNI netns)
  ↓
shim calls unshare(CLONE_NEWUSER)
  ↓
shim writes "1\n" to /run/userns/ready  (FIFO → host)
  ↓
perigeos reads ready FIFO, finds shim's host PID via machined
  ↓
perigeos writes /proc/<pid>/uid_map and /proc/<pid>/gid_map
  ↓
perigeos writes "<uid>:<gid>\n" to /run/userns/gate  (FIFO → shim)
  ↓
shim parses target, calls setgroups(0,NULL) → setgid() → setuid()
  ↓
shim exec()s the real workload — no lingering wrapper process
```

#### UID mapping

- `UIDBASE = 65536 * (2 + sha256(podUID) % 256)` — deterministic per-pod
- Slot 0 reserved for host, slot 1 for nspawn's own use
- Map line: `0 <UIDBASE> 65536` — inside UIDs 0-65535 map to host UIDBASE..UIDBASE+65535
- 256 buckets; collision risk acceptable for single-node deployments

#### Activation conditions

userns-shim is used when **all** of:
- `RunAsUser` is set in the pod/container security context
- The pod is **not** privileged (privileged pods need host-level capabilities)
- The `userns-shim` binary is installed at `/usr/local/lib/perigeos/userns-shim`

If any condition is false, falls back to nspawn's `--user=` (no userns isolation).

#### Bind mounts

- `--bind-ro=/usr/local/lib/perigeos/userns-shim:/usr/local/bin/userns-shim` — shim binary
- `--bind=/run/perigeos/userns/<podUID>-<container>:/run/userns` — FIFO directory

### Path 2: chroot (Host-PID Workloads)

No user namespace available via `RootDirectory=`. Instead:
- Keep `User=`/`Group=` systemd unit properties for RunAsUser/RunAsGroup
- Add conditional sandbox hardening for non-privileged workloads: `CapabilityBoundingSet=`, `NoNewPrivileges=yes`, `ProtectKernelTunables=yes`, `ProtectKernelModules=yes`, `PrivateDevices=yes`, `LockPersonality=yes`
- Skip hardening if `privileged: true` or `hostNetwork: true`

### Both Paths

- Inject `/etc/passwd` and `/etc/group` entries (username `peri-<uid>`) before container start
- Create home directory for the target user if running as non-root
- Honor pod-level `RunAsUser`/`RunAsGroup` with container-level overrides (standard k8s inheritance)
- No FSGroup support initially; defer to storage CSI drivers

---

## Implementation

### Files

| Path | Role |
|------|------|
| `cmd/userns-shim/main.c` | Static C shim binary — unshare, FIFO handshake, setuid, exec |
| `cmd/userns-shim/Makefile` | Cross-compile amd64/arm64, strip + UPX compress |
| `internal/runtime/systemd/userns.go` | UID mapping, passwd/group injection, FIFO setup, host-side handshake |
| `internal/runtime/systemd/runtime.go` | RunMachine wiring — detect userns, bind mounts, prepend shim, launch goroutine |
| `internal/runtime/systemd/musl.go` | getent initgroups shim for musl/Alpine (fallback path) |
| `internal/runtime/systemd/program.go` | chroot sandbox hardening |

### Host-side functions (`userns.go`)

- `computeUIDBASE(podUID)` — deterministic UIDBASE from pod UID hash
- `usernsShimExists()` — checks shim binary at install path
- `setupUserNSFIFOs(podUID, containerName)` — creates per-container FIFO dir with ready/gate
- `cleanupUserNSFIFOs(podUID, containerName)` — removes FIFO dir
- `completeUserNSSetup(fifoDir, machineName, podUID, targetUID, targetGID)` — goroutine: waits on ready FIFO → writes uid_map/gid_map via `/proc/<pid>/` → sends uid:gid via gate FIFO
- `prepareUserIdentity(rootfs, runAsUser, runAsGroup, logger)` — injects passwd/group entries, creates home dir

### RunMachine wiring (`runtime.go`)

1. **Detect**: `useUserNS := RunAsUser != nil && !Privileged && usernsShimExists()`
2. **Setup FIFOs**: `setupUserNSFIFOs()`, graceful fallback to `--user=` on error
3. **Bind mounts**: shim binary (ro) + FIFO dir (rw)
4. **Prepend shim**: `fullCmd = ["/usr/local/bin/userns-shim", ...originalCmd]`
5. **After StartTransientUnit**: `go completeUserNSSetup(...)`, cleanup FIFOs on completion
6. **Error path**: clean up FIFOs if StartTransientUnit fails

---

## Trade-offs

1. **Post-netns userns vs. `--private-users`**  
   `--private-users` is simpler but fundamentally incompatible with external network namespaces (EPERM on setns). The shim approach creates userns after netns, solving this at the cost of a FIFO handshake and a goroutine per container start.

2. **CAP_NET_ADMIN loss inside userns**  
   After `unshare(CLONE_NEWUSER)`, the container loses `CAP_NET_ADMIN` in the init userns. This is a feature — CNI has already configured networking before the container starts. Privileged pods (which may need network admin) skip userns entirely.

3. **No `--private-users-ownership=chown`**  
   The userns-shim approach doesn't use nspawn's rootfs remapping. Files in the rootfs (owned by host uid 0) appear as `nobody:nogroup` inside the userns. This is cosmetic — the workload runs as the mapped target UID. Can be addressed later with `chown` or idmap mounts if needed.

4. **Static C binary dependency**  
   The shim must be pre-installed on the host. If missing, falls back gracefully to `--user=` (no isolation). Cross-compiled for amd64/arm64 and UPX-compressed (~300KB).

5. **UIDBASE collision via hashing**  
   256 buckets from `sha256(podUID) % 256`. Collisions map pods to the same host UID range but they remain isolated via the container boundary (separate mount/pid/net namespaces).

6. **chroot path: no userns**  
   `RootDirectory=` transient units cannot use user namespaces. Mitigated by capability bounding — host-PID workloads are trusted (CNI agents, kubelet plugins) and typically run privileged anyway.

---

## Security Implications

**nspawn userns (shim path):**
- Genuine isolation: container root (UID 0) maps to host UIDBASE (unprivileged), preventing privilege escalation even on nspawn escape
- The shim exec()s away — no lingering wrapper process, no attack surface beyond the handshake window
- FIFO directory is per-container, mode 0700, cleaned up after handshake completes
- Target uid:gid sent via FIFO pipe (not env vars) — no leakage to child process environment

**nspawn fallback (--user= path):**
- No userns isolation — process runs as the requested UID but in the init user namespace
- Still benefits from nspawn's mount/pid/net namespace isolation
- getent shim for musl images handles `initgroups` database (see ADR-0010 musl appendix)

**chroot sandbox:**
- No userns: processes run as actual host UID. Escape from chroot means access at that UID level on host.
- Mitigated by: host-PID workloads are trusted; capability bounding prevents privilege escalation

**Both paths:**
- Pre-injected passwd entries are read-only and match target UID/GID; no trust boundary issue
- Home directory creation is best-effort; failures logged but non-fatal

---

## References

- [systemd-nspawn(1)](https://www.freedesktop.org/software/systemd/man/systemd-nspawn.html) — `--private-users`, `--user` flags
- [user_namespaces(7)](https://man7.org/linux/man-pages/man7/user_namespaces.7.html) — uid_map/gid_map semantics
- [Kubernetes SecurityContext](https://kubernetes.io/docs/tasks/configure-pod-container/security-context/) — runAsUser, runAsGroup inheritance
- [ADR-0010 Appendix: Musl Investigation](./0010-musl-investigation.md) — getent initgroups shim
