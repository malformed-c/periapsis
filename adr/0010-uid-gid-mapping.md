# ADR-0010: UID/GID Mapping and Isolation

**Status:** Proposed  
**Date:** 2026-04-12  

---

## Summary

Implement user namespace isolation for nspawn-based containers and sandbox hardening for chroot-based host-PID workloads to enforce Kubernetes RunAsUser/RunAsGroup semantics. The nspawn path gains real userns isolation via `--private-users`; the chroot path relies on capability dropping since user namespaces are unavailable with `RootDirectory=` transient units.

---

## Context

Periapsis runs containers via two runtime paths:

1. **nspawn path** (`internal/runtime/systemd/runtime.go` → `RunMachine`): Full container via `systemd-nspawn`
2. **chroot path** (`internal/runtime/systemd/program.go` → `runProgram`): Transient systemd service with `RootDirectory=` for host-PID workloads (CNI agents, kubelet plugins)

Both paths currently pass through RunAsUser/RunAsGroup as host-UID properties (`User=`/`Group=` systemd unit directives) but lack namespace isolation or sandbox controls. The nspawn path has no user namespace isolation; the chroot path cannot use user namespaces at all.

Verified constraints:
- systemd-nspawn `--private-users=UIDBASE:NUIDS` requires UIDBASE to be a multiple of 65536 and NUIDS to be exactly 65536 (common allocation: `65536 * 2` for first workload, `65536 * 3` for second, etc.)
- `--private-users-ownership=chown` remaps the rootfs on first use (expensive but one-time per rootfs)
- `--user=UID` inside nspawn requires that UID to exist in `/etc/passwd`; recommend pre-injecting entries
- `RootDirectory=` transient units do not support user namespaces
- `-U` is shorthand for `--private-users=pick --private-users-ownership=auto`

---

## Decision

### Path 1: nspawn (Full Containers)

Apply real user namespace isolation via `--private-users=UIDBASE:65536` where:
- UIDBASE = `65536 * (2 + hash(podUID) % 256)` — deterministic per-pod allocation, supporting ~256 concurrent pods per node before collision risk
- Combined with `--user=RUN_AS_UID` to map the entrypoint process to the requested UID inside the namespace
- `--private-users-ownership=chown` on container creation to remap rootfs
- Pre-inject `/etc/passwd` and `/etc/group` entries for the target UID/GID before container start (pattern: username `peri-<uid>`, create home directory)

### Path 2: chroot (Host-PID Workloads)

No user namespace available via `RootDirectory=`. Instead:
- Keep `User=`/`Group=` systemd unit properties for RunAsUser/RunAsGroup semantic correctness (k8s honors runAsUser even on privileged pods)
- Add conditional sandbox hardening for non-privileged workloads: `CapabilityBoundingSet=`, `NoNewPrivileges=yes`, `ProtectKernelTunables=yes`, `ProtectKernelModules=yes`, `PrivateDevices=yes`, `LockPersonality=yes`
- Skip hardening if `privileged: true` or `hostNetwork: true`

### Both Paths

- Inject `/etc/passwd` and `/etc/group` entries (username `peri-<uid>`) before container start
- Create home directory for the target user if running as non-root
- Honor pod-level `RunAsUser`/`RunAsGroup` with container-level overrides (standard k8s inheritance)
- No FSGroup support initially; defer to storage CSI drivers

---

## Implementation Plan

### Phase 1: Already Landed

PodConfig (`internal/runtime/interface.go`) already carries `RunAsUser *int64` and `RunAsGroup *int64`. SecurityContext extraction with standard k8s inheritance (container overrides pod) is wired in `node/gambit.go`. Both runtime paths pass them through as host-UID today.

### Phase 2: nspawn Modifications

Modify `internal/runtime/systemd/runtime.go` → `RunMachine`:
- Compute UIDBASE from pod UID hash if RunAsUID specified
- Append `--private-users=UIDBASE:65536` and `--private-users-ownership=chown`
- Pass `--user=RunAsUID` to execute entrypoint in the mapped namespace
- Pre-populate `/etc/passwd` entries in rootfs before exec

### Phase 3: chroot Modifications

Modify `internal/runtime/systemd/program.go` → `runProgram`:
- `User=`/`Group=` already plumbed; no change needed
- Conditionally append sandbox properties for non-privileged workloads
- Pre-inject `/etc/passwd` entry in rootfs

### Phase 4: Testing

- Verify nspawn: pod.spec.securityContext.runAsUser propagates correctly; `--private-users` flag appears in journal; `/proc/self/uid_map` shows the mapping inside container
- Verify chroot: User=/Group= properties set correctly; `NoNewPrivileges=` and `CapabilityBoundingSet=` appear in systemd unit
- Verify inheritance: container-level overrides pod-level; both default to 0 if unspecified

---

## Trade-offs

1. **nspawn isolation via hashing**  
   UIDBASE allocation uses deterministic hash of podUID, not cluster-wide coordination. Collision risk accepted for single-node deployments and typical pod counts (256 buckets). Multi-pod collisions map to same UIDBASE, sharing the namespace range but remaining isolated via the container boundary.

2. **chroot isolation via caps + mount ns**  
   Chroot path relies on capability bounding and mount namespace, not user namespace. This is weaker than nspawn isolation but appropriate for host-PID workloads that must share host kernel/pid namespace anyway.

3. **One-time rootfs remap cost**  
   `--private-users-ownership=chown` runs on container creation, recursively chowning the rootfs. For large rootfs images this is expensive. Accepted because it runs once per unique rootfs and isolates subsequent executions. Alternative (defer to image builder) rejected because Periapsis manages ad-hoc rootfs layouts via overlayfs.

4. **No FSGroup support**  
   FSGroup handling requires coordination between storage backends and the runtime. Deferred to Phase 2; storage CSI drivers should handle group ownership on mount.

---

## Security Implications

**nspawn userns:**
- Genuine isolation: container root (UID 0) inside namespace maps to host UIDBASE, preventing privilege escalation to true root even if nspawn escaped
- UID collision on host unlikely within single-node pod limits; acceptable for target deployments
- Isolation boundary remains nspawn itself (namespace and seccomp/mount isolation)

**chroot sandbox:**
- No userns: processes run as actual host UID (via systemd User= property). Escape from chroot means immediate access at that UID level on host.
- Mitigated by: host-PID workloads are trusted (CNI agents, kubelet plugins) and run in privileged sandbox anyway
- Capability bounding prevents privilege escalation from non-privileged workloads

**Both paths:**
- Pre-injected passwd entries are read-only and match target UID/GID; no trust boundary issue
- Home directory creation is best-effort; failures logged but non-fatal

---

## References

- [systemd-nspawn(1) man page](https://www.freedesktop.org/software/systemd/man/systemd-nspawn.html) — `--private-users`, `--user`, `--private-users-ownership` flags
- [Kubernetes SecurityContext documentation](https://kubernetes.io/docs/tasks/configure-pod-container/security-context/) — runAsUser, runAsGroup inheritance semantics
- [ADR-0002](./0002-perigeos-architecture.md) — nspawn vs. chroot path design
