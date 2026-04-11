# ADR-0010: UID/GID Mapping from Pod SecurityContext

**Status:** Proposed  
**Date:** 2026-01-23  
**Authors:** Qwen  

---

## Summary

Enable non-root container execution in systemd-nspawn by implementing Kubernetes `PodSecurityContext` and `ContainerSecurityContext` UID/GID mapping via nspawn's identity mapping facilities. This addresses the TODO item "UID/GID mapping from manifest" that blocks pod admission for workloads requiring explicit user/group identities (e.g., database servers, security-isolated services).

---

## Context

### Current State Analysis

Periapsis currently handles **only one** `SecurityContext` field:
```go
// node/lifecycle.go:514-517
func isPrivileged(c *corev1.Container) bool {
    return c.SecurityContext != nil &&
        c.SecurityContext.Privileged != nil &&
        *c.SecurityContext.Privileged
}
```

Missing implementations:
| Security Context Field | Status | Impact |
|------------------------|--------|--------|
| `RunAsUser` (container-level) | ❌ Not handled | Cannot run containers as non-root UID |
| `RunAsGroup` (pod/level)      | ❌ Not handled | Cannot specify primary GID for volumes/processes |
| `FSGroup` (pod-level)         | ❌ Not handled | Volume ownership not enforced via gids |

### Kubernetes SecurityContext Hierarchy

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: example-pod
spec:
  securityContext:           # ← Pod-level defaults applied to all containers
    runAsUser: 1000          # Container inherits this unless overridden
    runAsGroup: 3000         # All containers share this GID
  
  containers:
  - name: app-container
    image: nginx
    securityContext:        # ← Container-level overrides pod defaults
      runAsUser: 2000       # This container uses UID 2000 instead of 1000
    
  initContainers:           # Also inherit from pod, can override per-init-contain
```

### systemd-nspawn Capabilities

systemd-nspawn (v239+) provides identity mapping via CLI flags:

**`--uidmap=`**: Maps guest UIDs to host UIDs in ranges  
Format: `<guest-start>:<host-start>:<length>`  

Example mappings:
- `--uidmap=0:1000:65536` → Map all 65536 container UIDs starting at 0 to host UIDs starting at 1000
- `--uidmap=0:1000:1`     → Map only root (UID 0) in guest to UID 1000 on host

**`--gidmap=`**: Same syntax for GIDs  

**Alternative: userns mode**  
```bash
systemd-nspawn --userns=keep-id ...   # Preserves caller's uid/gid identity
# OR (requires root):
systemd-nspawn --userns=userns ...    # Full unprivileged container namespace
```

---

## Options Considered

### Option 1: Direct UID/GID Mapping via `--uidmap`/`--gidmap` ✅ **RECOMMENDED**

Pass mapping flags directly to nspawn command line based on SecurityContext values.

| Pros | Cons |
|------|------|
| Simple, explicit implementation | Only supports single-user containers (no process-specific UID switching inside container) |
| Works with all systemd versions ≥ v239 | Requires careful validation that mappings don't conflict with host UIDs/GIDs |
| No extra processes needed | FSGroup requires additional volume ownership handling post-startup |

**Implementation approach:**
```go
// runtime/systemd/runtime.go - RunMachine() function, after execStart is built

if cfg.ContainerUID != nil {
    // Map container root (uid 0) to configured UID on host
    execStart = append(execStart, 
        fmt.Sprintf("--uidmap=0:%d:1", *cfg.ContainerUID))
}

if cfg.ContainerGID != nil {
    // Map container root group (gid 0) to configured GID on host  
    execStart = append(execStart,
        fmt.Sprintf("--gidmap=0:%d:1", *cfg.ContainerGID))
}
```

### Option 2: Full User Namespace (`--userns=userns`) ❌ NOT RECOMMENDED

Create a full container user namespace inside nspawn with custom mappings.

| Pros | Cons |
|------|------|
| Complete isolation from host users | Requires root privileges for setup |
| Can support multiple UIDs/GIDs per process | Complex, fragile implementation |
| Matches Kubernetes behavior more closely | systemd-machined doesn't properly manage unprivileged containers on all distros |

**Why not this option:** The added complexity is unnecessary. Periapsis' design philosophy favors simplicity over feature completeness (see ADR-0001). Most workloads only need root mapped to a non-root UID, which Option 1 handles cleanly. User namespace mode can be added later if needed via `--userns` flag extension.

### Option 3: Pre-create Container Rootfs with Fixed Ownership ❌ NOT RECOMMENDED

Set ownership of merged overlayfs root before starting nspawn using chown/chmod.

| Pros | Cons |
|------|------|------|
| Doesn't require systemd-nspawn UID maps | Does NOT change process credentials (runAsUser not effective) |
| Simple filesystem-level fix | Only solves FSGroup, not RunAsUser |

**Why not this option:** Changing rootfs ownership doesn't affect the running container's user identity. A process inside a nspawn with `--uidmap=0:1000:1` that executes as UID 0 in guest still runs as host UID 1000 regardless of local file permissions. This only addresses FSGroup volume mount permissions, not RunAsUser/RunAsGroup at all.

---

## Decision

### Selected Approach: Option 1 (Direct UID/GID Mapping)

Periapsis will implement UID/GID mapping using systemd-nspawn's `--uidmap` and `--gidmap` flags, with additional volume ownership handling for FSGroup via bind mount post-processing.

**Rationale:**
- Matches the minimal-feature philosophy of Periapsis (see ADR-0001)
- No need to run machined as root or manage complex namespace configurations  
- Works across systemd versions that periapsis targets (v248+)
- Aligns with how kubelet handles this in containerd/shim implementations

---

## Implementation Plan

### Phase 1: Extend PodConfig Interface (`internal/runtime/interface.go`)

Add security context fields to `runtime.PodConfig`:

```go
type PodConfig struct {
    // ... existing fields (Name, Namespace, UID, etc.) ...
    
    // Container runtime configuration
    Container *corev1.Container
    
    // Security Context Fields (NEW)
    ContainerUID   *int64  // From container runAsUser or pod-level default  
    ContainerGID   *int64  // From container runAsGroup or pod-level default
    
    // FSGroup for volume ownership enforcement via bind mounts
    FSGroup        *int64  // Applied to host-side bind mount paths during startup
    
    TerminationGracePeriodSeconds int64
}
```

### Phase 2: Extract Security Context (`node/lifecycle.go`)

Modify `launchContainer()` around line ~150 where PodConfig is constructed. Add extraction logic before cfg creation:

```go
// node/lifecycle.go - launchContainer() function

func (g *Gambit) launchContainer(ctx context.Context, pod *corev1.Pod, c *corev1.Container, uid string, ...) {
    // ... existing code for rootfs, volumes, env vars ...

    // NEW: Extract security context fields with inheritance logic
    containerUID := extractRunAsUser(pod, c)  // Container overrides Pod default
    containerGID := extractRunAsGroup(pod, c) // Same inheritance pattern
    
    cfg := perigeos.PodConfig{
        Name:                      pod.Name,
        Namespace:                 pod.Namespace,
        UID:                       uid,
        ContainerName:             c.Name,
        Container:                 c,
        PawnName:                  g.Config.Name,
        RootFS:                    rootfs,
        BindMounts:                bindMounts,  // Volume mounts will be modified for FSGroup if needed
        NetNSPath:                 netPath,
        HostNetwork:               pod.Spec.HostNetwork,
        HostPID:                   pod.Spec.HostPID,
        Privileged:                isPrivileged(c),
        Environment:               resolvedEnv,
        PodIP:                     podIP,
        
        // NEW SECURITY FIELDS
        ContainerUID: containerUID,
        ContainerGID: containerGID,
        FSGroup:      extractFSGroup(pod),  // Always pod-level; applies to all volumes
        
        MemoryLimitBytes:              memLimit,
        CPULimitMillis:                cpuLimit,
        ImageEntrypoint:               ep,
        ImageCmd:                      cmd,
        TerminationGracePeriodSeconds: podTerminationGracePeriod(pod),
    }

    // NEW: Apply FSGroup to bind mounts if specified and not privileged container
    if cfg.FSGroup != nil && !cfg.Privileged {
        for i := range cfg.BindMounts {
            bm := &cfg.BindMounts[i]
            applyFSGroupOwnership(bm.HostPath, *cfg.FSGroup)
        }
    }

    // ... rest of function: RunMachine call and post-start logic ...
}

// Helper functions to extract SecurityContext fields with inheritance:

func extractRunAsUser(pod *corev1.Pod, c *corev1.Container) *int64 {
    // Container-level runAsUser takes precedence; fall back to pod default
    
    if c.SecurityContext != nil && c.SecurityContext.RunAsUser != nil {
        return c.SecurityContext.RunAsUser  // Return pointer as-is (container override)
    }
    
    if pod.Spec.SecurityContext != nil && pod.Spec.SecurityContext.RunAsUser != nil {
        return pod.Spec.SecurityContext.RunAsUser  // Return pointer for inheritance
    }
    
    return nil  // No runAsUser specified; nspawn will use UID 0 (root) by default
}

func extractRunAsGroup(pod *corev1.Pod, c *corev1.Container) *int64 {
    if c.SecurityContext != nil && c.SecurityContext.RunAsGroup != nil {
        return c.SecurityContext.RunAsGroup
    }
    
    if pod.Spec.SecurityContext != nil && pod.Spec.SecurityContext.RunAsGroup != nil {
        return pod.Spec.SecurityContext.RunAsGroup
    }
    
    // If neither is set but RunAsUser is, use GID=UID as a reasonable default
    // This matches Kubernetes behavior when only runAsUser is specified.
    if (c.SecurityContext == nil || c.SecurityContext.RunAsUser == nil) &&
       pod.Spec.SecurityContext != nil && 
       pod.Spec.SecurityContext.RunAsGroup == nil &&
       pod.Spec.SecurityContext.RunAsUser != nil {
        user := *pod.Spec.SecurityContext.RunAsUser
        return &user  // Use same UID as GID as fallback
    }
    
    return nil  // No runAsGroup specified; nspawn will use GID 0 (root) by default
}

func extractFSGroup(pod *corev1.Pod) *int64 {
    if pod.Spec.SecurityContext != nil && pod.Spec.SecurityContext.FSGroup != nil {
        return pod.Spec.SecurityContext.FSGroup
    }
    return nil  // FSGroup is always pod-level; container doesn't override it
}

// Apply ownership changes to a single bind mount path (recursive if directory)
func applyFSGroupOwnership(path string, fsGroup int64) {
    info, err := os.Stat(path)
    if err != nil {
        // Path may not exist yet (ephemeral volume); skip silently. 
        // Ownership will be set when the path is created by the CSI driver or kubelet.
        return
    }
    
    chownArgs := []string{"-R", fmt.Sprintf("%d:%d", info.Sys().(*syscall.Stat_t).Uid, fsGroup), path}
    if err := exec.Command("chgrp", "-R", fmt.Sprint(fsGroup), path).Run(); err != nil {
        // If the target GID doesn't exist on host (common with container images that have
        // custom UIDs/GIDs but not matching host IDs), this fails. Log and continue - 
        // FSGroup enforcement is best-effort for periapsis' use cases.
        logrus.Warnf("FSGroup chgrp failed for %s: %v; continuing without group ownership", path, err)
    }
    
    if info.IsDir() {
        os.Chmod(path, 0750) // Set rwxr-x--- permissions matching FSGroup access pattern
    }
}
```

### Phase 3: Apply UID/GID Mapping in Runtime (`internal/runtime/systemd/runtime.go`)

Modify `RunMachine()` around line ~160-250 after the execStart array is constructed but before it's executed. Add mapping flags right after Privileged capability check and environment variables:

```go
// runtime/systemd/runtime.go - RunMachine() function, insert after env vars (approx line 193)

    // Pass resolved env vars into the container via --setenv
    for _, envVar := range cfg.Environment {
        execStart = append(execStart, "--setenv="+envVar)
    }
    execStart = append(execStart,
        "--setenv=PERIGEOS_PAWN="+s.pawnName,
        "--setenv=PERIGEOS_UID="+podUID,
    )

    // NEW: Apply UID/GID identity mapping if specified in SecurityContext
    if cfg.ContainerUID != nil {
        execStart = append(execStart, 
            fmt.Sprintf("--uidmap=0:%d:1", *cfg.ContainerUID))  // Map guest root (0) → host uid
    }

    if cfg.ContainerGID != nil && !cfg.HostPID {  // HostPID containers skip UID/GID mapping entirely
        execStart = append(execStart,
            fmt.Sprintf("--gidmap=0:%d:1", *cfg.ContainerGID))  // Map guest root gid (0) → host gid
    }

    // Bind mounts section follows...
```

### Phase 4: Integration Testing

Add integration tests covering:
- **Basic non-root container**: `runAsUser: 1000`, verify process runs as UID 1000 in guest namespace (visible inside container via `/proc/self/status`)
- **Multi-container pod with mixed users**: Container A uses uid=2000, Container B inherits default from pod (uid=1000), verify each runs under correct UID  
- **FSGroup volume ownership**: Pod specifies FSGroup=3000, volume mount should have group 3000 on host side; inside container with matching GID mapping, files appear owned by that GID
- **Privileged override**: When `privileged: true`, ignore RunAsUser/RunAsGroup (matches Kubernetes behavior where privileged containers always run as root)

---

## Security Implications

### UID/GID Mapping Risks

1. **UID collisions on host**  
   If multiple pods specify the same non-root UID and they're running concurrently, file access conflicts can occur if both mount overlapping volumes to shared paths (e.g., `/var/cache`). Solution: Ensure each pod's namespace has unique UIDs or use isolated volume mounts. Periapsis doesn't implement this yet but it's not needed for single-node deployments where pods don't share hostside directories except via CSI drivers (which isolate per-volume).

2. **Root mapping exposure**  
   Mapping container root (`--uidmap=0:1000:1`) means the process appears as UID 1000 on host, but inside the guest it's still "root". If an attacker escapes nspawn isolation, they get elevated privileges (UID 1000 instead of regular user). This is no worse than running containers as root; it's actually better because even if escape occurs, the process lacks true root capabilities. The key defense remains: **networking isolation** via CNI prevents lateral movement to other pods/hosts.

3. **FSGroup without matching host GID**  
   If FSGroup=5000 but no group with GID 5000 exists on host, `chgrp` fails silently (best-effort behavior). Files remain owned by whatever UID created them via CSI driver or mount. This is acceptable because:
   - Most container images don't rely on specific internal file permissions for correctness  
   - Container image authors can embed proper permission handling if needed
   - Periapsis' target workloads are typically stateless web services, databases with isolated volumes, etc., where filesystem ownership isn't critical

4. **HostPID containers bypass UID/GID mapping**  
   The implementation explicitly skips identity mapping for `hostPID: true` pods because running in the host PID namespace inherently requires elevated privileges. This matches Kubernetes behavior but means such pods still run as root on both container and host side (no security benefit from RunAsUser).

---

## Trade-offs Acknowledged

1. **No process-level UID switching inside containers**  
   The `--uidmap=0:1000:1` mapping only affects the PID 1 process (the nspawn supervisor's entrypoint). Inside a multi-threaded or fork-heavy application, all threads continue to have effective UID = real UID regardless of mapped identity. This is sufficient for most workloads but means applications that dynamically switch UIDs via `setuid()` won't see the change reflected on host-side observability (journal logs show UID 1000 always).

2. **FSGroup best-effort vs. hard enforcement**  
   Periapsis applies FSGroup post-bind-mount as a convenience feature rather than through CSI driver hooks or kubelet's volume plugin infrastructure. This works for local-path-provisioner volumes and simple ConfigMap/Secret mounts but may not enforce correctly on all storage backends (e.g., NFS where permissions are controlled server-side). The "best-effort" approach balances simplicity against universal correctness; teams with strict compliance requirements should use privileged containers or hostPath volumes instead.

3. **Limited to systemd-nspawn identity features**  
   This implementation doesn't support advanced container security patterns like:
   - `allowPrivilegeEscalation: false` (requires seccomp/profile hooks not present in periapsis)
   - Drop-all-capabilities + selective re-adds (beyond the simple "--capability=all" for privileged containers)
   - SELinux/AppArmor contexts via nspawn's `-c` or `--selinux-context=` flags

These features could be added later as they map cleanly to systemd-nspawn CLI options, but aren't needed for basic non-root execution.

---

## Appendix: Example Usage

### Pod manifest with UID/GID mapping
```yaml
apiVersion: v1
kind: Pod
metadata:
  name: postgres-pod
spec:
  # Run all containers as uid=999 (postgres user), gid=998 (postgres group)  
  securityContext:
    runAsUser: 999
    runAsGroup: 998
    
  volumes:
  - name: postgres-data
    persistentVolumeClaim:
      claimName: pgdata-pvc
      
  containers:
  - name: postgres
    image: postgres:14-alpine
    # Container doesn't override pod-level security context; inherits runAsUser/runAsGroup
    
    volumeMounts:
    - name: postgres-data
      mountPath: /var/lib/postgresql/data
    
---

# Pod with container-level user override
apiVersion: v1
kind: Pod
metadata:
  name: nginx-sidecar-pod
spec:
  securityContext:
    runAsUser: 1000       # Default for all containers in pod
    
  containers:
  - name: web-nginx  
    image: nginx:alpine
    # Uses default uid=1000 from pod spec
  
  - name: init-db
    image: alpine:latest
    securityContext:     # Override with specific UID for init container
      runAsUser: 2000
    
    command: ["sh", "-c", "echo 'db initialized' > /shared/config"]
    
    volumeMounts:
    - name: shared-config
      mountPath: /shared
      
---

# Pod using FSGroup to enforce group ownership on volumes  
apiVersion: v1
kind: Pod
metadata:
  name: app-with-volume-pod
spec:
  securityContext:
    runAsUser: 3000       # Container runs as UID 3000
    fsGroup: 4000         # All volume mounts have group=4000 on host side
    
  containers:
  - name: app
    image: myapp:v1.2.3
    
    volumeMounts:
    - name: cache-dir
      mountPath: /cache
      
volumes:
- name: cache-dir
  emptyDir: {}          # FSGroup will apply to this ephemeral dir during startup

# Result inside container:
#   - /etc/passwd shows uid=3000 (actual running user)
#   - ls -la /cache shows group ownership = gname(4000)  
#   - Process audit logs show effective UID 3000, GID from fsGroup if mapped
```

---

## References

- [Kubernetes Pod SecurityContext documentation](https://kubernetes.io/docs/tasks/configure-pod-container/security-context/)
- [systemd-nspawn identity mapping flags](https://www.freedesktop.org/software/systemd/man/systemd-nspawn.html)  
- [Virtual-kubelet upstream security context handling (for comparison)](https://github.com/virtual-kubelet/virtual-kubelet/blob/master/node/pod.go#L362-L450) - VK passes `SecurityContext` through to providers but doesn't implement UID/GID mapping itself; this is where periapsis adds value as a more complete implementation.
