# Periapsis / Perigeos — Bug Tracker

Last updated: 2026-04-13

## Open

### Pod restart counts reset on perigeos restart
Restart counts are held in-memory. When perigeos restarts, all counts reset to zero. Would need persistence (file or annotation) to survive restarts.

### Lost k8s events on perigeos restart
Events are ephemeral in k8s — not persisted across perigeos restarts. Pods like nginx-engifire lose their event history. This is consistent with kubelet behavior but worth noting.

### SeaweedFS CSI sidecar crashes on engifire
`seaweedfs-csi-node` is 1/3 (CrashLoopBackOff) on engifire. The `driver-registrar` and `csi-liveness-probe` sidecars crash because they probe localhost ports across container boundaries — perigeos gives each container its own network namespace within the pod's netns, so localhost communication between containers fails. Also: emptyDir `/var/cache/seaweedfs` bind mount isn't working correctly for the plugin container.

### CSI mount namespace isolation (globalmount workaround active)
NodePublishVolume target paths under `/var/lib/apsis/perigeos/pawns/.../volumes/csi/...` are outside the CSI driver pod's Bidirectional mount at `/var/lib/kubelet/pods`. Bind mount silently fails. **Current workaround:** skip NodePublishVolume, use globalmount path directly (`/var/lib/kubelet/plugins/kubernetes.io/csi/<pvName>/globalmount`). Works on primary node (engix99), blocked on engifire by sidecar crash above.

### go-systemd StartTransientUnit race condition
Race in `startJob()` (dbus/methods.go:54): unit can start, run, and exit before the completion channel is registered, causing `<-ch` to block forever. Only manifests with `CollectMode=inactive-or-failed` + containers that exit in <1 second. **Workaround:** pass `nil` channel (fire-and-forget) and poll `MachineStatus` instead. Upstream issue not yet filed (coreos/go-systemd#485 placeholder).

## Resolved

### 2026-03-29
- **nginx:alpine ENXIO on `/dev/stderr`** — nspawn `--console=pipe` gave journal socket for fd 2. Fixed: allocate PTY for stdout/stderr, forward to journald via `journal.Send`.
- **"Unit already loaded" crash loop** — unit stayed in systemd table after failure. Fixed: always call `ResetFailedUnit` after `StopUnit`, pre-clear before `StartTransientUnit`.

### 2026-03-28
- **`reserved:init` endpoints** — label mismatch `perigeos.io/host` vs `periapsis.io/host`. Fixed: standardized on `periapsis.io/`.
- **Host-to-pod, NodePort, ClusterIP** — all working after label fix.

### 2026-03-22
- **Watchdog timeout** — `WatchdogSec` disabled until `sd_notify` implemented.
- **Log streaming for completed pods** — `GetContainerLogs` falls back to `completedPods` map.
- **CiliumNode race on agent restart** — `EnsureCiliumNode` at pawn startup + `/healthz` readiness probe.

### 2026-03-21
- **Overlay mount namespace isolation** — `ProtectHome=no`.
- **IPAM CIDR allocation** — `K8S_POD_NODE_NAME` in CNI args.
- **Overlay index=off** — kernel 6.19 upperdir protection.

### 2026-03-15
- **Pod state desync** — `labels.Everything()` selector, `CreatePod` semaphore, pre-registration as Pending.
- **Stale disk directories** — cleanup in `DeletePod` teardown.
- **Memory bloat** — no longer observed.
