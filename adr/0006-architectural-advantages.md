# ADR-0006: Improvements enabled by the systemd-nspawn / virtual-node architecture

**Status:** Accepted  
**Date:** 2026-03-20

## Context

Perigeos replaces the kubelet → CRI → containerd → runc stack with a direct systemd-nspawn model. This isn't just a different runtime — it removes architectural constraints that are baked into the standard Kubernetes data plane. Several long-standing Kubernetes pain points are fixable not because of cleverer code, but because the intermediary layers (CRI, containerd, shim) that prevent the fixes are absent.

This ADR catalogs improvements that the architecture uniquely enables. Items are grouped by priority: high-value differentiators first, then incremental wins. Each item notes what blocks it in standard Kubernetes and why perigeos can do it.

## High-value differentiators

### Lazy image pulling

**Problem in standard Kubernetes:** Containerd unpacks every OCI layer into a snapshotter, computes diffs, and does content-addressable verification per layer before the container can start. A 2GB image means 2GB of network transfer and disk writes before PID 1 runs, even if the workload only touches 50MB of the filesystem.

**Why perigeos can fix it:** ImageManager owns the entire image pipeline with no containerd in the middle. The overlayfs root can be backed by a FUSE filesystem (stargz, nydus, or zstd:chunked) that serves content lazily — the image manifest and TOC are fetched at pull time, but individual file blocks are demand-paged on first read. Pod startup becomes near-instant; actual binary pages fault in during execution.

**Implementation path:** Replace the eager unpack in ImageManager with a FUSE mount option. The eager path stays as a fallback for hosts without FUSE or for images without a compatible TOC. The `go-containerregistry` library already supports stargz index parsing.

### Event-driven cgroup metrics

**Problem in standard Kubernetes:** Kubelet runs cadvisor, which polls cgroup stats for every container on a fixed interval. At high pod counts this is measurably expensive — cadvisor reads `/sys/fs/cgroup/.../memory.current`, `cpu.stat`, etc. for every container every 10-15 seconds. The polling model means metrics are always stale by up to one interval, and the CPU cost scales linearly with pod count.

**Why perigeos can fix it:** Systemd already maintains resource accounting for every unit. The DBus properties `MemoryCurrent`, `CPUUsageNSecMonotonic`, `IOReadBytes`, `IOWriteBytes` are available without polling — they're kernel-maintained counters exposed through the cgroup filesystem that systemd wraps. Perigeos can query these on demand (for status reporting) or subscribe to `PropertiesChanged` signals on the systemd DBus interface for event-driven updates. Additionally, `inotify` on cgroup control files (`memory.events`, `memory.high`, `pids.events`) can trigger alerts on pressure thresholds without any polling.

**Implementation path:** Replace the current `pawstats` polling with DBus property reads on demand for `GetPodStatus`. Add `sd_bus_match_signal` subscriptions for memory/PID pressure events to drive node condition updates and eviction (ADR-0003 tier 2) reactively rather than on a timer.

### Real-time ConfigMap/Secret propagation

**Problem in standard Kubernetes:** Kubelet implements ConfigMap/Secret volumes as atomic symlink swaps with a polling interval (default 60 seconds, configurable down to 1 second at the cost of API server load). Updates are delayed by up to one interval. There is no delivery guarantee — the pod doesn't know when the swap happened and gets no inotify event from the symlink swap itself (only from the target directory change, which some runtimes handle inconsistently).

**Why perigeos can fix it:** Perigeos controls the overlayfs mount and the volume setup. ConfigMap volumes can be implemented as a FUSE filesystem that serves content directly from an API server watch stream. The flow becomes: ConfigMap update hits etcd → watch event fires → FUSE serves new content → inotify fires in the container on the actual file. Propagation latency drops from 60 seconds to watch latency (typically sub-second). Pods that `inotify` on their config files see changes immediately.

**Implementation path:** New volume driver in `internal/volume` that mounts a FUSE filesystem for ConfigMap/Secret volumes. The FUSE process watches the relevant object via the existing informer infrastructure. Standard overlayfs-backed volumes stay as the default; FUSE mode is opt-in via an annotation (`perigeos.io/volume-mode: watch`) or global config.

## Structural wins

### Reduced startup hop count

**Problem in standard Kubernetes:** Pod startup path: kubelet → CRI gRPC → containerd → containerd-shim-runc-v2 → runc → container PID 1. Four process hops and two IPC boundaries (CRI socket, shim ttrpc). Each hop adds latency and failure modes.

**Why perigeos can fix it:** Perigeos startup path: Gambit → DBus call → systemd-nspawn → container PID 1. Two hops, one IPC boundary (DBus). The systemd service manager is already running (PID 1), so there's no daemon to start or connect to. Combined with lazy image pulling, cold-start pod latency can be an order of magnitude lower than containerd.

**No additional implementation needed** — this is already how perigeos works. Worth measuring and publishing benchmarks against containerd cold-start times.

### Node overhead reduction

**Problem in standard Kubernetes:** A kubelet process is ~200MB RSS. It runs cadvisor, the CRI client, certificate management, device plugin manager, volume manager, and eviction manager as built-in components. At 30 nodes, that's 6GB of RAM just for kubelets.

**Why perigeos can fix it:** Perigeos runs 30 pawns from a single process. The overhead scales sub-linearly because informer caches, the HTTP server, the image cache, and the DBus connection are shared. Per-pawn overhead is a goroutine, a cgroup slice, and a map entry.

**No additional implementation needed** — this is already how perigeos works. Track RSS per pawn count for marketing purposes.

### Structured logging via journal

**Problem in standard Kubernetes:** `kubectl logs` reads from a log file that the container runtime writes. The path involves three symlinks: `/var/log/containers/<pod>.log` → `/var/log/pods/<pod>/<container>/0.log` → runtime log file. Log rotation is the runtime's responsibility (containerd has separate rotation config from kubelet). `--previous` depends on the runtime keeping the last rotated file. `--since-time` scans the file for timestamps because there's no time index.

**Why perigeos can fix it:** Container stdout/stderr goes to the systemd journal via `SyslogIdentifier`. The journal provides structured metadata (unit name, invocation ID), built-in size-based rotation, and cursor-based streaming with native time indexing. `--since-time` becomes a journal cursor seek instead of a file scan. `--previous` is an invocation ID lookup.

**Implementation path:** The journal integration already works. Two fixes are pending: `--previous` returning wrong invocation ID (known bug), and exposing journal cursors in `GetLogStream` for efficient `--since-time` seeks. Both are targeted fixes, not architectural changes.

## Incremental improvements

### Parallel init containers

**Problem in standard Kubernetes:** Init containers run sequentially, in order. Three independent init containers (e.g., schema migration, config generation, certificate fetch) must wait for each other serially. KEP-3085 (sidecar containers) only addresses long-running sidecars, not parallel init.

**Why perigeos can fix it:** Gambit controls init container execution directly. A pod annotation can declare dependencies between init containers, and Gambit runs independent ones concurrently.

**Implementation path:** Add support for `perigeos.io/init-parallel: "true"` (all init containers run concurrently) and `perigeos.io/init-depends-on: "container-name"` (run after a specific init container completes). The pod spec remains valid Kubernetes — the annotations are hints. Default behavior (sequential) is unchanged.

### Node-local scheduling fast path

**Problem in standard Kubernetes:** Every pod goes through the centralized scheduler, which evaluates every node. At 1000 nodes, that's 1000 scoring evaluations per pod. For unconstrained pods (no affinity, no topology spread, just "give me any node with room") this is wasted work.

**Why perigeos can fix it:** Since Apogeos controls pawn placement and perigeos knows its own resource availability, a pawn can detect unscheduled pods that fit its capacity and claim them directly by setting `spec.nodeName`. The centralized scheduler stays as the fallback for pods with constraints.

**Implementation path:** A watch on unscheduled pods (`.spec.nodeName == ""`) in Gambit or a separate controller. On seeing a pod with no affinity/anti-affinity/topology constraints, the pawn checks local capacity and atomically sets `spec.nodeName` via a status update. Race between multiple pawns is resolved by the API server's optimistic concurrency — first writer wins, others retry on conflict. This is opt-in via a node label or annotation.

**Risk:** This bypasses scheduler plugins, priority/preemption, and quota enforcement. Only suitable for workloads that explicitly opt in.

### UDP port-forward

**Problem in standard Kubernetes:** `kubectl port-forward` only supports TCP. This is baked into kubelet's SPDY/WebSocket upgrade path — the multiplexing protocol assumes reliable ordered streams.

**Why perigeos can fix it:** PawnServer handles the port-forward API endpoint directly. A parallel UDP path can use a different multiplexing strategy (datagram framing over WebSocket, or a separate UDP tunnel endpoint).

**Implementation path:** Extend `node/api` port-forward handler to detect a UDP request and use datagram framing. Requires a corresponding kubectl plugin or client-side proxy since upstream `kubectl port-forward` won't send UDP requests.

## Decision

Prioritize the three high-value differentiators (lazy image pulling, event-driven cgroup metrics, real-time ConfigMap propagation) as they represent the strongest technical differentiation against k3s and standard Kubernetes. These are things the containerd-based stack structurally cannot do.

Structural wins (startup latency, node overhead, journal logging) are already realized by the current architecture — invest in measuring and documenting them rather than building new features.

Incremental improvements (parallel init, local scheduling, UDP port-forward) are implemented as needed when concrete workloads demand them.

## Consequences

- Lazy image pulling introduces a FUSE dependency on the host; not all environments support FUSE (some container-in-container setups, some hardened kernels)
- Event-driven cgroup metrics depend on systemd DBus signal delivery being reliable under load; needs stress testing at 100+ pods per host
- FUSE-backed ConfigMap volumes add a per-volume process; at high volume counts the file descriptor and process overhead may matter
- Local scheduling bypasses scheduler plugins and quota — must be explicitly opt-in and clearly documented as a fast path, not a replacement
- Each differentiator should be benchmarked against the containerd equivalent before being marketed; claims need numbers
