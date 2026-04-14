# ADR-0005: Multi-tenancy - host daemon with per-cluster kubelet shims

**Status:** Todo (revisit when multi-cluster demand materializes)  
**Date:** 2026-03-20

## Context

Perigeos currently assumes one process, one cluster, one kubeconfig. The entire codebase - from `cmd/perigeos/main.go` through Gambit, the runtime, image manager, and network manager - is wired to a single `kubernetes.Interface`.

Multi-tenancy requires running pawns for different clusters on the same physical host. Two clusters (A and B) both want virtual nodes on the same machine. This raises the question of who owns the host resources: cgroup slices, memory, Constellation sockets, image cache, and the systemd bus.

Running two independent perigeos processes (one per cluster) is the quick answer but creates resource arbitration problems - both processes allocate cgroup slices and memory independently, leading to overcommit with no coordination. It also doubles the overhead: two image caches, two sets of informers, two Constellation agents.

## Decision

When multi-tenancy is needed, split perigeos into two layers:

### Layer 1: Periapsis host daemon

A single system-level daemon per host, running as a systemd service. It owns:

- **systemd bus access** - sole process talking to DBus for machine lifecycle (RunMachine, StopMachine)
- **Image cache** - single shared OCI image store, deduplicated across clusters
- **Cgroup tree** - the `perigeos.slice` root and all pawn sub-slices, with a global view of host capacity
- **Network namespace management** - creates and configures netns, calls CNI
- **Constellation agent lifecycle** - one agent per host regardless of cluster count

The daemon exposes a **varlink API** over a Unix socket for local clients. The API surface maps roughly to the current `Runtime` + `ImageManager` + `NetworkManager` interfaces:

```
io.perigeos.Machine.Run(config: PodConfig) -> (unit: string)
io.perigeos.Machine.Stop(uid: string, container: string) -> ()
io.perigeos.Machine.Status(uid: string, container: string) -> (state: MachineState)
io.perigeos.Machine.List(pawn: string) -> (machines: []MachineInfo)
io.perigeos.Machine.Exec(uid: string, container: string, cmd: []string) -> (fd: int)
io.perigeos.Machine.Logs(uid: string, container: string, opts: LogOpts) -> (fd: int)
io.perigeos.Image.Pull(ref: string) -> (digest: string)
io.perigeos.Image.Mount(digest: string, target: string) -> (layers: []string)
io.perigeos.Network.Setup(uid: string, netns: string) -> (ip: string)
io.perigeos.Network.Teardown(uid: string) -> ()
io.perigeos.Host.Capacity() -> (cpu: string, memory: string, disk: string)
io.perigeos.Host.Allocate(pawn: string, cpu: string, memory: string) -> ()
io.perigeos.Host.Release(pawn: string) -> ()
```

Resource allocation is explicit: a kubelet shim requests a slice allocation for its pawns, and the daemon rejects if the host is overcommitted. This prevents the race between independent processes.

### Layer 2: Perigeos kubelet shim

A thin process per cluster. Each shim:

- Holds one kubeconfig pointing to its cluster's API server
- Runs the periapsis node controller and pod controller (the VK / framework layer)
- Translates Kubernetes pod lifecycle calls into varlink calls to the host daemon
- Reports node status back to its cluster

The shim is stateless beyond its kubeconfig and pawn assignment. It doesn't touch systemd, the image store, or the network stack directly. If it crashes, the host daemon keeps machines running - the shim restarts, re-lists from the API server, and reconciles.

Multiple shims coexist on the same host, each managing a disjoint set of pawns for their respective clusters. Pawn names include a cluster identifier to avoid collisions.

### Registration

Each cluster's Apogeos operator has its own `PerigeosHost` CRD (ADR-0004). The host daemon knows about all clusters via a local config that maps cluster names to kubeconfig paths. When `perigeos join` runs with a token for cluster A, it registers with cluster A's Apogeos and adds a shim config entry for that cluster. Running `perigeos join` again with cluster B's token adds a second shim.

### Constellation

One Constellation agent per host, managed by the host daemon. Both clusters' pods share the same CNI. The agent's `--managed-nodes` includes pawns from all clusters. Each cluster's Apogeos operator reports its pawns to the host daemon (via varlink or local CRD), and the daemon writes the aggregated list.

CIDR allocation needs care - different clusters must not overlap pod CIDRs. Either use distinct `--cluster-pool-ipv4-cidr` ranges per cluster, or run separate Constellation agents per cluster (one socket per cluster, losing the shared-agent benefit).

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        Physical Host                         │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐   │
│  │           Periapsis Host Daemon (PID 1-ish)          │   │
│  │                                                      │   │
│  │   systemd DBus ←→ nspawn machines                    │   │
│  │   OCI image cache (shared)                           │   │
│  │   cgroup slice management                            │   │
│  │   Constellation agent lifecycle                      │   │
│  │   varlink socket: /run/periapsis/periapsis.sock      │   │
│  └───────────────────┬──────────────────────────────────┘   │
│                      │ varlink                               │
│          ┌───────────┴───────────┐                           │
│          │                       │                           │
│  ┌───────▼───────┐       ┌──────▼────────┐                  │
│  │  Perigeos     │       │  Perigeos     │                  │
│  │  Shim (A)     │       │  Shim (B)     │                  │
│  │               │       │               │                  │
│  │  kubeconfig-A │       │  kubeconfig-B │                  │
│  │  pawns:       │       │  pawns:       │                  │
│  │   a-worker-00 │       │   b-worker-00 │                  │
│  │   a-worker-01 │       │   b-worker-01 │                  │
│  └───────┬───────┘       └──────┬────────┘                  │
│          │                      │                            │
└──────────┼──────────────────────┼────────────────────────────┘
           │                      │
    ┌──────▼──────┐        ┌──────▼──────┐
    │  Cluster A  │        │  Cluster B  │
    │  API Server │        │  API Server │
    └─────────────┘        └─────────────┘
```

## Why not now

- Two hosts, one cluster. The overhead of the daemon/shim split is not justified.
- The varlink API surface needs to stabilize. The `Runtime`, `ImageManager`, and `NetworkManager` interfaces are still evolving (hostPID, RunProgram, privileged mode).
- ADR-0002 (framework integration) should land first. The shim layer depends on a clean boundary between "kubelet logic" and "runtime execution" - exactly what the framework refactor will clarify.
- CIDR allocation across clusters is an unsolved design problem that needs real multi-cluster testing.

## Trigger conditions

Revisit this ADR when:

- A concrete need arises to run pawns for two or more clusters on the same host
- The periapsis framework refactor (ADR-0002) has landed, establishing a clean kubelet/runtime boundary
- The varlink API surface is stable enough to version

## Consequences

- The host daemon becomes the single source of truth for host resource allocation
- Image pulls are deduplicated across clusters sharing a host
- Crash isolation improves - a shim crash doesn't affect other clusters' workloads
- The varlink API is a new interface to maintain and version
- CIDR overlap prevention requires either operator coordination or per-cluster Constellation instances
- Operational complexity increases: one daemon + N shims per host instead of one process
- The existing single-process mode remains for standalone/dev use
