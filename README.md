# Periapsis

A Kubernetes kubelet that runs pods as systemd-nspawn containers. One process, one machine, scales to thousands of pods with minimal overhead.

**Why Periapsis?** Standard kubelets carry Docker or containerd with them. Periapsis skips all of that — it wires Kubernetes directly to `systemd-nspawn`, dropping daemon overhead to ~220 KB per pod. It is distribution-agnostic: works with k3s, vanilla Kubernetes, or anything that speaks the kubelet API. A single physical host can be sliced into many independent virtual nodes ("pawns"), each with its own TLS cert, pod lifecycle, and cgroup tree — useful for edge fleets and high-density testing environments. Host slicing into multiple pawns with proper network namespace isolation requires the Constellation CNI.

**What it does:** Periapsis is a fork of virtual-kubelet v1.11.0 that registers as a virtual Kubernetes node, accepts pod assignments from the control plane, manages container lifecycle (image pull, network setup, resource limits, exec, logging), and reports status back to the API server.

**The perigeos binary** runs as a systemd service and orchestrates pod execution through systemd-nspawn containers. No Docker, no containerd — just systemd, cgroups, and the Linux kernel.

The name follows the orbital theme: *periapsis* is the generic term for the closest orbital point; *perigeos* is the Earth-specific version.

---

## Naming

- **Periapsis** — the monorepo (this project); the generic name for closest orbital approach
- **Perigeos** — the kubelet binary and systemd service
- **Pawn** — wordplay on systemd-ns**pawn**; each virtual Kubernetes node
- **Gambit** — the PodProvider implementation (image pull, network, runtime orchestration)
- **Constellation** — Cilium-based CNI fork for pod networking
- **Apsis** — CLI tool for introspection and debugging (apsis status, apsis doctor, etc.)

---

## Quick Start

### Build

```bash
go build ./cmd/perigeos
```

The binary is statically linked; no runtime dependencies beyond systemd and CNI plugins.

### Deploy

```bash
# Install systemd service
./deploy/perigeos-install.sh

# Start perigeos
systemctl start perigeos

# Verify
kubectl get nodes
```

The perigeos service reads configuration from:
- `/etc/apsis/perigeos/perigeos.toml` (config)
- `$KUBECONFIG` or `/etc/rancher/k3s/k3s.yaml` (API access)

State is stored in `/var/lib/apsis/perigeos`, logs go to journald.

### Verify

```bash
# Check cluster registration
kubectl get nodes -L container-runtime

# Run a test pod
kubectl run test --image=busybox --restart=Never -- sleep 3600

# Inspect via apsis
apsis status
apsis doctor
```

---

## Scale Demo

**1,772 pods across 2 physical hosts, 33 virtual nodes:**

```
$ apsis status
Pods:    1,660          (engix99, 30 pawns)
         112            (engifire, 2 pawns)
Memory:  365 MiB        (engix99, daemon RSS ~220 KB/pod)
          89 MiB        (engifire, daemon RSS ~795 KB/pod)
```

- **Hardware**: 28-core Xeon E5-2690 v4 (engix99) + Intel N150 (engifire)
- **Networking**: Constellation (eBPF per-pod netns, VXLAN cross-host tunnel)
- **Runtime**: systemd-nspawn, no external container daemon
- **Stress test**: 2.5 M requests, 11,913 rps sustained, 0% errors, 257 us median latency

### Envoy Gateway

L7 ingress via Envoy Gateway (hostNetwork DaemonSet on primary + control-plane nodes):

- 1 M requests through Gateway API HTTPRoute, 4,869 rps, 0% errors
- p95 = 134 ms, p99 = 151 ms — engix99 was at load average 100–200 during this test; latency reflects CPU starvation on an oversubscribed machine, not typical L7 overhead

See [docs/show-off.md](docs/show-off.md) for full results and comparison tables.

## How It Works

### Control Flow

1. **Pod Controller** watches the Kubernetes API server for pod assignments
2. **Gambit Provider** executes pod lifecycle operations:
   - Image pull (OCI, with auth and caching)
   - Network setup (CNI or veth, with IPAM)
   - Machine creation (systemd-nspawn transient unit)
   - Status reconciliation (cgroup limits, health checks)
3. **Kubelet API** serves exec, attach, logs, port-forward, metrics
4. **Control Socket** exposes introspection (apsis CLI)

### Components

| Component | Path | Role |
|-----------|------|------|
| Pod Controller | `node/podcontroller.go` | Sync loop: drives create/update/delete with metrics and retry logic |
| Gambit Provider | `node/gambit.go` | Pod lifecycle: image pull, network, runtime orchestration |
| Image Manager | `internal/image/` | OCI pull, overlayfs extraction, layer caching |
| Network Manager | `internal/network/` | CNI or veth setup, IPAM, per-pod netns |
| Runtime | `internal/runtime/systemd/` | systemd-nspawn, transient units, cgroup slices, journald |
| Kubelet API | `node/api/` | HTTP: exec, attach, logs, port-forward |
| Control Socket | `internal/control/` | Varlink + TCP for management operations |
| Envoy Gateway | `deploy/envoy/` | Gateway API ingress (EnvoyProxy, Gateway, HTTPRoute) |

### Architecture Decisions

See `adr/` for full records. Key milestones:

- **ADR-0001**: Fork virtual-kubelet to fix two provider-side bugs
- **ADR-0002**: Monorepo evolution (phases 4-8 complete)
  - Phase 4: PodProvider interface, dispatch layer removal
  - Phase 5: Live ConfigMap/Secret refresh
  - Phase 6: Forward reconciler for startup pod discovery
  - Phase 7: Work queue metrics tuning
  - Phase 8: Provider packages dissolved into node/

## Build & Test

**Build:**
```bash
go build ./cmd/perigeos
```

**Test:**
```bash
go test ./...
```

**Integration tests (runtime, requires root):**
```bash
sudo -E go test ./internal/runtime/systemd/... -v -count=1
```

## Deployment Details

The systemd service (`deploy/perigeos.service`) runs perigeos with:
- Binary: `/usr/local/bin/perigeos`
- Config: `/etc/apsis/perigeos/perigeos.toml`
- Kubeconfig: auto-detected from `$KUBECONFIG` or `/etc/rancher/k3s/k3s.yaml`
- State: `/var/lib/apsis/perigeos`
- Control socket: `/run/apsis/perigeos.sock`
- Logging: journald (use `journalctl -u perigeos` or `apsis logs`)

Installation: `./deploy/perigeos-install.sh`

For CNI-backed deployments, deploy Constellation manifests from `deploy/constellation/`.

For L7 ingress, deploy Envoy Gateway with the manifests in `deploy/envoy/` (GatewayClass, EnvoyProxy, Gateway, HTTPRoute). The proxy runs as a hostNetwork DaemonSet on primary and control-plane nodes.

## Key Files

- `cmd/perigeos/main.go` — Entrypoint, wires controllers
- `node/podcontroller.go` — Watch loop, create/update/delete dispatch
- `node/gambit.go` — Pod lifecycle (image, network, runtime orchestration)
- `node/api/` — Kubelet HTTP (exec, attach, logs, port-forward, metrics)
- `internal/runtime/systemd/` — Machine management, cgroups
- `internal/network/` — CNI/veth setup, IPAM
- `internal/image/` — OCI pull, overlayfs extraction
- `internal/control/` — Varlink control server
- `internal/podutils/` — Environment, downward API
- `adr/` — Architecture decisions
- `deploy/` — Systemd unit, install script, Constellation CNI, Envoy Gateway

## Requirements

- Go 1.26+
- systemd v250+ (transient units, cgroup v2)
- Linux kernel (systemd-nspawn container host)
- Kubernetes 1.34+
- Container registries: docker.io, gcr.io, or any OCI-compliant source

Optional: Constellation for eBPF CNI (default: veth bridges)

## Known Constraints

- Kubernetes API pinned to v0.34.x (perigeos compatibility)
- Gambit is the sole production provider (PodProvider interface retained for testing)
- Linux + systemd required (no Windows, no containerd)
- Memory pressure based on cgroup limits (ADR-0005 tracks multi-tenancy)
- Disk pressure thresholds: 85% (inode 95%, memory 95%)
- **Security:** Pods run unprivileged by default. Resource isolation uses cgroups v2 (CPU, memory, and IO limits supported). Not all Kubernetes `SecurityContext` fields map 1:1 to `systemd-nspawn`; full SecurityContext coverage is incomplete.
- **Storage:** PersistentVolumeClaims work and are tested with local-path provisioner. Distributed CSI drivers have not been tested.
- **Host slicing:** Slicing a host into multiple pawns with isolated pod networking requires the Constellation CNI. Without it, pods use veth bridges sharing the host network namespace.

## Learning More

- `CLAUDE.md` — Quick-start for developers
- `AGENTS.md` — Detailed repo guide (see `adr/` for current state)
- `CHANGES.md` — Patches vs upstream v1.11.0
- `adr/` — Architecture decisions with rationale
- `docs/show-off.md` — Scale and performance results

## Related Projects

- [Constellation](https://github.com/malformed-c/constellation) — eBPF/Cilium CNI used with Periapsis
- [virtual-kubelet](https://github.com/virtual-kubelet/virtual-kubelet) — Upstream fork base
