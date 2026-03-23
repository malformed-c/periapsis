# Periapsis

A Kubernetes kubelet that runs pods as systemd-nspawn containers. One process, one machine, scales to thousands of pods with minimal overhead.

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

**3000 pods on 30 virtual nodes, single 28-core machine:**

```
$ kubectl get deployment scale-test
NAME         READY       UP-TO-DATE   AVAILABLE
scale-test   3000/3000   3000         3000

$ apsis status
Pods:    3000
Pawns:   30
Memory:  351 MiB (perigeos process)
Per-pod: ~117 KB
```

- **CPU**: 28-core Xeon E5-2690 v4 @ 2.60 GHz
- **Memory**: 48 GB host, 6.2 GB in containers, 351 MiB perigeos process
- **Networking**: Constellation (eBPF per-pod netns), sub-millisecond pod-to-pod latency
- **Runtime**: systemd-nspawn, no external container daemon
- **Distribution**: 100 pods per virtual node, auto-balanced by kube-scheduler

See [docs/show-off.md](docs/show-off.md) for full results.

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
- `deploy/` — Systemd unit, install script, Constellation CNI

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

## Learning More

- `CLAUDE.md` — Quick-start for developers
- `AGENTS.md` — Detailed repo guide (see `adr/` for current state)
- `CHANGES.md` — Patches vs upstream v1.11.0
- `adr/` — Architecture decisions with rationale
- `docs/show-off.md` — Scale and performance results

## Related Projects

- [Constellation](https://github.com/malformed-c/constellation) — eBPF/Cilium CNI used with Periapsis
- [virtual-kubelet](https://github.com/virtual-kubelet/virtual-kubelet) — Upstream fork base
