# Periapsis

A Kubernetes node agent that runs pods as native systemd-nspawn containers, bypassing CRI and containerd entirely. One physical host can register as multiple independent virtual nodes ("pawns"), each with isolated networking, pod lifecycle, and cgroup tree.

---

## The Problem

Running Kubernetes on bare metal at high density is harder than it should be. A standard kubelet has a hard 110-pod limit and carries containerd, runc, and their shim layer with it. When you have powerful servers in expensive, space-constrained datacenters, you want thousands of pods per host — not hundreds.

The typical workaround is painful: deploy vSphere or KVM on the bare metal, provision VMs with KubeVirt or similar, then run kubelets inside those VMs. You get density, but at the cost of two extra abstraction layers, two control planes to maintain, and a debugging surface that requires specialists across Kubernetes, the hypervisor, and networking just to diagnose a single issue.

Periapsis takes a different approach. A single perigeos daemon registers the host as multiple virtual nodes directly in the Kubernetes API. Pods run as systemd transient units — no hypervisor, no extra control plane, one process to debug.

---

## How It Works

Periapsis is a fork of virtual-kubelet that registers as a virtual Kubernetes node, accepts pod assignments from the control plane, and manages the full container lifecycle: image pull, network setup, resource limits, exec, logging, and status reporting.

The runtime is systemd-nspawn. No Docker. No containerd. Just:

- **systemd transient units** — pods are machines in machinectl, logs go to journald
- **overlayfs** — OCI images extracted once, shared across pods via copy-on-write layers
- **cgroups v2** — CPU, memory, and IO limits enforced by the kernel directly
- **CNI** — per-pod network namespaces; Constellation (Cilium fork) for cross-host eBPF routing

### Multi-pawn architecture

One host registers as N virtual nodes. Each pawn has its own TLS certificate, pod CIDR, and cgroup slice. The Kubernetes scheduler treats them as independent nodes. This is how a single physical machine runs 2000 pods while the scheduler still thinks it's talking to 30 separate nodes.

```
engix99 (Xeon E5-2690 v4)
├── compute-00  (virtual node)
├── compute-01  (virtual node)
├── ...
└── compute-29  (virtual node, 30 total)

engifire (Intel N150)
├── engifire-pawn-01
└── engifire
```

---

## Performance

**2000 nginx replicas across 2 physical hosts, 32 virtual nodes:**

```
10,763 RPS  ·  229µs median latency  ·  0.00% errors  ·  2.26M requests
p95 = 12.67ms  ·  p99 < 2s  ·  1000 concurrent VUs
```

Pawn hit distribution (even spread, no hotspots):
```
engifire-pawn-01    4.4%  (100,113 hits)
engifire            4.3%  ( 97,503 hits)
compute-27          4.3%  ( 97,052 hits)
...
compute-22          1.8%  ( 40,923 hits)    ← N150 has fewer pawns
```

Daemon RSS: ~67–200 MiB (varies by pawn and pod count).

Full results: [docs/show-off.md](docs/show-off.md)

---

## What Works

- Pod lifecycle: create, update, delete, restart policies, crash loop backoff
- Init containers and sidecar containers
- OCI image pull with layer caching and peer-to-peer layer sharing
- ConfigMap, Secret, emptyDir, projected volumes, downward API
- exec, attach, logs, port-forward (Kubelet API)
- Resource limits (CPU, memory, IO) via cgroups v2
- Liveness, readiness, and startup probes
- Environment variable injection including service discovery vars
- Multi-pawn host registration (N virtual nodes per host)
- Constellation CNI: eBPF datapath, VXLAN cross-host routing, per-pod netns
- Envoy Gateway for L7 ingress via Gateway API
- journald integration — pod logs visible in journalctl
- machinectl integration — running pods visible as machines
- `apsis` CLI for introspection, debugging, and reconciliation
- KillMode=process — `systemctl restart perigeos` leaves pods running

## What Does Not Work Yet

- **PersistentVolumeClaims**: local-path provisioner works; distributed CSI drivers untested
- **SecurityContext**: unprivileged pods work; full SecurityContext field coverage is incomplete — not all fields map cleanly to systemd-nspawn
- **Windows**: not supported, not planned
- **Non-systemd Linux**: not supported
- **StatefulSets with stable network identity**: untested at scale
- **VolumeSnapshot, ephemeral inline volumes**: not implemented
- **Vertical Pod Autoscaler**: untested

---

## Naming

| Name | Role |
|------|------|
| **Periapsis** | The project (closest orbital approach — generic) |
| **Perigeos** | The daemon binary (Earth-specific periapsis) |
| **Pawn** | A virtual Kubernetes node — wordplay on systemd-ns**pawn** |
| **Gambit** | The PodProvider implementation |
| **Constellation** | Cilium-based CNI fork for multi-pawn networking |
| **Apsis** | CLI for introspection and debugging |

---

## Requirements

- Linux with systemd v250+ and cgroups v2
- Kernel 5.15+ (eBPF features used by Constellation)
- Kubernetes 1.34+
- Go 1.26+ (to build)

Optional: Constellation CNI for cross-host pod networking and multi-pawn isolation. Without it, pods use veth bridges on the host network namespace — single-pawn deployments only.

---

## Quick Start

### Build

```bash
go build ./cmd/perigeos
go build ./cmd/apsis
```

### Deploy

```bash
# Install systemd service and binary
./deploy/perigeos-install.sh

# Start
systemctl start perigeos

# Verify
kubectl get nodes
apsis status
```

Config: `/etc/apsis/perigeos/perigeos.toml`
State: `/var/lib/apsis/perigeos`
Logs: `journalctl -u perigeos`

For CNI-backed multi-pawn deployments, apply manifests from `deploy/constellation/`.
For L7 ingress, apply `deploy/envoy/` (GatewayClass, EnvoyProxy, Gateway, HTTPRoute).

### Verify

```bash
kubectl get nodes
kubectl run test --image=busybox --restart=Never -- sleep 3600
kubectl exec -it test -- sh

apsis status
apsis doctor
```

---

## Architecture

```
Kubernetes API server
        │
        ▼
  PodController          ← watches pod assignments
        │
        ▼
     Gambit               ← PodProvider: image, network, runtime
    ┌──┴──────────────────────────────┐
    │                                 │
Image Manager                   Network Manager
(OCI pull, overlayfs, CAS)      (CNI, netns, IPAM)
    │                                 │
    └──────────────┬──────────────────┘
                   ▼
            systemd-nspawn
            (transient unit)
                   │
          ┌────────┴────────┐
        cgroups v2       journald
```

Key paths:
- `cmd/perigeos/main.go` — entrypoint, wires all controllers
- `node/lifecycle.go` — pod creation: CNI → init containers → app containers
- `node/batchwatcher.go` — container state polling and status push
- `node/podstore.go` — in-memory pod state registry
- `internal/runtime/systemd/` — systemd-nspawn machine management
- `internal/image/` — OCI pull, overlayfs extraction, layer cache
- `internal/network/` — CNI setup, Constellation integration
- `node/api/` — Kubelet HTTP API (exec, logs, port-forward)
- `adr/` — architecture decisions with full rationale

---

## Architecture Decisions

See `adr/` for full records. Notable:

- **ADR-0002**: Monorepo split — gambit.go → lifecycle.go, hydration.go, status.go, exec.go, saga.go
- **ADR-0009**: KillMode=process — perigeos restarts leave pods running; zero-downtime upgrades
- **ADR-0010**: UID/GID mapping for unprivileged containers

---

## Related Projects

- [Constellation](https://github.com/malformed-c/constellation) — eBPF/Cilium CNI fork
- [virtual-kubelet](https://github.com/virtual-kubelet/virtual-kubelet) — upstream fork base (Apache 2.0)

---

Periapsis is licensed under GPLv3. It incorporates a fork of virtual-kubelet by the VK authors (Apache 2.0). See [NOTICES](NOTICES) for full third-party attribution.

Kubernetes is a trademark of The Linux Foundation.
