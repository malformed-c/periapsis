# Periapsis

Periapsis is a fork of virtual-kubelet v1.11.0 that serves as the complete perigeos kubelet implementation. It is a monorepo containing the full perigeos stack: the `cmd/perigeos` binary, the Gambit provider (`node/gambit.go`), systemd-nspawn runtime, CNI networking, OCI image management, and control plane.

The name follows the orbital theme: *periapsis* is the generic term for the closest orbital point — perigeos is the Earth-specific version.

## What it is

Periapsis runs Kubernetes pods as isolated systemd-nspawn containers on a Linux host. It registers as a virtual Kubernetes node that accepts pod assignments from the control plane, manages their full lifecycle (image pull, network setup, resource limits, logging, exec, port-forward), and reports status back to the API server.

The perigeos binary is deployed as a systemd service and communicates with Kubernetes via kubeconfig. Containers are managed through systemd-machined and journald, with networking optionally handled by Constellation (Cilium-based CNI) or built-in veth bridges.

## Architecture

### Core Components

**Pod Controller** (`node/podcontroller.go`, `node/pod.go`)
- Syncs pod state between API server and systemd-nspawn runtime
- Drives create, update, and delete operations
- Manages the work queue with custom retry policy and metrics
- Handles provider errors and pod status updates

**Gambit Provider** (`node/gambit.go`)
- Implements pod lifecycle: image pull, network setup, machine creation, status reconciliation
- Orchestrates the image manager, network manager, and runtime
- Forward reconciler enables provider-driven pod discovery on startup

**Image Manager** (`internal/image/`)
- Pulls OCI images using go-containerregistry
- Extracts rootfs to overlayfs mounts for container isolation
- Handles registry authentication and layer caching

**Network Manager** (`internal/network/`)
- Supports multiple networking backends: built-in veth bridges or Constellation (Cilium CNI)
- Manages IP allocation via multi-pool IPAM
- Creates network namespaces and veth pairs for pods

**Runtime** (`internal/runtime/systemd/`)
- Uses systemd-nspawn for container isolation
- Manages transient machines with resource limits via cgroup slices
- Provides exec, attach, and log streaming via journald
- Exposes container status and lifecycle events

**Control Socket** (`internal/control/`)
- Varlink + TCP server for management operations
- Used by apsis CLI for introspection and debugging

**Kubelet API** (`node/api/`)
- HTTP API for exec, attach, logs, port-forward, metrics
- Implements standard kubelet endpoints

### Key Design Decisions

See `adr/` for full architectural decision records. Key milestones:

- **ADR-0001**: Fork virtual-kubelet to fix two bugs that cannot be patched from provider side
- **ADR-0002**: Evolve from library to framework — perigeos moves inside periapsis as a monorepo (phases 4-8 complete)
  - Phase 4: PodProvider interface replaces PodLifecycleHandler; dispatch layer removed
  - Phase 5: Live ConfigMap/Secret volume refresh
  - Phase 6: Forward reconciler for pod discovery on startup
  - Phase 7: Work queue tuning with metrics
  - Phase 8: Internal provider packages dissolved into node/

## Building

```bash
go build ./cmd/perigeos
```

The resulting binary is statically linked and does not require external dependencies at runtime beyond systemd, cni plugins, and container images.

## Testing

```bash
go mod tidy
go test ./...
```

Integration tests for the systemd runtime require root and a running systemd:

```bash
sudo -E go test ./internal/runtime/systemd/... -v -count=1
```

## Deployment

Periapsis deploys as a systemd service. The systemd unit is defined in `deploy/perigeos.service` and includes:

- ExecStart pointing to `/usr/local/bin/perigeos`
- Configuration via `/etc/apsis/perigeos/perigeos.toml` and kubeconfig
- State stored in `/var/lib/apsis/perigeos`
- Control socket at `/run/apsis/perigeos.sock`
- Logging to journald

The `deploy/perigeos-install.sh` script handles installation. Constellation manifests (for CNI-backed deployments) are in `deploy/constellation/`.

## Key Files

| Path | Role |
|------|------|
| `cmd/perigeos/main.go` | Binary entrypoint — spawns pawns, wires controllers |
| `node/podcontroller.go` | Pod sync loop — drives create/update/delete |
| `node/pod.go` | Pod creation, status updates, error handling |
| `node/gambit.go` | Pod lifecycle: image pull, network, runtime orchestration |
| `node/api/` | Kubelet HTTP endpoints (exec, attach, logs, port-forward) |
| `internal/runtime/systemd/` | systemd-nspawn machine management |
| `internal/network/` | CNI and veth bridge management, IPAM |
| `internal/image/` | OCI image pull and overlayfs extraction |
| `internal/control/` | Varlink control server for management |
| `internal/podutils/` | Environment variable and downward API handling |
| `errdefs/` | Error type definitions |
| `adr/` | Architecture Decision Records |
| `deploy/` | Systemd service file and deployment scripts |

## Dependencies

- Go 1.26+
- systemd v250+ (for nspawn transient units and cgroup v2)
- Kubernetes API client (pinned to v0.34.x)
- Container runtime: systemd-nspawn (no Docker or containerd required)
- Networking: CNI plugins (for Constellation or built-in veth)
- Container registries: docker.io, gcr.io, or any OCI-compliant registry

## Known Constraints

- Kubernetes API version pinned to v0.34.x for perigeos compatibility
- PodProvider interface retained as a testing seam; Gambit is the sole production implementation
- Containers run with systemd-nspawn — requires Linux host with systemd
- Memory pressure detection currently based on cgroup memory limits (ADR-0005 multi-tenancy)
- Disk pressure thresholds hardcoded to 85% (inode 95%, memory 95%)

## Documentation

- `CLAUDE.md` — Developer quick-start for agents
- `AGENTS.md` — Detailed repo guide (older; see `adr/` for current state)
- `CHANGES.md` — Patches applied vs upstream v1.11.0
- `adr/` — Architecture decisions, rationale, and consequences

## Related

- [perigeos](https://github.com/malformed-c/perigeos) — Full perigeos stack (legacy)
- [constellation](https://github.com/malformed-c/constellation) — Cilium-based CNI plugin used with Periapsis
- [virtual-kubelet](https://github.com/virtual-kubelet/virtual-kubelet) — Upstream fork base
