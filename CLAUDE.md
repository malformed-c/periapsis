# CLAUDE.md

Quick-start for Claude Code. Read `AGENTS.md` first — it has the full picture.

## What this is

Periapsis is a fork of virtual-kubelet v1.11.0 that contains the full perigeos stack (ADR-0002).
The `cmd/perigeos` binary, provider (`internal/provider/gambit.go`), runtime, network, and image
packages all live in this single module.

## Build

```bash
go build ./cmd/perigeos
```

## Test

```bash
go mod tidy
go test ./...
```

Integration tests in `internal/runtime/systemd/` require root and a running systemd:
```bash
sudo -E go test ./internal/runtime/systemd/... -v -count=1
```

## Key files

| Path | Role |
|------|------|
| `cmd/perigeos/main.go` | Binary entrypoint — spawns pawns, wires controllers |
| `node/podcontroller.go` | Pod sync loop — drives create/update/delete |
| `node/pod.go` | `createOrUpdatePod`, `handleProviderError` |
| `node/dispatch.go` | Routes provider calls to Gambit directly (ADR-0002) |
| `node/sync.go` | Async pod status polling wrapper |
| `internal/provider/gambit.go` | Pod lifecycle implementation (image, network, runtime) |
| `internal/podutils/env.go` | Env var population — where the fieldRef fix lives |
| `internal/runtime/systemd/` | systemd-nspawn machine management |
| `internal/network/` | CNI network management |
| `internal/image/` | OCI image pull and overlayfs |
| `internal/control/` | Varlink + TCP control server |
| `errdefs/` | Error type definitions |
| `node/api/` | Kubelet HTTP API (exec, attach, logs, port-forward) |
| `deploy/` | Systemd service file and install script |
| `adr/` | Architecture Decision Records |

## Constraints

- ADR-0002 is in progress: `PodLifecycleHandler` interface is being inlined (dispatch.go routes to Gambit directly)
- The interface is kept for now because `node/` tests use mock providers
- k8s deps are pinned to v0.34.x
