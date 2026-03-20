# CLAUDE.md

Quick-start for Claude Code. Read `AGENTS.md` first — it has the full picture.

## What this is

Periapsis is a fork of virtual-kubelet v1.11.0, used as the kubelet layer of the perigeos stack. It is a Go module library — there is no binary. Perigeos imports it via a `replace` directive.

## Build

There is no binary to build. To verify the library compiles:

```bash
go build ./...
```

## Test

```bash
go mod tidy
make test
```

## Key files

| Path | Role |
|------|------|
| `node/podcontroller.go` | Pod sync loop — where the terminal phase fix lives |
| `internal/podutils/env.go` | Env var population — where the fieldRef fix lives |
| `node/pod.go` | `createOrUpdatePod`, `handleProviderError` |
| `errdefs/` | Error type definitions used by providers |
| `node/api/` | Kubelet HTTP API (exec, attach, logs, port-forward) |
| `CHANGES.md` | All patches applied on top of v1.11.0 |
| `adr/` | Architecture Decision Records |

## Constraints

- k8s dependency versions must stay compatible with perigeos `go.mod` — do not bump independently
- The `PodLifecycleHandler` interface must remain backward compatible with perigeos
- Do not add new dependencies without a corresponding update to perigeos `go.mod`
