# AGENTS.md — Guide for AI agents working on Periapsis

## What this repo is

Periapsis is a fork of [virtual-kubelet v1.11.0](https://github.com/virtual-kubelet/virtual-kubelet) maintained for the [perigeos](https://github.com/malformed-c/perigeos) stack. It is a **Go library only** — no binary, no deployment. Perigeos imports it as a module.

The fork exists to fix two bugs that cannot be worked around from the provider side. See `adr/0001-fork-virtual-kubelet.md` for the full rationale and `CHANGES.md` for the patch list.

---

## Repository layout

```
node/
  podcontroller.go     — pod sync loop, queue setup, event handlers
  pod.go               — createOrUpdatePod, handleProviderError, updatePodStatus
  node.go              — node controller, lease renewal
  api/                 — kubelet HTTP API: exec, attach, logs, port-forward, metrics
internal/
  podutils/
    env.go             — PopulateEnvironmentVariables, fieldRef resolution (patched)
    helper.go          — ExtractFieldPathAsString, ConvertDownwardAPIFieldLabel
  queue/               — rate-limited work queue used by pod controller
  manager/             — resource manager (configmaps, secrets, services)
  expansion/           — env var expansion
errdefs/               — NotFound, Invalid error types
log/                   — VK logger interface (nop default; perigeos injects its own)
trace/                 — tracing interface (nop default)
adr/                   — Architecture Decision Records
CHANGES.md             — all patches vs upstream v1.11.0
```

---

## Known patches (do not revert)

### `internal/podutils/env.go` — `podFieldSelectorRuntimeValue`
Added explicit handling for `status.podIP`, `status.podIPs`, `status.hostIP`, `status.phase`. Unsupported fieldPaths now return `""` instead of an error. This prevents `createOrUpdatePod` from failing before reaching `CreatePod` when a pod uses Downward API env vars referencing status fields that are empty pre-CNI.

### `node/podcontroller.go` — `syncPodInProvider`
Added `GetPod` check before the terminal phase skip. If `pod.Status.Phase` is `Failed`/`Succeeded` but the provider returns nil for `GetPod`, creation is driven anyway. This recovers pods whose informer ADD event was dropped during watch reconnection.

---

## Constraints

- **k8s dep versions** — must stay in sync with perigeos `go.mod`. Never bump `k8s.io/*` or `sigs.k8s.io/*` independently.
- **PodLifecycleHandler interface** — must remain backward compatible with perigeos. Do not add required methods.
- **No new deps** — any new dependency requires a matching update in perigeos `go.mod`.
- **No binary** — Periapsis is a library. Do not add `cmd/` entries.

---

## Open work

1. **Event emission before CreatePod** — errors in `PopulateEnvironmentVariables` and other pre-creation steps don't emit Kubernetes events, making them invisible in `kubectl describe pod`. Should emit a Warning event from `createOrUpdatePod` on any pre-provider error.
2. **Forward reconciler hook** — a provider callback to list pods the provider knows about on startup, enabling perigeos to recover missed pods without polling the API server independently.
