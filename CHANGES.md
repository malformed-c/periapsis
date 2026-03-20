# Changes from virtual-kubelet v1.11.0

## Fixes

### `internal/podutils/env.go` — status fieldRef support
`podFieldSelectorRuntimeValue` now handles `status.podIP`, `status.podIPs`, `status.hostIP`, and `status.phase` directly from the pod object. Previously these fell through to `ExtractFieldPathAsString` which returned `unsupported fieldPath` — a hard error that blocked `createOrUpdatePod` before it ever reached `CreatePod`. Any remaining unsupported fieldPath now degrades to an empty string rather than erroring.

### `node/podcontroller.go` — dropped-event recovery for terminal phase pods
`syncPodInProvider` previously skipped all pods in `Failed` or `Succeeded` phase unconditionally. Now it first calls `GetPod` on the provider — if the provider has no record of the pod, it drives creation anyway. This recovers pods whose informer ADD event was dropped during a watch reconnection, leaving the pod in a stale terminal phase in etcd without the provider ever running it.

## Module

- Module path changed from `github.com/virtual-kubelet/virtual-kubelet` to `github.com/malformed-c/periapsis`

## Removed

- `cmd/virtual-kubelet` — upstream CLI, not used by perigeos
- `log/logrus`, `log/klogv2` — upstream log adapters, perigeos provides its own
- `trace/opencensus`, `trace/opentelemetry` — upstream tracing adapters
- `node/nodeutil` — upstream helper utilities not used by perigeos
- `docs/`, `examples/`, `hack/`, `website/`, `test/` — upstream infra
