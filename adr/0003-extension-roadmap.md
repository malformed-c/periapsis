# ADR-0003: Periapsis extension roadmap

**Status:** Implemented (eviction + ephemeral containers remain tier 2 backlog)
**Date:** 2026-03-20
**Updated:** 2026-03-28

## Context

Periapsis forks virtual-kubelet v1.11.0 as a library that perigeos plugs into via the `PodLifecycleHandler` interface. ADR-0001 fixed two bugs that the interface couldn't reach. ADR-0002 proposes eventually dissolving the interface and making periapsis a framework.

Regardless of when ADR-0002 triggers, periapsis has gaps that affect real workloads today. These fall into three tiers based on impact, plus a cross-cutting observability problem that compounds all of them.

## Observability: event emission

VK's sync loop swallows most errors into log lines and queue requeues. The only code path that emits Kubernetes events is `createOrUpdatePod` in `node/pod.go`, and only for provider-level create/update/delete outcomes. Everything else is invisible to `kubectl describe pod`.

A pod stuck in Pending could be caused by `PopulateEnvironmentVariables` failing on an unresolvable configmap ref, a pod lister error, a status update conflict, or a dozen other internal failures - none of which produce an event. The queue retries with backoff, generating hundreds of log lines over minutes but zero user-visible signal. Diagnosing this requires access to perigeos process logs and knowledge of which wrapped error messages to grep for.

### Changes required - **all implemented**

**`node/pod.go`:**
- ~~`createOrUpdatePod`: emit Warning event when `PopulateEnvironmentVariables` fails (reason: `FailedPopulateEnv`).~~ Done - emitted from `createPodSync` in gambit.go.
- ~~`handleProviderError`: emit Warning event with the original provider error (reason: `ProviderError`).~~ Done.
- ~~`updatePodStatus`: emit Warning event when the status API call fails (reason: `FailedStatusUpdate`).~~ Done.

**`node/podcontroller.go`:**
- ~~`syncPodFromKubernetesHandler`: emit Warning event on non-404 lister errors (reason: `FailedSync`).~~ Done.
- ~~`deleteDanglingPods`: emit Warning event when provider-side delete fails (reason: `FailedDeleteDangling`).~~ Done.

**`node/pod.go` delete path:**
- ~~`deletePodsFromKubernetesHandler`: emit Warning event on non-404 non-conflict API delete failures (reason: `FailedForceDelete`).~~ Done.

**`node/node.go`:**
- ~~Add a recorder to `NodeController`.~~ Done - `WithNodeEventRecorder` option.
- ~~Emit Warning events on the Node object for lease renewal failure (`FailedLeaseRenewal`), node status update failure (`FailedNodeStatusUpdate`), ping timeout (`NodePingFailed`).~~ Done.

**Queue catch-all:**
- ~~When the queue exhausts retries for a key and calls `Forget`, emit a Warning event on the associated pod (reason: `SyncRetriesExhausted`).~~ Done - via `Queue.OnForget` callback.

**`node/gambit.go` (provider-level):**
- `CheckMachined` failure: `FailedPreFlight` event. Done.
- `PopulateEnvironmentVariables` failure: `FailedPopulateEnv` wrapping event. Done.
- Creation retry loop: `FailedCreate` event per attempt. Done.
- `markPodFailed`: `CreateFailed` event for terminal failures. Done.

## Tier 1 - breaks real workloads - **all implemented**

### Graceful termination - done

`Gambit.DeletePod` now enforces `terminationGracePeriodSeconds` as a context timeout shared
between PreStop hooks and container stop. `runPreStopHooks` executes exec and HTTP hooks
before signalling containers. `StopMachine` sends SIGTERM; systemd's `TimeoutStopSec`
(set from `terminationGracePeriodSeconds` at unit creation) enforces the SIGKILL deadline.

### Pod admission - done

`Gambit.admitPod` checks aggregate resource requests against remaining node capacity before
`CreatePod` proceeds. Rejected pods receive a `FailedAdmission` Warning event and the error
propagates to the API server.

### Forward reconciler - done

`HydrateFromRuntime` reconstructs the pod set from running systemd units at startup.
`CreatePod` detects hydrated pods and skips re-creation, upgrading hydration stubs to full
pod objects when the informer reconnects. `RequestSync(namespace, name)` lets Gambit trigger
PodController re-syncs without polling.

## Tier 2 - production gaps

### Eviction

Gambit computes disk/memory/PID pressure for node conditions. VK reports these conditions and the scheduler stops sending new pods. But existing pods keep running until the OOM killer gets them randomly. Real kubelet evicts lowest-priority pods in order.

**Changes required:**
- Periodic eviction check in the sync loop (or as a separate controller)
- Priority-sorted pod list with QoS-class tiebreaking (BestEffort → Burstable → Guaranteed)
- Provider hook to report current per-pod resource consumption

### ConfigMap/Secret refresh - done

`Gambit.SetInformers` registers `UpdateFunc` handlers on ConfigMap and Secret informers.
`refreshConfigMapVolumes` / `refreshSecretVolumes` perform inode-preserving rewrites of
projected volume files via `RefreshConfigMapDirect` / `RefreshSecretDirect`. Env vars
remain immutable after start (matching kubelet behavior).

### Ephemeral containers

`kubectl debug` does not work. VK's UpdatePod is effectively a no-op for the ephemeral container case. Supporting this means detecting a new ephemeral container in the pod spec diff and driving a partial creation - adding a container to an existing machine's network/PID namespace.

**Changes required:**
- `createOrUpdatePod` detects ephemeral container additions in the update path
- Provider (or framework-inline) creates a new nspawn machine sharing the target pod's namespaces
- Status reporting includes ephemeral container states

## Tier 3 - unlocked by framework integration (ADR-0002) - **all implemented**

### Pod creation reordering - done

ADR-0002 dissolved the provider interface boundary. `createPodSync` now runs:
CNI setup → `PopulateEnvironmentVariables` (podIP known) → image pull → RunMachine.
The ADR-0001 `status.podIP` empty-at-env-time bug is structurally eliminated.

### Unified status reporting - done

The batch watcher observes systemd unit state transitions and pushes status updates
via `notifyPodStatus`. `GetPodStatus` polling is eliminated; `buildPodStatus` reads
live unit state and journal exit codes atomically with the transition that caused them.

## Decision

Implement in order: observability (event emission), then tier 1 (graceful termination, admission, forward reconciler), then tier 2 as needed. Tier 3 is deferred to ADR-0002 trigger conditions.

**Update (2026-03-28):** All tiers complete except eviction and ephemeral containers (tier 2).
ADR-0002 was completed, dissolving the provider interface and enabling tier 3 items.
Graceful termination and admission were implemented directly in Gambit without interface extensions.

## Future: PreStop hook shim

The current PreStop implementation runs hooks via `RunInContainer` before calling `StopMachine`.
This works but has a timing window: the hook runs while the container is still fully alive,
then `StopMachine` sends SIGTERM as a separate step.

A cleaner approach: build a small shim binary (`perigeos-prestop`) that is bind-mounted into
containers and invoked via `ExecStop=` in the systemd unit. This would:

- Execute the PreStop hook command inside the container's namespaces
- Run as part of systemd's native stop sequence (after SIGTERM, before SIGKILL)
- Eliminate the two-phase stop (hook then stop) in favor of systemd's built-in ordering
- Handle the case where the container process exits before the hook completes

The shim would read the hook spec from an environment variable or a mounted JSON file
and dispatch exec/HTTP hooks accordingly. This is a refinement, not a prerequisite -
the current `RunInContainer`-based approach is correct and matches kubelet's behavior.

## Consequences

- `kubectl describe` and `kubectl get events` are now the primary debugging tools - all error paths emit events
- Graceful termination enables stateful workloads (databases, queue consumers, anything with connection draining)
- Pod admission prevents resource overcommit and the cascading failures it causes
- ADR-0002 was triggered and completed - `PodLifecycleHandler` dissolved, Gambit inlined into the framework
- Remaining tier 2 items (eviction, ephemeral containers) are optional and should be driven by concrete workload requirements
