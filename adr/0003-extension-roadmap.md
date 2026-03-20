# ADR-0003: Periapsis extension roadmap

**Status:** Accepted  
**Date:** 2026-03-20

## Context

Periapsis forks virtual-kubelet v1.11.0 as a library that perigeos plugs into via the `PodLifecycleHandler` interface. ADR-0001 fixed two bugs that the interface couldn't reach. ADR-0002 proposes eventually dissolving the interface and making periapsis a framework.

Regardless of when ADR-0002 triggers, periapsis has gaps that affect real workloads today. These fall into three tiers based on impact, plus a cross-cutting observability problem that compounds all of them.

## Observability: event emission

VK's sync loop swallows most errors into log lines and queue requeues. The only code path that emits Kubernetes events is `createOrUpdatePod` in `node/pod.go`, and only for provider-level create/update/delete outcomes. Everything else is invisible to `kubectl describe pod`.

A pod stuck in Pending could be caused by `PopulateEnvironmentVariables` failing on an unresolvable configmap ref, a pod lister error, a status update conflict, or a dozen other internal failures — none of which produce an event. The queue retries with backoff, generating hundreds of log lines over minutes but zero user-visible signal. Diagnosing this requires access to perigeos process logs and knowledge of which wrapped error messages to grep for.

### Changes required

**`node/pod.go`:**
- `createOrUpdatePod`: emit Warning event when `PopulateEnvironmentVariables` fails (reason: `FailedPopulateEnv`). Highest-value single fix.
- `handleProviderError`: emit Warning event with the original provider error (reason: `ProviderError`). Currently only updates status, which can itself fail silently.
- `updatePodStatus`: emit Warning event when the status API call fails (reason: `FailedStatusUpdate`).

**`node/podcontroller.go`:**
- `syncPodFromKubernetesHandler`: emit Warning event on non-404 lister errors (reason: `FailedSync`).
- `deleteDanglingPods`: emit Warning event when provider-side delete fails (reason: `FailedDeleteDangling`).

**`node/pod.go` delete path:**
- `deletePodsFromKubernetesHandler`: emit Warning event on non-404 non-conflict API delete failures (reason: `FailedForceDelete`).

**`node/node.go`:**
- Add a recorder to `NodeController` (currently has none; requires a new `NodeControllerOpt`).
- Emit Warning events on the Node object for lease renewal failure (`FailedLeaseRenewal`), node status update failure (`FailedNodeStatusUpdate`), ping timeout (`NodePingTimeout`).

**Queue catch-all:**
- When the queue exhausts retries for a key and calls `Forget`, emit a Warning event on the associated pod (reason: `SyncRetriesExhausted`). Insurance for any error path missed above.

## Tier 1 — breaks real workloads

### Graceful termination

VK's `deletePod` immediately calls `provider.DeletePod` with no lifecycle handling. There is no PreStop hook execution, no terminationGracePeriodSeconds observance, no SIGTERM-then-wait-then-SIGKILL sequence. When the API server requests deletion with a 30-second grace period, VK calls DeletePod immediately and Gambit kills the machine.

Any workload that drains connections, flushes buffers, or persists state on shutdown is broken. This is the single largest gap between perigeos and a real kubelet.

**Changes required:**
- `deletePod` in `node/pod.go` must read `pod.Spec.TerminationGracePeriodSeconds` and `pod.DeletionGracePeriodSeconds`
- Execute PreStop hooks (exec and HTTP) before signalling the provider
- Pass the grace period to the provider so it can SIGTERM → wait → SIGKILL
- `PodLifecycleHandler` needs a new method or `DeletePod` needs a grace period parameter (this is one of the interface changes that may trigger ADR-0002)

### Pod admission

VK accepts every pod the scheduler sends. There is no resource fit check against advertised node capacity. If the scheduler places 50 pods on a pawn advertising 1G memory, VK calls CreatePod for all of them. Real kubelet rejects with Unschedulable.

**Changes required:**
- Pre-CreatePod admission check in `createOrUpdatePod` comparing pod resource requests against node allocatable minus already-running pods
- Reject with a status update (phase Pending, reason Unschedulable) and Warning event when the pod doesn't fit
- The provider needs to expose current resource usage, either via a new interface method or by the framework querying it directly

### Forward reconciler

On startup after a crash, perigeos has pods on disk and running systemd machines, but VK's sync loop has an empty in-memory state. VK re-lists from the API server and drives CreatePod for pods the provider already has running. Gambit handles this defensively (checks for existing machines), but it causes unnecessary churn and race conditions with the reconciler.

**Changes required:**
- A startup hook where the provider returns its known pod set before the sync loop begins processing
- `syncPodInProvider` consults this set to skip creation for pods already running
- This is listed in periapsis open work item #2

## Tier 2 — production gaps

### Eviction

Gambit computes disk/memory/PID pressure for node conditions. VK reports these conditions and the scheduler stops sending new pods. But existing pods keep running until the OOM killer gets them randomly. Real kubelet evicts lowest-priority pods in order.

**Changes required:**
- Periodic eviction check in the sync loop (or as a separate controller)
- Priority-sorted pod list with QoS-class tiebreaking (BestEffort → Burstable → Guaranteed)
- Provider hook to report current per-pod resource consumption

### ConfigMap/Secret refresh

Mounted volumes are populated at pod creation and never updated. Workloads depending on rotated credentials or dynamic configuration will read stale data indefinitely.

**Changes required:**
- Watch or periodic poll for ConfigMap/Secret changes affecting running pods
- Provider hook to update mounted volume content in a running machine
- Needs careful handling — some pods mount as env vars (immutable after start), some as volume files (updatable)

### Ephemeral containers

`kubectl debug` does not work. VK's UpdatePod is effectively a no-op for the ephemeral container case. Supporting this means detecting a new ephemeral container in the pod spec diff and driving a partial creation — adding a container to an existing machine's network/PID namespace.

**Changes required:**
- `createOrUpdatePod` detects ephemeral container additions in the update path
- Provider (or framework-inline) creates a new nspawn machine sharing the target pod's namespaces
- Status reporting includes ephemeral container states

## Tier 3 — unlocked by framework integration (ADR-0002)

### Pod creation reordering

Currently: env population → CreatePod → CNI → status.podIP. The ADR-0001 `status.podIP` bug was a direct consequence — env population runs before CNI, so podIP is empty. With the framework approach: CNI setup → env population (podIP now known) → RunMachine. Impossible through the provider interface because `PopulateEnvironmentVariables` runs before `CreatePod`.

### Unified status reporting

Currently VK polls `GetPodStatus` on a timer and the provider reconstructs status from systemd state. With framework integration, the sync loop directly observes machine state transitions and updates status atomically with the action that caused them. Eliminates poll lag and status races.

## Decision

Implement in order: observability (event emission), then tier 1 (graceful termination, admission, forward reconciler), then tier 2 as needed. Tier 3 is deferred to ADR-0002 trigger conditions.

Event emission and forward reconciler can be implemented today without interface changes. Graceful termination and admission will require either interface extensions or ADR-0002 — track which approach is taken, as interface extensions here count toward ADR-0002's trigger threshold.

## Consequences

- `kubectl describe` and `kubectl get events` become the primary debugging tools, replacing log grep
- Graceful termination enables stateful workloads (databases, queue consumers, anything with connection draining)
- Pod admission prevents resource overcommit and the cascading failures it causes
- Each tier 1 item that requires a `PodLifecycleHandler` change moves closer to ADR-0002's trigger conditions
- Tier 2 items are optional and should be driven by concrete workload requirements, not speculative completeness
