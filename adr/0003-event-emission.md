# ADR-0003: Emit Kubernetes events for all sync loop errors

**Status:** Accepted  
**Date:** 2026-03-20

## Context

Virtual-kubelet's pod sync loop swallows most errors into log lines and queue requeues. The only path that emits Kubernetes events is `createOrUpdatePod` in `node/pod.go`, and only for provider-level create/update/delete outcomes. Every other failure is invisible to `kubectl describe pod`.

This makes debugging unnecessarily painful. A pod stuck in Pending could be caused by any of the following, none of which produce an event:

1. **`PopulateEnvironmentVariables` failure** — `createOrUpdatePod` calls `podutils.PopulateEnvironmentVariables`, which can fail on unresolvable configmap/secret refs, unsupported fieldRef paths, or missing resource manager data. The error returns up the call chain, gets wrapped in `"failed to sync pod"`, logged at warn level by the queue, and requeued. The pod sits in Pending with no event explaining why.

2. **`syncPodFromKubernetesHandler` lister failure** — if the pod lister returns a non-404 error, the key is requeued silently. No event, no status update.

3. **`updatePodStatus` failure** — the status update path (`syncPodStatusFromProviderHandler`) can fail on conflict, not-found, or API errors. Failures are returned to the queue handler and requeued. The pod shows stale status with no indication that updates are failing.

4. **`deletePodsFromKubernetesHandler` failure** — force-delete failures (conflict, API error) are logged and requeued. No event on the pod being deleted.

5. **`deleteDanglingPods` failure** — provider-side dangling pod cleanup logs errors but emits no events. Orphaned pods are silently retried.

6. **`handleProviderError` status update failure** — when the provider returns an error and VK tries to update the pod's status to reflect it, the status update itself can fail. This is logged at warn level but produces no event — the original provider error is also lost from the pod's event stream because `handleProviderError` doesn't emit an event, it only updates status.

7. **Node controller failures** — `node/node.go` emits zero events. Lease renewal failures, node status update failures, and ping failures are all log-only. A pawn that loses API connectivity shows no events on the Node object.

The queue's retry mechanism makes this worse: errors are retried with backoff, so a persistent failure (e.g. missing configmap) generates hundreds of log lines over minutes but zero user-visible signal. The only way to diagnose is to have access to the perigeos process logs and know which log messages to grep for.

## Decision

Add Kubernetes event emission to every error path in periapsis's sync loop and node controller. Specifically:

### Pod controller — `node/pod.go`

- **`createOrUpdatePod`**: emit a Warning event when `PopulateEnvironmentVariables` fails, before returning the error. Reason: `FailedPopulateEnv`. This is the single highest-value fix — it covers the class of errors that previously required 5 hours of log reading.
- **`handleProviderError`**: emit a Warning event with the original provider error. Reason: `ProviderError`. Currently this only updates pod status, which can itself fail silently.
- **`updatePodStatus`**: emit a Warning event when the status API call fails. Reason: `FailedStatusUpdate`.
- **`deletePod`**: already emits events (good).

### Pod controller — `node/podcontroller.go`

- **`syncPodFromKubernetesHandler`**: emit a Warning event when the pod lister returns a non-404 error. Reason: `FailedSync`.
- **`syncPodInProvider`**: emit a Warning event when `createOrUpdatePod` fails (this wraps the inner error that already has an event, but the sync-level wrapper adds context about retry state).
- **`deleteDanglingPods`**: emit a Warning event on the pod when provider-side delete fails. Reason: `FailedDeleteDangling`.

### Pod controller — `node/pod.go` delete path

- **`deletePodsFromKubernetesHandler`**: emit a Warning event when the API server delete call fails with a non-404, non-conflict error. Reason: `FailedForceDelete`.

### Node controller — `node/node.go`

- Add a recorder to `NodeController`.
- Emit Warning events on the Node object for: lease renewal failure (`FailedLeaseRenewal`), node status update failure (`FailedNodeStatusUpdate`), ping timeout (`NodePingTimeout`).

### Queue-level last-error surfacing

- When the queue exhausts retries for a key (reaches `MaxRetries` and calls `Forget`), emit a Warning event on the associated pod. Reason: `SyncRetriesExhausted`. Message includes the final error. This is the catch-all for any error path that was missed above — if a pod's sync fails enough times to be forgotten by the queue, the user sees it.

### Event reason constants

All new reasons are defined in a single `const` block in `node/pod.go` alongside the existing `podEventCreateFailed` etc. constants, prefixed consistently.

## Consequences

- `kubectl describe pod` and `kubectl get events` become the primary debugging tools for stuck pods, replacing log grep
- Event volume increases; persistent failures will generate one event per retry cycle. This is acceptable — events are the Kubernetes-native mechanism for communicating issues, and they have built-in deduplication (same reason + message within a time window collapses)
- The recorder must be wired into `NodeController` (currently it has none); this requires a small API change to `NewNodeController` or its option funcs
- No behavioral change to error handling — errors still return, requeue, and retry exactly as before. Events are additive observability
