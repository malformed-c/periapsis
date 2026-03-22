# ADR-0002: Evolve Periapsis from library to framework

**Status:** In Progress (Phase 4 — PodLifecycleHandler removed, PodProvider interface replaces it, dispatch.go deleted, syncProviderWrapper removed)
**Date:** 2026-03-20

## Context

Periapsis (ADR-0001) forks virtual-kubelet v1.11.0 as a library. Perigeos imports it and plugs in via the `PodLifecycleHandler` interface. This works today — the two bugs that motivated the fork were fixed with small patches to VK internals that the provider interface couldn't reach.

However, the interface boundary already constrains perigeos in ways that will compound:

1. **Pre-provider pipeline is opaque.** `PopulateEnvironmentVariables`, status phase decisions, and env expansion all run inside VK before `CreatePod` is called. Perigeos cannot reorder them (e.g. running CNI before env population to make `status.podIP` available earlier) or inject error handling.

2. **Reconciliation is one-sided.** VK's sync loop decides what to create, delete, and skip. The provider can only react. The forward reconciler hook (open work item) is a workaround — the provider wants to participate in the loop, not just receive its output. Each such hook fattens `PodLifecycleHandler` into a contract that only perigeos implements.

3. **Work queue internals are inaccessible.** The pods-stuck-Pending bug under 30+ concurrent pods is likely VK's internal queue backpressure. Diagnosing and fixing it requires patching `podcontroller.go` — not the interface, but the implementation behind it. This is viable in a fork but erodes the "clean separation" argument.

4. **Upstream cherry-picking is already fictional.** The module path is renamed, k8s deps are pinned to perigeos versions, and the two existing patches touch core sync logic. The practical likelihood of pulling upstream VK improvements without conflict is low and decreasing.

## Decision

When the trigger conditions below are met, restructure periapsis from a library that perigeos imports into a framework that contains perigeos. Concretely:

- `cmd/perigeos` moves into periapsis (or periapsis absorbs perigeos — repo choice is cosmetic)
- Gambit's pod lifecycle logic inlines into the sync loop in `node/podcontroller.go` and `node/pod.go`, replacing the `PodLifecycleHandler` dispatch with direct calls to image pull, overlayfs, CNI, and `RunMachine`
- The `PodLifecycleHandler` interface is removed; the runtime, network, and image packages become internal to the unified module
- Packages that are genuinely reusable (`errdefs`, `node/api` HTTP handlers, `log` interface) stay as-is

This is not a rewrite. The VK scaffolding (node controller, lease renewal, pod controller loop, API server) stays intact. What changes is that perigeos logic moves from behind an interface into the body of the loop.

## Trigger conditions

Do not act on this ADR until at least one of:

- A third provider-side callback or hook is added to `PodLifecycleHandler` that exists solely for perigeos (two hooks is a pattern; three is an abstraction serving one consumer)
- A bug fix requires patching VK's `podcontroller.go` or `node.go` internals for the third time (at that point the fork divergence makes the library boundary vestigial)
- The pods-stuck-Pending issue is confirmed as a VK work queue problem requiring internal changes to queue sizing, retry policy, or backpressure

## Consequences

- Single module, single repo, atomic commits across the full stack
- No more `go.mod` sync constraint between two repos
- The forward reconciler hook becomes trivial — the reconciler *is* the provider
- Pod creation ordering (CNI before env population, etc.) becomes a local refactor
- Upstream VK cherry-picks are no longer possible; this is accepted as already effectively true
- The `PodLifecycleHandler` interface disappears, so a second provider cannot plug in; this is accepted as a non-goal
