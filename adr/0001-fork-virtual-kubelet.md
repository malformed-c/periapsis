# ADR-0001: Fork virtual-kubelet as Periapsis

**Status:** Implemented
**Date:** 2026-03-20

## Context

Perigeos uses virtual-kubelet as a library to implement virtual Kubernetes nodes backed by systemd-nspawn containers. Two bugs in virtual-kubelet v1.11.0 cannot be fixed from the provider side:

1. `PopulateEnvironmentVariables` hard-errors on `status.podIP` fieldRef — this runs inside VK before `CreatePod` is ever called, so the provider has no way to intercept or work around it. Pods using Downward API env vars with `status.podIP` (e.g. sonobuoy) fail permanently.

2. `syncPodInProvider` skips pods already in `Failed`/`Succeeded` phase without checking whether the provider actually ran them. If a watch reconnection drops the ADD event for a pod, VK sees a pod in `Pending` phase, starts polling `GetPodStatus` (which returns NotFound since the provider never received the pod), and after ~1 minute marks it `Failed`. On the next sync it hits the terminal phase skip and never retries. The pod is stuck forever.

Upstream virtual-kubelet is not actively maintained at the pace needed to fix these quickly.

## Decision

Fork virtual-kubelet v1.11.0 as `github.com/malformed-c/periapsis`. Squash upstream history into a single commit to keep the repo clean. Apply targeted fixes to the two known bugs. Keep the `PodLifecycleHandler` interface intact — perigeos plugs in as a standard provider.

## Consequences

- Perigeos depends on Periapsis via a `replace` directive pointing to a local checkout
- k8s dependency versions in Periapsis must stay in sync with perigeos — do not bump them independently
- Future VK upstream improvements must be cherry-picked manually
- The fork scope is intentionally narrow: fix bugs, don't redesign
