# Periapsis

Periapsis is a fork of [virtual-kubelet v1.11.0](https://github.com/virtual-kubelet/virtual-kubelet) maintained as the kubelet layer of the [perigeos](https://github.com/malformed-c/perigeos) stack.

The name follows the orbital theme: *periapsis* is the generic term for the closest orbital point — perigeos is the Earth-specific version.

---

## Why a fork?

Virtual-kubelet's provider interface has a few hard bugs that cannot be worked around from the provider side:

- `PopulateEnvironmentVariables` returns a hard error for `status.podIP` and other status fieldRefs that are legitimately empty before CNI runs, blocking pod creation entirely
- `syncPodInProvider` skips pods in `Failed`/`Succeeded` phase without checking whether the provider actually ran them — a dropped watch event leaves pods permanently stuck

These are fixed here. The provider interface itself is unchanged; perigeos plugs in as a standard `PodLifecycleHandler`.

---

## Relationship to perigeos

Perigeos uses Periapsis as a Go module dependency via a `replace` directive:

```
require github.com/malformed-c/periapsis v0.0.0
replace github.com/malformed-c/periapsis => ../periapsis
```

Dependency versions in `go.mod` are intentionally kept compatible with perigeos. Do not bump k8s deps independently.

---

## Changes from upstream

See [`adr/`](adr/) for documented decisions and [`CHANGES.md`](CHANGES.md) for a summary of patches applied on top of v1.11.0.
