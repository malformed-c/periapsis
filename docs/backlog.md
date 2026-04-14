# Periapsis / Perigeos - Feature Backlog

Last updated: 2026-04-13

## Kubelet Conformance Stack (goal)

Target: perigeos passes kubelet conformance with a full production stack.

| Component | Status |
|-----------|--------|
| k8s cluster (vanilla, not k3s-dependent) | Blocked on CSR flow |
| Constellation CNI (Cilium fork) | Working |
| Envoy Gateway (ingress/gateway API) | Not started |
| SeaweedFS CSI (distributed storage) | Partial - globalmount-direct works on primary node |
| Kubelet conformance tests | Not started |

## Feature Backlog

### CSR flow for pawn TLS
Replace CA key signing with k8s CertificateSigningRequest API. Removes k3s CA dependency, enables vanilla k8s support. **Prerequisite for `perigeos join`.**

### `perigeos join` command
Bootstrap a host into the cluster: discover API server, generate kubeconfig, detect existing kubelet, write default config, install systemd service. **Depends on CSR flow.**

### Kubernetes topology support
Topology-aware scheduling - `topology.kubernetes.io` labels, topology spread constraints, zone/region awareness.

### KEDA support
Event-driven autoscaling for perigeos workloads (ScaledObject/ScaledJob targeting perigeos nodes).

### Multi-process pawn architecture
Split monolithic perigeos into 30 pawn processes (one per virtual node) for fault isolation. Trade-off: ~900 MiB RSS vs ~200 MiB current, 30x API server watches. Not urgent - current single-process model works.

### P2P in-flight image sharing
Hosts discover and wait on in-flight image pulls from other hosts instead of pulling independently. P2P image distribution and manifest caching already work.

### Plugin registration (CSINode creation)
Implement kubelet plugin registration protocol: watch `/var/lib/kubelet/plugins_registry/` with fsnotify, discover CSI drivers via gRPC `GetInfo()`, call `NodeGetInfo()`, create CSINode objects for all pawn nodes. Required for external-attacher to process VolumeAttachments. Implementation plan exists - see memory for full protocol details.

## Completed (for reference)

- Multi-pool IPAM (both perigeos + constellation sides - managed scope allocator, pool-directed allocation, CiliumNode CIDR reconciliation)
- ADR-0002 phases 4-8 (provider dissolution, env pipeline, forward reconciler, work queue, package dissolution)
- Extended events (probe events with thresholds, image pull progress %)
- P2P image distribution + manifest caching
- VXLAN hairpin bypass for managed node CIDRs
- Cilium identity for perigeos pods (label standardization to `periapsis.io/`)
- CiliumNode CiliumInternalIP propagation to all managed pawns
- Tidal metadata.labels/annotations via fieldRef
