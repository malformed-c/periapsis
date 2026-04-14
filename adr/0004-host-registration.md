# ADR-0004: Token-based host registration

**Status:** Accepted  
**Date:** 2026-03-20

## Context

Adding a host to a Perigeos-managed cluster is fully manual: SSH in, copy a kubeconfig, write `perigeos.toml`, build the binary, run it. There is no discovery, no authentication handshake, and no mechanism for the Apogeos operator to track which hosts exist or push configuration to them.

As the fleet grows beyond the current two hosts (engix99, engicarbon), this manual process becomes the bottleneck. It also has a security gap - the kubeconfig placed on the host has full cluster access and never expires.

## Decision

Implement a kubeadm-style bootstrap token flow for registering new hosts with an Apogeos-managed cluster. The token is short-lived and single-purpose: it proves the host is authorized to join, bootstraps a scoped credential, and then expires.

### Flow

1. **Token creation.** An admin (or Apogeos operator) creates a bootstrap token:
   ```
   apogeosctl token create --ttl 15m
   ```
   This generates a `bootstrap.kubernetes.io/token` Secret in `kube-system` (or a dedicated `perigeos-system` namespace) with a configurable TTL. The token format follows kubeadm convention: `[a-z0-9]{6}.[a-z0-9]{16}` (token ID + token secret).

2. **Host join.** On the new host:
   ```
   perigeos join --token <token> --apogeos <endpoint>
   ```
   Perigeos presents the token to the Apogeos operator's registration endpoint. Apogeos validates the token against the Secret (checking expiry and usage count), then:
   - Creates a `PerigeosHost` CRD object representing this host
   - Issues a client certificate scoped to the host identity (signed by the cluster CA or a dedicated Perigeos CA), with permissions limited to: watching its own `PerigeosHost` object, reporting Node status for its pawns, and the standard kubelet RBAC policy
   - Returns the certificate bundle and the host's assigned name

3. **Token expiry.** The bootstrap token is valid for its TTL (default 15 minutes). After expiry, or after a configurable number of uses (default 1), the Secret is deleted or marked expired. A leaked token has a narrow exploitation window.

4. **Ongoing auth.** After bootstrap, the host uses its client certificate for all subsequent communication. Certificate rotation follows the standard Kubernetes CSR flow - perigeos requests a new cert before the current one expires, Apogeos auto-approves CSRs from known hosts.

### PerigeosHost CRD

```yaml
apiVersion: periapsis.io/v1alpha1
kind: PerigeosHost
metadata:
  name: engicarbon
spec:
  # Desired pawn configuration - pushed by Apogeos, reconciled by perigeos
  pawns:
    - name: pawn-01-compute
      cpu: "100%"
      memory: "512M"
      labels:
        kubernetes.io/os: linux
        kubernetes.io/arch: amd64
      taints:
        - key: node.periapsis.io/type
          value: pawn
          effect: NoSchedule
    - name: pawn-02-infra
      cpu: "200%"
      memory: "1G"
      infrastructure: true
  cni:
    enabled: true
status:
  # Reported by perigeos on the host
  phase: Ready  # Joining | Ready | NotReady | Draining
  capacity:
    cpu: "8"
    memory: "16Gi"
    disk: "200Gi"
  activePawns:
    - name: pawn-01-compute
      nodeName: pawn-01-compute  # as registered in k8s
      ready: true
    - name: pawn-02-infra
      nodeName: pawn-02-infra
      ready: true
  lastHeartbeat: "2026-03-20T14:30:00Z"
```

Perigeos watches its own `PerigeosHost` object and reconciles toward `spec.pawns` - creating, removing, or reconfiguring pawns without a restart. The operator watches `status` to track fleet health.

### Constellation integration

The Apogeos operator owns the `constellation-config` ConfigMap. It watches all `PerigeosHost` status reports, aggregates active pawn names across the fleet, and writes the authoritative `managed-nodes` value. This replaces the current per-host read-modify-write pattern that races under concurrent updates.

## Consequences

- `perigeos join` is the single entry point for adding a host - no manual kubeconfig or TOML editing
- Bootstrap tokens are temporal, minimizing the blast radius of leaks
- The `PerigeosHost` CRD is the source of truth for host configuration, enabling the operator to push pawn changes fleet-wide
- Perigeos becomes a reconciliation loop on its own `PerigeosHost` spec rather than a static config reader
- The existing TOML config path remains as a fallback for standalone/dev use where no Apogeos operator is running
- Certificate rotation requires implementing the CSR flow in perigeos, which is well-documented but non-trivial
- The `PerigeosHost` CRD must be registered by the Apogeos operator before any host can join
