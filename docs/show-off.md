# Perigeos Show-off: 1,770 Pods Across 2 Hosts

**Date:** 2026-04-04
**Stack:** Perigeos (virtual-kubelet fork) + Constellation (Cilium fork) + k3s

---

## Hardware

| Host | CPU | Cores | RAM | Kernel | Role |
|------|-----|-------|-----|--------|------|
| engix99 | Xeon E5-2690 v4 @ 2.60 GHz | 28 | 48 GB | 6.19.11-arch1-1 | Control plane (k3s) + 30 pawns |
| engifire | Intel N150 | 4 | 16 GB | 6.19.10-zen1-1-zen | 1 primary + 1 pawn |

Interconnect: direct Ethernet (192.168.50.x), 0.15 ms RTT.
VXLAN tunnel between hosts runs over the wired link, not WiFi.

## Cluster Topology

```
33 Kubernetes nodes total  (1 real k3s + 32 perigeos virtual nodes)

engix99      30 pawns    1,660 pods    365 MiB daemon RSS
engifire      2 pawns      112 pods     89 MiB daemon RSS
                          ─────
                          1,772 pods
```

Each pawn is a fully-functional virtual kubelet: own TLS certificate,
independent pod lifecycle, cgroup tree, and network namespace. Pods run as
systemd-nspawn machines with overlayfs rootfs and Constellation CNI networking.

## Stress Test (k6)

1,000 concurrent VUs ramping over 3 m 30 s against a ClusterIP service
fronting nginx-whoami across all 33 nodes.

```
  THRESHOLDS

    errors
    rate=0.00%                              < 0.01  PASS

    http_req_duration
    p(95)=4.05ms                            < 500   PASS
    p(99)=10.04ms                           < 1000  PASS


  TOTAL RESULTS

    checks_total........: 7,507,074   35,740/s
    checks_succeeded....: 100.00%
    checks_failed.......: 0.00%

    HTTP
    http_reqs...........: 2,502,358   11,913/s
    http_req_duration...: avg=998 us  med=257 us  p(90)=1.48 ms  p(95)=4.05 ms  p(99)=10 ms  max=860 ms
    http_req_failed.....: 0.00%

    NETWORK
    data_received.......: 4.4 GB      21 MB/s
    data_sent...........: 170 MB      810 kB/s

    EXECUTION
    iterations..........: 2,502,358   11,913/s
    vus_max.............: 1,000


  running (3m30.0s), 0/1000 VUs, 2,502,358 complete and 0 interrupted iterations
```

**Zero errors. 2.5 million requests. Sub-millisecond median latency at ~12k rps.**

## Runtime Status

### engix99

```
$ apsis status
Hostname:    engix99
Version:     dev
Uptime:      1h47m
Pawns:       30
Pods:        1,660
Kernel:      6.19.11-arch1-1
Memory:      29 / 48 GB
CPU cores:   28

Machines:    1,660
RSS:         365 MiB
LXC veths:   1,665
Netns:       1,665
```

### engifire

```
$ apsis status
Hostname:    engifire
Version:     dev
Uptime:      1h45m
Pawns:       2
Pods:        112
Kernel:      6.19.10-zen1-1-zen
Memory:      2.7 / 16 GB
CPU cores:   4

Machines:    112
RSS:         89 MiB
LXC veths:   113
Netns:       112
```

## Pawn Distribution (engix99)

```
$ apsis pawns
NAME        ROLE  PORT   PODS
compute-00  pawn  12261  56
compute-01  pawn  12262  55
compute-02  pawn  12263  55
compute-03  pawn  12264  56
...
compute-29  pawn  12290  56

30 pawns, ~55 pods each, balanced by kube-scheduler.
```

## Envoy Gateway

Envoy Gateway deployed as a hostNetwork DaemonSet on primary + control-plane
nodes. HTTPRoute fronts the same nginx-whoami deployment.

```
$ kubectl -n envoy-gateway-system get pods -o wide
NAME                                                      READY   NODE
envoy-envoy-gateway-system-apsis-gateway-fbf4d1d5-rkpmp   2/2     engifire
envoy-envoy-gateway-system-apsis-gateway-fbf4d1d5-x5hrg   2/2     engix99
envoy-gateway-59f6cb6596-bs2zt                            1/1     engix99
```

### Stress Test: Envoy Gateway (k6, 1000 VUs, 3m30s)

```
  THRESHOLDS

    errors
    rate=0.00%                              < 0.01  PASS

    http_req_duration
    p(95)=134.48ms                          < 500   PASS
    p(99)=151.47ms                          < 1000  PASS


  HTTP
  http_reqs...........: 1,022,563   4,869/s
  http_req_duration...: avg=76.3 ms  med=72.0 ms  p(90)=125.8 ms  p(95)=134.5 ms  p(99)=151.5 ms  max=215.8 ms
  http_req_failed.....: 0.00%

  NETWORK
  data_received.......: 1.8 GB      8.4 MB/s
  data_sent...........: 70 MB       331 kB/s
```

### Direct vs Envoy Gateway

| Metric | Direct (ClusterIP) | Envoy Gateway |
|--------|-------------------|---------------|
| Throughput | 11,913 rps | 4,869 rps |
| Median latency | 257 us | 72.0 ms |
| p95 latency | 4.05 ms | 134.5 ms |
| p99 latency | 10 ms | 151.5 ms |
| Error rate | 0.00% | 0.00% |

**Note on Envoy latency:** During this test engix99 was running at a load average of 100–200. The 72 ms median latency reflects Envoy threads being starved of CPU on a heavily oversubscribed machine, not standard L7 proxy overhead. The fact that Envoy still routed 1 million requests with 0% errors under these conditions demonstrates datapath stability; the raw throughput and latency numbers are not representative of a properly-resourced deployment.

## The Numbers

| Metric | Value |
|--------|-------|
| Total pods | 1,772 across 2 physical hosts |
| Virtual nodes | 33 (1 k3s + 30 pawns + 1 primary + 1 pawn) |
| Stress test requests | 2,502,358 (0% failure) |
| Throughput | 11,913 rps sustained |
| Median latency | 257 us |
| p99 latency | 10 ms |
| Perigeos RSS (engix99) | 365 MiB for 1,660 pods (~220 KB/pod) |
| Perigeos RSS (engifire) | 89 MiB for 112 pods (~795 KB/pod) |
| Container runtime | systemd-nspawn |
| CNI | Constellation (eBPF datapath, VXLAN tunnel) |
| Cross-host link | Direct Ethernet, 0.15 ms RTT |

## What's Under the Hood

- **Runtime:** systemd-nspawn machines, one per container
- **Networking:** Constellation CNI (Cilium fork) - VXLAN + BPF datapath, per-pod netns
- **Images:** OCI pulls with pod-level dedup cache + peer blob serving over TLS
- **TLS:** Auto-renewing certificates via k8s CSR API with SAN-aware hot reload
- **Rollout:** `apsis rollout` - stepped scaling with per-step readiness gates
- **Multi-host:** NodeIP config routes tunnel traffic over dedicated ethernet, not WiFi
