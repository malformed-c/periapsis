# Perigeos at Scale: 2000 Pods, 30 Virtual Nodes, Single Host

One box. 28 cores, 48 GB RAM. 30 virtual Kubernetes nodes running as a single
perigeos process. 2000 pods, all Running, all Ready.

No Docker. No containerd. Just systemd-nspawn and a Linux kernel.

---

## Hardware

```
Intel Xeon E5-2690 v4 @ 2.60GHz (28 cores)
48 GB RAM, 12 GB swap
932 GB NVMe (Arch Linux, kernel 6.19.9)
```

## Cluster

31 Kubernetes nodes on a single machine — 1 real k3s control plane + 30 perigeos
virtual nodes (pawns), each appearing as a full schedulable node:

```
$ kubectl get nodes
NAME         STATUS   ROLES                   VERSION          CONTAINER-RUNTIME
engix99      Ready    control-plane,primary   v1.35.2+k3s1     containerd://2.1.5-k3s1
compute-00   Ready    pawn                    perigeos://dev   systemd://260-1-arch
compute-01   Ready    pawn                    perigeos://dev   systemd://260-1-arch
...
compute-29   Ready    pawn                    perigeos://dev   systemd://260-1-arch
```

## Rollout: 0 to 2000 in under 6 minutes

Using stepped rollout (50 pods per step) via the `apsis` CLI:

```
$ apsis rollout --deployment scale-test --replicas 2000 --step 50 --timeout 180s
Deployment:  default/scale-test
Current:     0 replica(s)
Target:      2000 replica(s)

── Step 1/40: scaling 0 → 50 ──
  ready=0 updated=0 available=0 perigeos=0 / 50 desired
  ready=50 updated=50 available=50 perigeos=50 / 50 desired
  ✓ 50/50 replicas ready
...
── Step 40/40: scaling 1950 → 2000 ──
  ready=2000 updated=2000 available=2000 perigeos=2000 / 2000 desired
  ✓ 2000/2000 replicas ready

✓ Rollout complete: default/scale-test is at 2000 replica(s).
```

Sustained throughput: ~5.5 pods/second across 30 pawns.

## All 2000 Running

```
$ kubectl get deployment scale-test
NAME         READY       UP-TO-DATE   AVAILABLE   AGE
scale-test   2000/2000   2000         2000        72m

$ kubectl get pods --field-selector=status.phase=Running --no-headers | wc -l
2001
```

## Even Distribution

~67 pods per pawn, balanced by the kube-scheduler across all 30 virtual nodes:

```
$ kubectl get pods -o wide --no-headers | awk '{print $7}' | sort | uniq -c | sort -rn
     68 compute-01
     67 compute-29
     67 compute-28
     67 compute-27
     ...
     66 compute-04
```

## Pod Operations Work

### exec

```
$ kubectl exec scale-test-7d48c89997-225b5 -- ps aux
PID   USER     TIME  COMMAND
    1 root      0:00 /bin/sh -c while true; do sleep 3600; done
    2 root      0:00 sleep 3600
    4 root      0:00 ps aux
```

### Networking

Each pod gets a unique IP via Constellation CNI:

```
$ kubectl get pods -l app=scale-test -o wide --no-headers | head -5
scale-test-7d48c89997-225b5   1/1   Running   0   7m   10.0.126.247   compute-07
scale-test-7d48c89997-24dnb   1/1   Running   0  12m   10.0.190.57    compute-11
scale-test-7d48c89997-24xwj   1/1   Running   0   9m   10.0.254.67    compute-15
scale-test-7d48c89997-25cb5   1/1   Running   0   9m   10.1.213.204   compute-29
scale-test-7d48c89997-25mtg   1/1   Running   0  11m   10.0.102.143   compute-06
```

### Pod-to-pod connectivity

Cross-pawn ping — pod on compute-07 reaching pod on compute-27, sub-millisecond:

```
$ kubectl exec scale-test-7d48c89997-225b5 -- ping -c 3 10.0.221.45
PING 10.0.221.45 (10.0.221.45): 56 data bytes
64 bytes from 10.0.221.45: seq=0 ttl=63 time=0.083 ms
64 bytes from 10.0.221.45: seq=1 ttl=63 time=0.082 ms
64 bytes from 10.0.221.45: seq=2 ttl=63 time=0.115 ms

--- 10.0.221.45 ping statistics ---
3 packets transmitted, 3 packets received, 0% packet loss
round-trip min/avg/max = 0.082/0.093/0.115 ms
```

## Runtime Status

```
$ apsis status
Hostname:    engix99
Version:     dev
Uptime:      20m26s
Pawns:       30
Pods:        2013
Kernel:      6.19.9-arch1-1
Arch:        linux/amd64
Memory:      18362 / 48004 MiB
CPU cores:   28
Load avg:    9.37 15.83 12.14

Machines:    2013
Disk dirs:   2013
Units:       2013
RSS:         263 MiB
LXC veths:   2016
Netns:       2015
```

## Health Check: All Sources Agree

`apsis doctor` cross-references three independent sources of truth per pawn:
gambit (in-memory), systemd (running units), and disk (overlay dirs). All match.

```
$ apsis doctor
Status:  HEALTHY
Sources: gambit=2013  systemd=2013  disk=2013
Network: lxc_veths=2016  netns=2015

── compute-00 ──
  gambit=67  systemd=67  disk=67  OK
── compute-01 ──
  gambit=68  systemd=68  disk=68  OK
...
── compute-29 ──
  gambit=67  systemd=67  disk=67  OK
```

## Resource Usage

### Perigeos process

```
$ ps -p $(pgrep -x perigeos) -o rss,vsz,pcpu,pmem --no-headers
278824 6753216 22.9 0.5

RSS: 263 MiB for 2000+ pods — ~130 KB per pod.
```

### Cgroups

```
$ systemctl status perigeos.slice
● perigeos.slice
     Active: active
     Tasks: 6160
     Memory: 4.1G (peak: 4.3G)
     CPU: 6min 56s

$ systemctl status perigeos-compute-00.slice
● perigeos-compute-00.slice — Perigeos Pawn: compute-00
     Tasks: 201
     Memory: 137.2M (max: 488.2M, available: 351M, peak: 143.8M)
     CPU: 11.760s
```

### System

```
$ free -h
              total   used   available
Mem:           46Gi   17Gi      28Gi
Swap:          11Gi     0B      11Gi

Host memory with 2000 pods: 17 GB used, 28 GB available.
```

## The Numbers

| Metric | Value |
|--------|-------|
| Pods | 2000, all Running, all Ready |
| Virtual Nodes | 30 (compute-00 through compute-29) |
| Distribution | ~67 pods per pawn |
| Rollout speed | ~5.5 pods/sec sustained |
| Perigeos RSS | 263 MiB (~130 KB per pod) |
| Total slice memory | 4.1 GB for all containers |
| Per-pawn memory | ~137 MiB (67 pods) |
| Host memory free | 28 GB of 48 GB |
| Container runtime | systemd-nspawn (no Docker, no containerd) |
| CNI | Constellation (eBPF, per-pod netns) |
| System | 28-core Xeon E5-2690 v4, Arch Linux, kernel 6.19.9 |
