# Perigeos at Scale: 3000 Pods, 30 Virtual Nodes, Single Host

One box. 28 cores, 48 GB RAM. 30 virtual Kubernetes nodes running as a single
perigeos process. 3000 pods, all Running, all Ready.

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

## All 3000 Running

```
$ kubectl get deployment scale-test
NAME         READY       UP-TO-DATE   AVAILABLE   AGE
scale-test   3000/3000   3000         3000        102m

$ kubectl get pods --field-selector=status.phase=Running --no-headers | wc -l
3001
```

## Even Distribution

100 pods per pawn, balanced by the kube-scheduler across all 30 virtual nodes:

```
$ kubectl get pods -o wide --no-headers | awk '{print $7}' | sort | uniq -c | sort -rn
    101 compute-01
    100 compute-29
    100 compute-28
    100 compute-27
    ...
    100 compute-00
```

## Pod Operations Work

### exec

```
$ kubectl exec scale-test-7d48c89997-225b5 -- ps aux
PID   USER     TIME  COMMAND
    1 root      0:00 /bin/sh -c while true; do sleep 3600; done
    2 root      0:00 sleep 3600
    6 root      0:00 ps aux
```

### Networking

Each pod gets a unique IP via Constellation CNI:

```
$ kubectl get pods -l app=scale-test -o wide --no-headers | head -5
scale-test-7d48c89997-225b5   1/1   Running   0   38m   10.0.126.247   compute-07
scale-test-7d48c89997-24d5j   1/1   Running   0   18m   10.1.21.95     compute-17
scale-test-7d48c89997-24dnb   1/1   Running   0   42m   10.0.190.57    compute-11
scale-test-7d48c89997-24ll5   1/1   Running   0   20m   10.1.42.101    compute-18
scale-test-7d48c89997-24xwj   1/1   Running   0   39m   10.0.254.67    compute-15
```

### Pod-to-pod connectivity

Cross-pawn ping — pod on compute-07 reaching pod on compute-27, sub-millisecond:

```
$ kubectl exec scale-test-7d48c89997-225b5 -- ping -c 3 10.1.180.167
PING 10.1.180.167 (10.1.180.167): 56 data bytes
64 bytes from 10.1.180.167: seq=0 ttl=63 time=0.081 ms
64 bytes from 10.1.180.167: seq=1 ttl=63 time=0.075 ms
64 bytes from 10.1.180.167: seq=2 ttl=63 time=0.140 ms

--- 10.1.180.167 ping statistics ---
3 packets transmitted, 3 packets received, 0% packet loss
round-trip min/avg/max = 0.075/0.098/0.140 ms
```

## Runtime Status

```
$ apsis status
Hostname:    engix99
Version:     dev
Uptime:      50m46s
Pawns:       30
Pods:        3013
Kernel:      6.19.9-arch1-1
Arch:        linux/amd64
Go:          go1.26.1-X:nodwarf5
Memory:      25931 / 48004 MiB
CPU cores:   28
Load avg:    8.23 10.59 14.85

Machines:    3013
Disk dirs:   3013
Units:       3013
RSS:         351 MiB
LXC veths:   3016
Netns:       3015
```

## Health Check: All Sources Agree

`apsis doctor` cross-references three independent sources of truth per pawn:
gambit (in-memory), systemd (running units), and disk (overlay dirs). All match.

```
$ apsis doctor
Status:  HEALTHY
Sources: gambit=3013  systemd=3013  disk=3013
Network: lxc_veths=3016  netns=3015

── compute-00 ──
  gambit=100  systemd=100  disk=100  OK
── compute-01 ──
  gambit=101  systemd=101  disk=101  OK
...
── compute-29 ──
  gambit=100  systemd=100  disk=100  OK
```

## Resource Usage

### Perigeos process

```
$ ps -p $(pgrep -x perigeos) -o rss,vsz,pcpu,pmem --no-headers
377128 6822940 33.8 0.7

RSS: 351 MiB for 3000+ pods — ~117 KB per pod.
```

### Cgroups

```
$ systemctl status perigeos.slice
● perigeos.slice
     Active: active
     Tasks: 9160
     Memory: 6.2G (peak: 6.3G)
     CPU: 19min 44s

$ systemctl status perigeos-compute-00.slice
● perigeos-compute-00.slice — Perigeos Pawn: compute-00
     Tasks: 300
     Memory: ~207 MiB (100 pods)
```

### System

```
$ free -h
              total   used   available
Mem:           46Gi   25Gi      21Gi
Swap:          11Gi     0B      11Gi

Host memory with 3000 pods: 25 GB used, 21 GB available.
```

## The Numbers

| Metric | Value |
|--------|-------|
| Pods | 3000, all Running, all Ready |
| Virtual Nodes | 30 (compute-00 through compute-29) |
| Distribution | 100 pods per pawn |
| Perigeos RSS | 351 MiB (~117 KB per pod) |
| Total slice memory | 6.2 GB for all containers |
| Per-pawn memory | ~207 MiB (100 pods) |
| Host memory free | 21 GB of 48 GB |
| Container runtime | systemd-nspawn (no Docker, no containerd) |
| CNI | Constellation (eBPF, per-pod netns) |
| System | 28-core Xeon E5-2690 v4, Arch Linux, kernel 6.19.9 |
