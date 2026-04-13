# Tracing Pod Lifecycle in Perigeos Logs

Perigeos uses structured logging (`slog` with `tint` for color). Every log line
includes key-value pairs that let you filter and correlate events across the
pod lifecycle.

## Key Fields

| Field | Source | Example |
|-------|--------|---------|
| `pod` | node layer | `test-security` |
| `pawn` | node layer | `compute-00` |
| `namespace` | node layer | `default` |
| `container` | node + runtime | `alpine` |
| `machine` | runtime layer | `default_test-security_alpine` |
| `service` | runtime layer | `peri-<uid>-<container>.service` |
| `op` | runtime layer | `userns-setup` |
| `ip` | node layer | `10.42.1.5` |

## Filtering by Pod

To follow a single pod from creation to teardown:

```bash
journalctl -u perigeos -o cat | grep 'pod=test-security'
```

Or with `jq` if using JSON output:

```bash
journalctl -u perigeos -o json | jq -r 'select(.MESSAGE | contains("test-security"))'
```

## Lifecycle Phases

### 1. Admission and Scheduling

```
INF CreatePod pawn=compute-00 namespace=default pod=test-security
INF Starting pod creation process
```

### 2. Network Setup

```
INF CNI network configured, podIP=10.42.1.5
```

On failure: `WRN CNI setup failed: ...`

### 3. Image Pull

```
INF Pulling image alpine for container alpine
INF Pulled image alpine
```

Or if cached: `INF Image alpine already present`

### 4. Overlay Mount

```
INF Mounted overlay for container alpine
```

### 5. User Identity and Userns

When `runAsUser` is set:
```
INF Container alpine: running as uid 1000 via userns shim
```

The runtime layer logs the userns handshake with `op=userns-setup`:
```
INF Shim signaled ready, writing uid_map/gid_map  machine=default_test-security_alpine op=userns-setup
INF Wrote userns mappings  uidbase=131072 pid=12345 machine=... op=userns-setup
INF Userns setup complete  targetUID=1000 targetGID=1000 machine=... op=userns-setup
```

### 6. Container Start

```
INF Starting Machine  service=peri-<uid>-alpine.service slice=peri-<uid>.slice
INF Created container alpine
```

### 7. Running (via BatchWatcher)

The batchwatcher detects `substate=running` from D-Bus and promotes the pod:
```
DBG D-Bus unit event  unit=peri-<uid>-alpine.service substate=running
```

### 8. Teardown

```
INF Executing idempotent teardown for pod  pod=test-security
INF Stopping Machine  pod=<uid> container=alpine
INF Stopped container alpine
INF CNI network released for pod test-security
```

## Correlating Events with K8s

Pod events are also emitted as Kubernetes events. View them with:

```bash
kubectl --kubeconfig /etc/rancher/k3s/k3s.yaml describe pod test-security
```

Events include: `Starting`, `Pulling`, `Pulled`, `Cached`, `Mounted`,
`NetworkReady`, `UserIdentity`, `SandboxReady`, `Created`, `Started`,
`Killing`, `Killed`, `NetworkTeardown`.

## CrashLoopBackOff Traces

When a container fails, the batchwatcher handles restarts:

```
INF Restarting container (CrashLoopBackOff)  pod=test-security container=alpine
INF Container restarted successfully  pod=test-security container=alpine
```

The restart count is tracked per container and visible in `kubectl get pods`.

## Userns-Specific Debugging

If the userns handshake stalls (shim never signals ready):

```bash
# Check if the shim is running inside the container
machinectl shell default_test-security_alpine /bin/ps aux

# Check FIFO state
ls -la /run/perigeos/userns/<uid>-alpine/
```

If userns falls back to `--user=`:

```
ERR Failed to setup userns FIFOs, falling back to --user=  error=...
```
