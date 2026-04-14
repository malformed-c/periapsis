# ADR-0008: Apogeos Operator as Fleet Journal Sink

**Status:** Proposed  
**Date:** 2026-03-23

## Context

Perigeos runs all pod workloads as systemd-nspawn machines, so container stdout/stderr lands in the host's systemd journal via `SyslogIdentifier`. Logs are available locally via `journalctl` with no per-pod log file management required.

In a multi-host fleet, logs are siloed per host. Observing logs across the fleet requires SSH-ing into each host or deploying a separate logging stack (Loki, Fluentd, Elasticsearch). This is operational overhead that contradicts the platform's goal of consolidating observability into the control plane.

The Apogeos operator already has fleet-wide visibility - it watches all `PerigeosHost` objects, tracks pawn state, and owns the Constellation config. It is the natural aggregation point for host telemetry.

## Decision

Extend the Apogeos operator to act as a **journal sink** for all hosts in the fleet. Each perigeos host forwards structured journal entries to the operator over a persistent connection. The operator aggregates, indexes, and exposes logs fleet-wide.

### Transport

Each perigeos host opens a persistent gRPC stream to the Apogeos operator (reusing the existing registration connection from ADR-0004). Journal entries are forwarded as structured protobuf messages. Backpressure is handled by the gRPC stream - if the operator is slow, the host buffers up to a configurable limit before dropping oldest entries.

### Entry format

Each forwarded entry includes:

- `host` - physical host name (e.g. `engix99`)
- `pawn` - pawn name (e.g. `compute-07`), empty for host-level entries
- `pod_uid`, `pod_name`, `pod_namespace` - from journal fields set by perigeos at machine start
- `container` - container name within the pod
- `message` - log line
- `timestamp` - journal realtime timestamp (microsecond precision)
- `priority` - syslog priority (0–7)
- `cursor` - journal cursor for resumption after reconnect

### Resumption

On reconnect, the host sends its last acknowledged cursor. The operator responds with the cursor it last received from that host. The host resumes forwarding from `max(local_last_sent, operator_last_acked)` to avoid gaps and minimize duplication.

### Operator storage

The operator maintains a rolling in-memory ring buffer per host (configurable size, default 100k entries). A persistent backend (SQLite or an external sink like Loki) is optional and pluggable. Without a persistent backend, logs survive operator restarts only if hosts replay from their journal cursors on reconnect.

### Query API

The operator exposes a query endpoint (gRPC + HTTP/JSON) supporting:

- Filter by host, pawn, pod name, pod namespace, container
- Filter by time range (from/to)
- Filter by priority
- Tail mode (streaming)

`apsis logs` wraps this endpoint, providing a `kubectl logs`-compatible interface across the fleet:

```
$ apsis logs -n default deploy/myapp
$ apsis logs --pawn compute-07 --since 10m
$ apsis logs --host engix99 --priority error
```

### Integration with kubectl logs

The existing `GetLogStream` implementation in perigeos streams from the local journal. This stays intact - `kubectl logs` continues to work per-pod via the node API. The Apogeos journal sink is additive, not a replacement.

## Why not a standard logging stack

- Loki/Fluentd/Elasticsearch require separate deployment, configuration, and maintenance
- Per-node log collectors (Promtail, Fluentbit) need DaemonSets - with the primary/pawn split, these only run on primaries and miss pawn workload logs
- The journal already provides structured metadata (unit name, invocation ID, cgroup path) that standard log shippers partially lose
- The operator already has an authenticated connection to each host; no new network paths or credentials required

## Consequences

- Fleet-wide log access without deploying a logging stack
- `apsis logs` becomes the primary debugging tool for cross-host issues
- The operator becomes stateful in a new dimension - log buffer memory scales with fleet size and log volume
- Persistent storage is optional but recommended for production; without it logs are lost on operator restart unless hosts replay
- Journal cursor tracking adds a small amount of per-host state to the operator
- High-volume logging workloads may require tuning buffer sizes and considering an external sink
- `kubectl logs` continues to work unchanged via the existing node API path
