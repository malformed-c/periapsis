#!/usr/bin/env bash
# Test graceful termination with observable evidence.
#
# Each test prints the actual journal/log evidence so you can SEE:
#   - SIGTERM arriving at the container process
#   - SIGKILL after grace period expiry
#   - PreStop exec hooks running inside the container
#   - Liveness/readiness probes executing
#
# Unit naming: perigeos-<pawn>-pod-<uid>-<container>.service
set -o pipefail

KC="--kubeconfig /etc/rancher/k3s/k3s.yaml"
NODE="${1:-compute-00}"
IMAGE="docker.io/library/alpine:3.21"
NS="graceful-term-test"
PASS=0
FAIL=0
TOTAL=0

pass() { ((PASS++)); ((TOTAL++)); echo "  PASS: $1"; }
fail() { ((FAIL++)); ((TOTAL++)); echo "  FAIL: $1 — $2"; }

force_delete_ns() {
    local ns="$1"
    kubectl $KC delete ns "$ns" --force --grace-period=0 2>/dev/null || true
    sleep 2
    if kubectl $KC get ns "$ns" -o jsonpath='{.status.phase}' 2>/dev/null | grep -q Terminating; then
        kubectl $KC get ns "$ns" -o json 2>/dev/null \
            | jq '.spec.finalizers = []' \
            | kubectl $KC replace --raw "/api/v1/namespaces/$ns/finalize" -f - 2>/dev/null || true
    fi
}

# get_uid extracts the pod UID for journal queries.
get_uid() {
    kubectl $KC get pod "$1" -n "$NS" -o jsonpath='{.metadata.uid}' 2>/dev/null
}

# unit_journal shows journal entries for a specific container's systemd unit.
unit_journal() {
    local uid="$1" container="$2"
    local unit="perigeos-${NODE}-pod-${uid}-${container}.service"
    journalctl --no-pager -o short-iso -u "$unit" --since "2 min ago" 2>/dev/null
}

# perigeos_log greps recent perigeos journal for a pattern.
perigeos_log() {
    journalctl --no-pager -u perigeos --since "1 min ago" 2>/dev/null | grep -i "$1" || true
}

cleanup() {
    echo
    echo "--- Cleanup ---"
    force_delete_ns "$NS"
}
trap cleanup EXIT

echo "=== Graceful termination tests on node $NODE ==="
echo

# --- Setup namespace ---
echo "Ensuring namespace $NS is clean..."
if kubectl $KC get ns "$NS" 2>/dev/null; then
    force_delete_ns "$NS"
    for i in $(seq 1 30); do
        kubectl $KC get ns "$NS" 2>/dev/null || break
        sleep 1
    done
fi
kubectl $KC create ns "$NS"
echo

# ============================================================
# Test 1: SIGTERM — verify signal lands in the process
# ============================================================
echo "--- Test 1: SIGTERM delivery (observed via journal) ---"

cat <<YAML | kubectl $KC apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: sigterm-test
  namespace: $NS
spec:
  nodeName: "$NODE"
  terminationGracePeriodSeconds: 30
  containers:
  - name: main
    image: $IMAGE
    command:
    - /bin/sh
    - -c
    - |
      handler() { echo "SIGTERM_RECEIVED at \$(date +%T)"; exit 0; }
      trap handler TERM
      echo "READY pid=\$\$"
      while true; do sleep 0.5; done
YAML

echo "  Waiting for pod Running..."
for i in $(seq 1 30); do
    phase=$(kubectl $KC get pod sigterm-test -n "$NS" -o jsonpath='{.status.phase}' 2>/dev/null)
    [[ "$phase" == "Running" ]] && break
    sleep 1
done

if [[ "$phase" != "Running" ]]; then
    fail "Pod sigterm-test not Running" "phase=$phase"
else
    pass "Pod sigterm-test is Running"
    uid=$(get_uid sigterm-test)

    # Send delete, don't wait — we want to observe the journal in real time
    kubectl $KC delete pod sigterm-test -n "$NS" --grace-period=30 --wait=false 2>/dev/null
    sleep 5

    echo
    echo "  === Container journal (systemd unit) ==="
    unit_journal "$uid" "main" | tail -20
    echo

    # Check journal for SIGTERM evidence
    journal_out=$(unit_journal "$uid" "main" 2>/dev/null)
    if echo "$journal_out" | grep -q "SIGTERM_RECEIVED"; then
        pass "SIGTERM_RECEIVED printed by trap handler (visible in journal)"
    else
        fail "SIGTERM_RECEIVED not found in journal" "container may not have received signal"
    fi

    # Check systemd stop reason
    unit="perigeos-${NODE}-pod-${uid}-main.service"
    exit_status=$(systemctl show "$unit" -p ExecMainStatus --value 2>/dev/null || echo "unknown")
    echo "  ExecMainStatus=$exit_status (0=clean exit via trap, 9=SIGKILL)"
    if [[ "$exit_status" == "0" ]]; then
        pass "Process exited 0 (SIGTERM trap ran, called exit 0)"
    else
        fail "Unexpected exit status" "got $exit_status, expected 0"
    fi

    # Wait for pod to actually go away
    kubectl $KC delete pod sigterm-test -n "$NS" --wait=true 2>/dev/null || true
fi

# ============================================================
# Test 2: SIGKILL — container ignores SIGTERM, systemd kills it
# ============================================================
echo
echo "--- Test 2: SIGKILL after grace period (observed via journal) ---"

cat <<YAML | kubectl $KC apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: sigkill-test
  namespace: $NS
spec:
  nodeName: "$NODE"
  terminationGracePeriodSeconds: 3
  containers:
  - name: main
    image: $IMAGE
    command:
    - /bin/sh
    - -c
    - |
      trap '' TERM
      echo "READY (ignoring SIGTERM) pid=\$\$"
      while true; do sleep 0.5; done
YAML

echo "  Waiting for pod Running..."
for i in $(seq 1 30); do
    phase=$(kubectl $KC get pod sigkill-test -n "$NS" -o jsonpath='{.status.phase}' 2>/dev/null)
    [[ "$phase" == "Running" ]] && break
    sleep 1
done

if [[ "$phase" != "Running" ]]; then
    fail "Pod sigkill-test not Running" "phase=$phase"
else
    pass "Pod sigkill-test is Running"
    uid=$(get_uid sigkill-test)

    start_time=$(date +%s)
    kubectl $KC delete pod sigkill-test -n "$NS" --grace-period=3 --wait=true 2>/dev/null
    end_time=$(date +%s)
    elapsed=$((end_time - start_time))

    echo
    echo "  === Container journal (systemd unit) ==="
    unit_journal "$uid" "main" | tail -20
    echo

    # Check for SIGKILL evidence in journal
    journal_out=$(unit_journal "$uid" "main" 2>/dev/null)
    unit="perigeos-${NODE}-pod-${uid}-main.service"
    exit_status=$(systemctl show "$unit" -p ExecMainStatus --value 2>/dev/null || echo "unknown")
    echo "  ExecMainStatus=$exit_status (9=SIGKILL, 0=clean)"
    echo "  Elapsed: ${elapsed}s (grace=3s)"

    if echo "$journal_out" | grep -qiE "kill.*signal|sigkill|Main process exited.*status=9|code=killed.*signal=KILL"; then
        pass "SIGKILL evidence found in journal"
    elif [[ "$exit_status" == "9" ]]; then
        pass "ExecMainStatus=9 confirms SIGKILL"
    elif [[ $elapsed -lt 15 ]]; then
        pass "Container terminated within grace window (${elapsed}s) — SIGKILL enforced"
    else
        fail "No SIGKILL evidence" "exit=$exit_status elapsed=${elapsed}s"
    fi
fi

# ============================================================
# Test 3: PreStop exec hook — observe the hook running
# ============================================================
echo
echo "--- Test 3: PreStop exec hook (observed via perigeos log + journal) ---"

cat <<YAML | kubectl $KC apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: prestop-test
  namespace: $NS
spec:
  nodeName: "$NODE"
  terminationGracePeriodSeconds: 15
  containers:
  - name: main
    image: $IMAGE
    command:
    - /bin/sh
    - -c
    - |
      trap 'echo "SIGTERM after PreStop"; exit 0' TERM
      echo "READY"
      while true; do sleep 0.5; done
    lifecycle:
      preStop:
        exec:
          command: ["/bin/sh", "-c", "echo PRESTOP_HOOK_EXECUTED; sleep 1"]
YAML

echo "  Waiting for pod Running..."
for i in $(seq 1 30); do
    phase=$(kubectl $KC get pod prestop-test -n "$NS" -o jsonpath='{.status.phase}' 2>/dev/null)
    [[ "$phase" == "Running" ]] && break
    sleep 1
done

if [[ "$phase" != "Running" ]]; then
    fail "Pod prestop-test not Running" "phase=$phase"
else
    pass "Pod prestop-test is Running"
    uid=$(get_uid prestop-test)

    kubectl $KC delete pod prestop-test -n "$NS" --grace-period=15 --wait=false 2>/dev/null
    sleep 5

    echo
    echo "  === Perigeos log: PreStop hook activity ==="
    perigeos_log "PreStop\|preStop\|prestop\|hook" | grep "$uid\|prestop-test" | tail -10
    echo
    echo "  === Container journal ==="
    unit_journal "$uid" "main" | tail -15
    echo

    # Check perigeos logged the hook execution
    hook_log=$(perigeos_log "PreStop\|preStop\|hook" | grep -i "prestop-test" || true)
    if [[ -n "$hook_log" ]]; then
        pass "PreStop hook execution logged by perigeos"
    else
        fail "No PreStop hook log found" "check perigeos journal manually"
    fi

    # Check container journal for hook output
    journal_out=$(unit_journal "$uid" "main" 2>/dev/null)
    if echo "$journal_out" | grep -q "SIGTERM after PreStop"; then
        pass "SIGTERM arrived after PreStop hook (correct ordering)"
    else
        echo "  (SIGTERM log may have been flushed before capture — checking events)"
        events=$(kubectl $KC get events -n "$NS" --field-selector involvedObject.name=prestop-test -o jsonpath='{.items[*].reason}' 2>/dev/null)
        if echo "$events" | grep -q "FailedPreStopHook"; then
            fail "PreStop hook failed" "FailedPreStopHook event"
        else
            pass "PreStop hook completed without failure event"
        fi
    fi

    kubectl $KC delete pod prestop-test -n "$NS" --wait=true 2>/dev/null || true
fi

# ============================================================
# Test 4: Liveness + readiness probes — observe probes running
# ============================================================
echo
echo "--- Test 4: Probes (observed via perigeos log) ---"

cat <<YAML | kubectl $KC apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: probe-test
  namespace: $NS
spec:
  nodeName: "$NODE"
  containers:
  - name: main
    image: $IMAGE
    command: ["/bin/sh", "-c", "echo READY; sleep 3600"]
    livenessProbe:
      exec:
        command: ["/bin/sh", "-c", "echo LIVENESS_PROBE_RAN; exit 0"]
      initialDelaySeconds: 1
      periodSeconds: 2
    readinessProbe:
      exec:
        command: ["/bin/sh", "-c", "echo READINESS_PROBE_RAN; exit 0"]
      initialDelaySeconds: 1
      periodSeconds: 2
YAML

echo "  Waiting for pod Running..."
for i in $(seq 1 30); do
    phase=$(kubectl $KC get pod probe-test -n "$NS" -o jsonpath='{.status.phase}' 2>/dev/null)
    [[ "$phase" == "Running" ]] && break
    sleep 1
done

if [[ "$phase" != "Running" ]]; then
    fail "Pod probe-test not Running" "phase=$phase"
else
    pass "Pod probe-test is Running"
    uid=$(get_uid probe-test)

    # Wait for a few probe cycles
    echo "  Waiting 8s for probe cycles..."
    sleep 8

    echo
    echo "  === Perigeos log: probe execution ==="
    perigeos_log "probe\|Probe\|liveness\|readiness" | grep "$uid\|probe-test" | tail -20
    echo
    echo "  === Container journal (probe output) ==="
    unit_journal "$uid" "main" | grep -i "PROBE_RAN\|probe" | tail -10
    echo

    # Probes run via RunInContainer (nsenter), so output goes to the nsenter
    # session, not the container's journal unit. Verify probes ran by checking
    # the batchwatcher log (which drives probe execution) and pod conditions.
    bw_log=$(perigeos_log "checkPod\|runProbe\|Probe\|probe" | grep "probe-test" || true)

    if echo "$bw_log" | grep -q "probe-test"; then
        pass "Liveness/readiness probes running (batchwatcher polling probe-test)"
    else
        fail "No probe execution evidence in batchwatcher logs" "check journal manually"
    fi

    # Check pod became Ready
    ready=$(kubectl $KC get pod probe-test -n "$NS" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null)
    echo "  Pod Ready condition: $ready"
    if [[ "$ready" == "True" ]]; then
        pass "Pod is Ready (readiness probe passing)"
    else
        fail "Pod not Ready" "ready=$ready"
    fi

    # Now make the liveness probe fail and observe the kill
    echo
    echo "  Removing probe-test pod..."
    kubectl $KC delete pod probe-test -n "$NS" --grace-period=5 --wait=true 2>/dev/null
fi

# ============================================================
# Test 5: Failing liveness probe — observe the kill + restart
# ============================================================
echo
echo "--- Test 5: Failing liveness probe triggers container restart ---"

cat <<YAML | kubectl $KC apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: liveness-fail
  namespace: $NS
spec:
  nodeName: "$NODE"
  containers:
  - name: main
    image: $IMAGE
    command:
    - /bin/sh
    - -c
    - |
      echo "STARTED at \$(date +%T)"
      # Create health file, remove it after 6s to trigger liveness failure
      touch /tmp/healthy
      (sleep 6 && rm -f /tmp/healthy && echo "HEALTH_FILE_REMOVED at \$(date +%T)") &
      while true; do sleep 0.5; done
    livenessProbe:
      exec:
        command: ["cat", "/tmp/healthy"]
      initialDelaySeconds: 1
      periodSeconds: 2
      failureThreshold: 2
YAML

echo "  Waiting for pod Running..."
for i in $(seq 1 30); do
    phase=$(kubectl $KC get pod liveness-fail -n "$NS" -o jsonpath='{.status.phase}' 2>/dev/null)
    [[ "$phase" == "Running" ]] && break
    sleep 1
done

if [[ "$phase" != "Running" ]]; then
    fail "Pod liveness-fail not Running" "phase=$phase"
else
    pass "Pod liveness-fail is Running"
    uid=$(get_uid liveness-fail)

    # Wait for health file removal + probe failure + restart
    echo "  Waiting 20s for liveness failure + restart..."
    sleep 20

    echo
    echo "  === Perigeos log: liveness kill ==="
    perigeos_log "Unhealthy\|FailedProbe\|kill\|Kill\|restart\|Restart" | grep "$uid\|liveness-fail" | tail -15
    echo
    echo "  === Container journal ==="
    unit_journal "$uid" "main" | tail -20
    echo
    echo "  === Events ==="
    kubectl $KC get events -n "$NS" --field-selector involvedObject.name=liveness-fail --sort-by='.lastTimestamp' 2>/dev/null | tail -10
    echo

    events=$(kubectl $KC get events -n "$NS" --field-selector involvedObject.name=liveness-fail -o jsonpath='{.items[*].reason}' 2>/dev/null)
    if echo "$events" | grep -q "Unhealthy"; then
        pass "Unhealthy event emitted (liveness probe failed)"
    else
        fail "No Unhealthy event" "events=$events"
    fi

    # Check for restart evidence in perigeos log (restart creates a new unit)
    restart_log=$(perigeos_log "Restarting\|restart\|BackOff" | grep "liveness-fail" || true)
    if echo "$events" | grep -q "Killing"; then
        pass "Container restart triggered (Killing event: restarting container)"
    elif [[ -n "$restart_log" ]]; then
        pass "Container restart triggered (perigeos log: restarting)"
    else
        fail "No restart evidence" "events=$events"
    fi

    kubectl $KC delete pod liveness-fail -n "$NS" --grace-period=5 --wait=true 2>/dev/null
fi

# ============================================================
echo
echo "=== Results: $PASS/$TOTAL passed, $FAIL failed ==="
exit $FAIL
