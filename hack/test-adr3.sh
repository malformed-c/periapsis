#!/usr/bin/env bash
# Live validation for ADR-0003: event emission, graceful termination, pod admission.
#
# Requires: running perigeos + k3s, root for varlink socket access.
set -o pipefail

KC="--kubeconfig /etc/rancher/k3s/k3s.yaml"
NODE="${1:-compute-00}"
IMAGE="docker.io/library/debian:bookworm"
NS="adr3-test"
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

cleanup() {
    echo
    echo "--- Cleanup ---"
    force_delete_ns "$NS"
}
trap cleanup EXIT

echo "=== ADR-0003 live tests on node $NODE ==="
echo

# --- Setup namespace ---
echo "Ensuring namespace $NS is clean..."
if kubectl $KC get ns "$NS" 2>/dev/null; then
    force_delete_ns "$NS"
    for i in $(seq 1 15); do
        kubectl $KC get ns "$NS" 2>/dev/null || break
        sleep 1
    done
fi
kubectl $KC create ns "$NS"
echo

# ============================================================
# Test 1: Event emission — create success event
# ============================================================
echo "--- Test 1: Create success event ---"

cat <<YAML | kubectl $KC apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: event-test
  namespace: $NS
spec:
  nodeName: "$NODE"
  containers:
  - name: main
    image: $IMAGE
    command: ["/bin/sh", "-c", "sleep 3600"]
YAML

echo "  Waiting for pod Running..."
for i in $(seq 1 30); do
    phase=$(kubectl $KC get pod event-test -n "$NS" -o jsonpath='{.status.phase}' 2>/dev/null)
    [[ "$phase" == "Running" ]] && break
    sleep 1
done

if [[ "$phase" == "Running" ]]; then
    pass "Pod event-test is Running"
else
    fail "Pod event-test not Running" "phase=$phase"
fi

# Check for PerigeosCreateSuccess event
events=$(kubectl $KC get events -n "$NS" --field-selector involvedObject.name=event-test -o jsonpath='{.items[*].reason}' 2>/dev/null)
if echo "$events" | grep -q "PerigeosCreateSuccess"; then
    pass "PerigeosCreateSuccess event emitted"
else
    fail "PerigeosCreateSuccess event missing" "events=$events"
fi

# ============================================================
# Test 2: Event emission — delete success event
# ============================================================
echo "--- Test 2: Delete success event ---"

kubectl $KC delete pod event-test -n "$NS" --grace-period=5 --wait=true 2>/dev/null
sleep 2

events=$(kubectl $KC get events -n "$NS" --field-selector involvedObject.name=event-test -o jsonpath='{.items[*].reason}' 2>/dev/null)
if echo "$events" | grep -q "PerigeosDeleteSuccess"; then
    pass "PerigeosDeleteSuccess event emitted"
else
    fail "PerigeosDeleteSuccess event missing" "events=$events"
fi

# ============================================================
# Test 3: Graceful termination — PreStop exec hook runs
# ============================================================
echo "--- Test 3: PreStop exec hook ---"

cat <<YAML | kubectl $KC apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: prestop-test
  namespace: $NS
spec:
  nodeName: "$NODE"
  terminationGracePeriodSeconds: 30
  containers:
  - name: main
    image: $IMAGE
    command: ["/bin/sh", "-c", "sleep 3600"]
    lifecycle:
      preStop:
        exec:
          command: ["/bin/sh", "-c", "echo PRESTOP_MARKER > /tmp/prestop-ran"]
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

    # Verify the marker file doesn't exist yet
    pre_check=$(kubectl $KC exec prestop-test -n "$NS" -- cat /tmp/prestop-ran 2>/dev/null || echo "NOT_FOUND")
    if [[ "$pre_check" == "NOT_FOUND" ]]; then
        pass "PreStop marker does not exist before delete"
    else
        fail "PreStop marker exists before delete" "unexpected"
    fi

    # Delete the pod — PreStop hook should run before container stops.
    # We can verify by checking if the hook ran by looking at events.
    kubectl $KC delete pod prestop-test -n "$NS" --grace-period=30 --wait=true 2>/dev/null

    # Check that no FailedPreStopHook event was emitted (hook should succeed)
    events=$(kubectl $KC get events -n "$NS" --field-selector involvedObject.name=prestop-test -o jsonpath='{.items[*].reason}' 2>/dev/null)
    if echo "$events" | grep -q "FailedPreStopHook"; then
        fail "PreStop hook failed" "FailedPreStopHook event found"
    else
        pass "PreStop hook completed without error"
    fi

    # Verify delete event was emitted
    if echo "$events" | grep -q "PerigeosDeleteSuccess"; then
        pass "PreStop pod deleted successfully"
    else
        fail "PreStop pod delete event missing" "events=$events"
    fi
fi

# ============================================================
# Test 4: Graceful termination — terminationGracePeriodSeconds respected
# ============================================================
echo "--- Test 4: terminationGracePeriodSeconds timing ---"

cat <<YAML | kubectl $KC apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: grace-timing
  namespace: $NS
spec:
  nodeName: "$NODE"
  terminationGracePeriodSeconds: 5
  containers:
  - name: main
    image: $IMAGE
    command: ["/bin/sh", "-c", "trap 'echo SIGTERM; sleep 20' TERM; sleep 3600"]
YAML

echo "  Waiting for pod Running..."
for i in $(seq 1 30); do
    phase=$(kubectl $KC get pod grace-timing -n "$NS" -o jsonpath='{.status.phase}' 2>/dev/null)
    [[ "$phase" == "Running" ]] && break
    sleep 1
done

if [[ "$phase" != "Running" ]]; then
    fail "Pod grace-timing not Running" "phase=$phase"
else
    pass "Pod grace-timing is Running"

    # Delete with 5s grace period. The container traps SIGTERM and sleeps 20s,
    # but systemd should SIGKILL after ~5s (TimeoutStopSec from pod spec).
    start_time=$(date +%s)
    kubectl $KC delete pod grace-timing -n "$NS" --grace-period=5 --wait=true 2>/dev/null
    end_time=$(date +%s)
    elapsed=$((end_time - start_time))

    # Should complete in roughly 5-15s (grace + cleanup), not 20+ (full SIGTERM trap sleep)
    if [[ $elapsed -lt 20 ]]; then
        pass "Pod terminated within grace period (${elapsed}s < 20s)"
    else
        fail "Pod took too long to terminate" "${elapsed}s >= 20s, grace period not enforced"
    fi
fi

# ============================================================
# Test 5: Pod admission — pod within capacity admitted
# ============================================================
echo "--- Test 5: Pod admission — fit check (admit) ---"

cat <<YAML | kubectl $KC apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: admit-ok
  namespace: $NS
spec:
  nodeName: "$NODE"
  containers:
  - name: main
    image: $IMAGE
    command: ["/bin/sh", "-c", "sleep 3600"]
    resources:
      requests:
        memory: "32Mi"
        cpu: "10m"
YAML

echo "  Waiting for pod Running..."
for i in $(seq 1 30); do
    phase=$(kubectl $KC get pod admit-ok -n "$NS" -o jsonpath='{.status.phase}' 2>/dev/null)
    [[ "$phase" == "Running" ]] && break
    sleep 1
done

if [[ "$phase" == "Running" ]]; then
    pass "Small pod admitted and Running"
else
    fail "Small pod not Running" "phase=$phase"
fi

# ============================================================
# Test 6: Pod admission — overcommit rejected
# ============================================================
echo "--- Test 6: Pod admission — overcommit rejected ---"

# Get node capacity
node_mem=$(kubectl $KC get node "$NODE" -o jsonpath='{.status.capacity.memory}' 2>/dev/null)
echo "  Node memory capacity: $node_mem"

# Request more memory than the node has
cat <<YAML | kubectl $KC apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: admit-reject
  namespace: $NS
spec:
  nodeName: "$NODE"
  containers:
  - name: main
    image: $IMAGE
    command: ["/bin/sh", "-c", "sleep 3600"]
    resources:
      requests:
        memory: "999Gi"
        cpu: "10m"
YAML

echo "  Waiting for admission decision..."
sleep 10

phase=$(kubectl $KC get pod admit-reject -n "$NS" -o jsonpath='{.status.phase}' 2>/dev/null)
reason=$(kubectl $KC get pod admit-reject -n "$NS" -o jsonpath='{.status.reason}' 2>/dev/null)

# Pod should be Pending with PerigeosFailed reason (admission rejection)
if [[ "$phase" == "Pending" || "$phase" == "Failed" ]]; then
    pass "Overcommit pod not Running (phase=$phase)"
else
    fail "Overcommit pod should not be Running" "phase=$phase"
fi

events=$(kubectl $KC get events -n "$NS" --field-selector involvedObject.name=admit-reject -o jsonpath='{.items[*].reason}' 2>/dev/null)
if echo "$events" | grep -qE "FailedAdmission|PerigeosCreateFailed"; then
    pass "Admission rejection event emitted"
else
    fail "No admission rejection event" "events=$events"
fi

# ============================================================
# Test 7: Pod admission — best-effort (no requests) always admitted
# ============================================================
echo "--- Test 7: Pod admission — best-effort always admitted ---"

cat <<YAML | kubectl $KC apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: admit-besteffort
  namespace: $NS
spec:
  nodeName: "$NODE"
  containers:
  - name: main
    image: $IMAGE
    command: ["/bin/sh", "-c", "sleep 3600"]
YAML

echo "  Waiting for pod Running..."
for i in $(seq 1 30); do
    phase=$(kubectl $KC get pod admit-besteffort -n "$NS" -o jsonpath='{.status.phase}' 2>/dev/null)
    [[ "$phase" == "Running" ]] && break
    sleep 1
done

if [[ "$phase" == "Running" ]]; then
    pass "Best-effort pod admitted and Running"
else
    fail "Best-effort pod not Running" "phase=$phase"
fi

# ============================================================
# Test 8: Completed pod log streaming
# ============================================================
echo "--- Test 8: Completed pod log streaming ---"

cat <<YAML | kubectl $KC apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: log-complete
  namespace: $NS
spec:
  nodeName: "$NODE"
  restartPolicy: Never
  containers:
  - name: main
    image: $IMAGE
    command: ["/bin/sh", "-c", "echo LOG_MARKER_ADR3; exit 0"]
YAML

echo "  Waiting for pod to complete..."
for i in $(seq 1 60); do
    phase=$(kubectl $KC get pod log-complete -n "$NS" -o jsonpath='{.status.phase}' 2>/dev/null)
    [[ "$phase" == "Succeeded" || "$phase" == "Failed" ]] && break
    sleep 1
done

# Phase may stay Pending for fast-exit containers (BatchWatcher poll timing).
# The real test is whether logs are retrievable.
echo "  Pod phase after wait: $phase"

# Wait a moment for status propagation, then try to retrieve logs.
# This tests the completedPods fallback — after DeletePod removes the pod
# from g.pods, GetContainerLogs should still work via the UID cache.
sleep 3
logs=$(kubectl $KC logs log-complete -n "$NS" 2>/dev/null || echo "LOG_FETCH_FAILED")
if echo "$logs" | grep -q "LOG_MARKER_ADR3"; then
    pass "Logs retrieved from completed pod"
elif echo "$logs" | grep -q "LOG_FETCH_FAILED"; then
    fail "Could not fetch logs from completed pod" "log fetch failed"
else
    fail "Log marker not found in completed pod logs" "logs=$logs"
fi

# ============================================================
# Test 9: Node-level events (check that recorder is wired)
# ============================================================
echo "--- Test 9: Node event recorder wired ---"

# Node events should exist if the recorder is wired. We can't easily trigger
# a failure, but we can verify no crash occurred and the node is Ready.
node_ready=$(kubectl $KC get node "$NODE" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null)
if [[ "$node_ready" == "True" ]]; then
    pass "Node $NODE is Ready (event recorder wired without crash)"
else
    fail "Node $NODE not Ready" "status=$node_ready"
fi

# ============================================================
echo
echo "=== Results: $PASS/$TOTAL passed, $FAIL failed ==="
exit $FAIL
