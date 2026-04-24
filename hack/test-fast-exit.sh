#!/usr/bin/env bash
# Validates that fast-exit containers (restartPolicy: Never) correctly
# reach Succeeded/Completed status. This test catches the go-systemd
# StartTransientUnit job signal race (coreos/go-systemd#485).
#
# Requires: running perigeos + k3s.
set -o pipefail

KC="--kubeconfig /etc/rancher/k3s/k3s.yaml"
NODE="${1:-compute-00}"
NS="fast-exit-test"
PASS=0
FAIL=0
TOTAL=0

pass() { ((PASS++)); ((TOTAL++)); echo "  PASS: $1"; }
fail() { ((FAIL++)); ((TOTAL++)); echo "  FAIL: $1 - $2"; }

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

echo "=== Fast-exit container tests on node $NODE ==="
echo

# --- Setup namespace ---
echo "Ensuring namespace $NS is clean..."
force_delete_ns "$NS"
kubectl $KC create ns "$NS" 2>/dev/null || true
sleep 1

# -- Test 1: Instant exit (echo + exit 0) reaches Completed -----------------
echo "Test 1: Instant exit (echo + exit 0)"
cat <<EOF | kubectl $KC apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: instant-exit
  namespace: $NS
spec:
  restartPolicy: Never
  nodeSelector:
    kubernetes.io/hostname: $NODE
  containers:
  - name: instant
    image: docker.io/library/busybox:latest
    command: ["sh", "-c", "echo hello; exit 0"]
EOF

# Wait up to 30s for terminal phase
for i in $(seq 1 30); do
    phase=$(kubectl $KC get pod instant-exit -n "$NS" -o jsonpath='{.status.phase}' 2>/dev/null)
    if [[ "$phase" == "Succeeded" || "$phase" == "Failed" ]]; then
        break
    fi
    sleep 1
done

phase=$(kubectl $KC get pod instant-exit -n "$NS" -o jsonpath='{.status.phase}' 2>/dev/null)
if [[ "$phase" == "Succeeded" ]]; then
    pass "instant-exit pod reached Succeeded phase"
else
    fail "instant-exit pod phase" "expected Succeeded, got '$phase'"
fi

# Verify logs are retrievable from completed pod
logs=$(kubectl $KC logs instant-exit -n "$NS" 2>/dev/null)
if echo "$logs" | grep -q "hello"; then
    pass "instant-exit logs retrievable after completion"
else
    fail "instant-exit logs" "expected 'hello' in logs"
fi

# -- Test 2: Instant failure (exit 1) reaches Failed -----------------------
echo "Test 2: Instant failure (exit 1)"
cat <<EOF | kubectl $KC apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: instant-fail
  namespace: $NS
spec:
  restartPolicy: Never
  nodeSelector:
    kubernetes.io/hostname: $NODE
  containers:
  - name: failer
    image: docker.io/library/busybox:latest
    command: ["sh", "-c", "echo oops; exit 1"]
EOF

for i in $(seq 1 30); do
    phase=$(kubectl $KC get pod instant-fail -n "$NS" -o jsonpath='{.status.phase}' 2>/dev/null)
    if [[ "$phase" == "Succeeded" || "$phase" == "Failed" ]]; then
        break
    fi
    sleep 1
done

phase=$(kubectl $KC get pod instant-fail -n "$NS" -o jsonpath='{.status.phase}' 2>/dev/null)
if [[ "$phase" == "Failed" ]]; then
    pass "instant-fail pod reached Failed phase"
else
    fail "instant-fail pod phase" "expected Failed, got '$phase'"
fi

# -- Test 3: Job with fast-exit completes ----------------------------------
echo "Test 3: Job with fast-exit container completes"
cat <<EOF | kubectl $KC apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: fast-job
  namespace: $NS
spec:
  template:
    spec:
      restartPolicy: Never
      nodeSelector:
        kubernetes.io/hostname: $NODE
      containers:
      - name: worker
        image: docker.io/library/busybox:latest
        command: ["sh", "-c", "echo job-done; exit 0"]
EOF

for i in $(seq 1 45); do
    completions=$(kubectl $KC get job fast-job -n "$NS" -o jsonpath='{.status.succeeded}' 2>/dev/null)
    if [[ "$completions" == "1" ]]; then
        break
    fi
    sleep 1
done

completions=$(kubectl $KC get job fast-job -n "$NS" -o jsonpath='{.status.succeeded}' 2>/dev/null)
if [[ "$completions" == "1" ]]; then
    pass "fast-job completed with 1 success"
else
    status=$(kubectl $KC get job fast-job -n "$NS" -o jsonpath='{.status.conditions[0].type}' 2>/dev/null)
    fail "fast-job completion" "expected 1 success, got completions='$completions' status='$status'"
fi

# -- Test 4: Event-based detection speed ----------------------------------
echo "Test 4: Event-based detection is faster than poll interval"
start_time=$(date +%s)
cat <<EOF | kubectl $KC apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: timing-test
  namespace: $NS
spec:
  restartPolicy: Never
  nodeSelector:
    kubernetes.io/hostname: $NODE
  containers:
  - name: timer
    image: docker.io/library/busybox:latest
    command: ["sh", "-c", "echo fast; exit 0"]
EOF

for i in $(seq 1 30); do
    phase=$(kubectl $KC get pod timing-test -n "$NS" -o jsonpath='{.status.phase}' 2>/dev/null)
    if [[ "$phase" == "Succeeded" ]]; then
        break
    fi
    sleep 1
done

end_time=$(date +%s)
elapsed=$((end_time - start_time))
if [[ "$elapsed" -le 20 ]]; then
    pass "timing-test reached Succeeded in ${elapsed}s (< 20s threshold)"
else
    fail "timing-test speed" "took ${elapsed}s (threshold 20s)"
fi

# -- Summary --------------------------------------------------------------
echo
echo "=== Results: $PASS/$TOTAL passed, $FAIL failed ==="
[[ $FAIL -eq 0 ]] && exit 0 || exit 1
