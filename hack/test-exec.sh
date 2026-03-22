#!/usr/bin/env bash
# Quick exec/attach e2e tests for pawn nodes.
# Replaces the slow sonobuoy [sig-cli].*exec suite.
set -o pipefail

KC="--kubeconfig /etc/rancher/k3s/k3s.yaml"
NODE="${1:-compute-00}"
IMAGE="docker.io/library/debian:bookworm"
PASS=0
FAIL=0
TOTAL=0

pass() { ((PASS++)); ((TOTAL++)); echo "  PASS: $1"; }
fail() { ((FAIL++)); ((TOTAL++)); echo "  FAIL: $1 — $2"; }

cleanup() {
    kubectl $KC delete pod test-exec-e2e --force --grace-period=0 2>/dev/null || true
}
trap cleanup EXIT

echo "=== Exec/Attach e2e tests on node $NODE ==="

# --- Setup: create a long-running pod ---
echo "Creating test pod on $NODE..."
kubectl $KC run test-exec-e2e --image="$IMAGE" --restart=Never \
    --overrides="{\"spec\":{\"nodeName\":\"$NODE\"}}" \
    -- /bin/sh -c 'while true; do sleep 60; done' >/dev/null 2>&1
kubectl $KC wait --for=condition=Ready pod/test-exec-e2e --timeout=120s >/dev/null 2>&1
echo "Pod ready."
echo

# --- Test 1: basic exec ---
echo "[1] exec echo"
out=$(kubectl $KC exec test-exec-e2e -- echo hello 2>&1)
if [[ "$out" == *"hello"* ]]; then pass "echo hello"; else fail "echo hello" "$out"; fi

# --- Test 2: exec with sh -c ---
echo "[2] exec sh -c"
out=$(kubectl $KC exec test-exec-e2e -- /bin/sh -c 'echo "from shell"' 2>&1)
if [[ "$out" == *"from shell"* ]]; then pass "sh -c echo"; else fail "sh -c echo" "$out"; fi

# --- Test 3: exit code propagation ---
echo "[3] exit code propagation"
set +e
kubectl $KC exec test-exec-e2e -- /bin/sh -c 'exit 42' >/dev/null 2>&1
rc=$?
set -e
if [[ "$rc" == "42" ]]; then pass "exit 42 → rc=42"; else fail "exit 42" "got rc=$rc"; fi

# --- Test 4: exit code 0 ---
echo "[4] exit code 0"
set +e
kubectl $KC exec test-exec-e2e -- /bin/sh -c 'exit 0' >/dev/null 2>&1
rc=$?
set -e
if [[ "$rc" == "0" ]]; then pass "exit 0 → rc=0"; else fail "exit 0" "got rc=$rc"; fi

# --- Test 5: stdin piping ---
echo "[5] stdin piping"
out=$(echo "value" | kubectl $KC exec -i test-exec-e2e -- /bin/sh -c 'read FOO && echo "read:$FOO"' 2>&1)
if [[ "$out" == *"read:value"* ]]; then pass "stdin pipe"; else fail "stdin pipe" "$out"; fi

# --- Test 6: multi-arg exec (no double sh -c wrapping) ---
echo "[6] multi-arg exec"
out=$(kubectl $KC exec test-exec-e2e -- /bin/sh -c 'echo arg1 arg2' 2>&1)
if [[ "$out" == *"arg1 arg2"* ]]; then pass "multi-arg"; else fail "multi-arg" "$out"; fi

# --- Test 7: no getcwd warning ---
echo "[7] no getcwd warning"
out=$(kubectl $KC exec test-exec-e2e -- /bin/sh -c 'echo clean' 2>&1)
if [[ "$out" != *"getcwd"* ]]; then pass "no getcwd warning"; else fail "no getcwd warning" "$out"; fi

# --- Cleanup exec pod ---
kubectl $KC delete pod test-exec-e2e --force --grace-period=0 >/dev/null 2>&1 || true
sleep 2

# --- Test 8: kubectl run --attach --stdin ---
echo "[8] kubectl run --attach --stdin"
out=$(echo "value" | timeout 120 kubectl $KC run test-exec-e2e --image="$IMAGE" --restart=Never --rm \
    --attach --stdin --overrides="{\"spec\":{\"nodeName\":\"$NODE\"}}" \
    -- /bin/sh -c 'read FOO && echo "read:$FOO"' 2>&1)
if [[ "$out" == *"read:value"* ]]; then pass "attach stdin"; else fail "attach stdin" "$out"; fi

# --- Test 9: kubectl run --attach --stdin (websockets/spdy) ---
echo "[9] kubectl run --attach --stdin (second run)"
out=$(echo "value" | timeout 120 kubectl $KC run test-exec-e2e --image="$IMAGE" --restart=Never --rm \
    --attach --stdin --overrides="{\"spec\":{\"nodeName\":\"$NODE\"}}" \
    -- /bin/sh -c 'read FOO && echo "read:$FOO"' 2>&1)
if [[ "$out" == *"read:value"* ]]; then pass "attach stdin 2"; else fail "attach stdin 2" "$out"; fi

# --- Summary ---
echo
echo "=== Results: $PASS/$TOTAL passed, $FAIL failed ==="
if [[ "$FAIL" -gt 0 ]]; then exit 1; fi
