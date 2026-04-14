#!/usr/bin/env bash
# Live validation for ADR-0002 Phase 5 (env pipeline + live CM/Secret refresh)
# and Phase 7 (queue depth metrics).
#
# Requires: running perigeos + k3s, root for varlink socket access.
set -o pipefail

KC="--kubeconfig /etc/rancher/k3s/k3s.yaml"
NODE="${1:-compute-00}"
IMAGE="docker.io/library/debian:bookworm"
NS="phase5-7-test"
SOCK="/run/apsis/perigeos.sock"
PASS=0
FAIL=0
TOTAL=0

pass() { ((PASS++)); ((TOTAL++)); echo "  PASS: $1"; }
fail() { ((FAIL++)); ((TOTAL++)); echo "  FAIL: $1 - $2"; }

# Read an env var from PID 1 inside the container (exec spawns a new process
# that doesn't inherit nspawn --setenv vars, so we read /proc/1/environ).
pod_env() {
    local ns="$1" pod="$2" key="$3"
    kubectl $KC exec "$pod" -n "$ns" -- /bin/sh -c \
        "cat /proc/1/environ | tr '\0' '\n' | grep ^${key}= | head -1 | cut -d= -f2-" 2>/dev/null
}

force_delete_ns() {
    local ns="$1"
    kubectl $KC delete ns "$ns" --force --grace-period=0 2>/dev/null || true
    # If stuck in Terminating (metrics API stale), force-finalize.
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

echo "=== Phase 5 & 7 live tests on node $NODE ==="
echo

# --- Setup namespace (clear any previous stuck ns) ---
echo "Ensuring namespace $NS is clean..."
if kubectl $KC get ns "$NS" 2>/dev/null; then
    force_delete_ns "$NS"
    # Wait until gone
    for i in $(seq 1 15); do
        kubectl $KC get ns "$NS" 2>/dev/null || break
        sleep 1
    done
fi
kubectl $KC create ns "$NS"
echo

# ============================================================
# Phase 5A: Env pipeline - fieldRef status.podIP resolves after CNI
# ============================================================
echo "--- Phase 5A: fieldRef status.podIP resolution ---"

cat <<YAML | kubectl $KC apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: env-fieldref
  namespace: $NS
spec:
  nodeName: "$NODE"
  containers:
  - name: main
    image: $IMAGE
    command: ["/bin/sh", "-c", "sleep 3600"]
    env:
    - name: MY_POD_IP_FROM_FIELDREF
      valueFrom:
        fieldRef:
          fieldPath: status.podIP
    - name: MY_NODE_NAME_FROM_FIELDREF
      valueFrom:
        fieldRef:
          fieldPath: spec.nodeName
    - name: MY_POD_NAME_FROM_FIELDREF
      valueFrom:
        fieldRef:
          fieldPath: metadata.name
    - name: MY_POD_NS_FROM_FIELDREF
      valueFrom:
        fieldRef:
          fieldPath: metadata.namespace
YAML

kubectl $KC wait --for=condition=Ready pod/env-fieldref -n "$NS" --timeout=120s 2>/dev/null
echo

# Check status.podIP is not empty
echo "[1] status.podIP resolved (not empty)"
pod_ip=$(kubectl $KC get pod env-fieldref -n "$NS" -o jsonpath='{.status.podIP}')
if [[ -n "$pod_ip" && "$pod_ip" != "<none>" ]]; then
    pass "podIP=$pod_ip"
else
    fail "podIP is empty" "got '$pod_ip'"
fi

# Check the env var inside the container matches the actual podIP
echo "[2] MY_POD_IP_FROM_FIELDREF matches status.podIP"
env_ip=$(pod_env "$NS" env-fieldref MY_POD_IP_FROM_FIELDREF)
if [[ "$env_ip" == "$pod_ip" ]]; then
    pass "fieldRef podIP=$env_ip matches status.podIP=$pod_ip"
else
    fail "fieldRef podIP mismatch" "env=$env_ip status=$pod_ip"
fi

# Check spec.nodeName resolved
echo "[3] spec.nodeName resolved"
env_node=$(pod_env "$NS" env-fieldref MY_NODE_NAME_FROM_FIELDREF)
if [[ "$env_node" == "$NODE" ]]; then
    pass "nodeName=$env_node"
else
    fail "nodeName mismatch" "got '$env_node' want '$NODE'"
fi

# Check metadata.name
echo "[4] metadata.name resolved"
env_name=$(pod_env "$NS" env-fieldref MY_POD_NAME_FROM_FIELDREF)
if [[ "$env_name" == "env-fieldref" ]]; then
    pass "podName=$env_name"
else
    fail "podName mismatch" "got '$env_name'"
fi

# Check metadata.namespace
echo "[5] metadata.namespace resolved"
env_ns=$(pod_env "$NS" env-fieldref MY_POD_NS_FROM_FIELDREF)
if [[ "$env_ns" == "$NS" ]]; then
    pass "namespace=$env_ns"
else
    fail "namespace mismatch" "got '$env_ns'"
fi

# Check force-injected MY_POD_IP is present
echo "[6] MY_POD_IP (force-injected) present"
my_pod_ip=$(pod_env "$NS" env-fieldref MY_POD_IP)
if [[ -n "$my_pod_ip" && "$my_pod_ip" == "$pod_ip" ]]; then
    pass "MY_POD_IP=$my_pod_ip"
else
    fail "MY_POD_IP missing or wrong" "got '$my_pod_ip' want '$pod_ip'"
fi

echo

# ============================================================
# Phase 5A: ConfigMap env var injection
# ============================================================
echo "--- Phase 5A: ConfigMap env injection ---"

kubectl $KC create configmap test-env-cm -n "$NS" --from-literal=DB_HOST=postgres.local --from-literal=DB_PORT=5432

cat <<YAML | kubectl $KC apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: env-from-cm
  namespace: $NS
spec:
  nodeName: "$NODE"
  containers:
  - name: main
    image: $IMAGE
    command: ["/bin/sh", "-c", "sleep 3600"]
    envFrom:
    - configMapRef:
        name: test-env-cm
YAML

kubectl $KC wait --for=condition=Ready pod/env-from-cm -n "$NS" --timeout=120s 2>/dev/null

echo "[7] envFrom configMapRef resolves DB_HOST"
db_host=$(pod_env "$NS" env-from-cm DB_HOST)
if [[ "$db_host" == "postgres.local" ]]; then
    pass "DB_HOST=$db_host"
else
    fail "DB_HOST wrong" "got '$db_host'"
fi

echo "[8] envFrom configMapRef resolves DB_PORT"
db_port=$(pod_env "$NS" env-from-cm DB_PORT)
if [[ "$db_port" == "5432" ]]; then
    pass "DB_PORT=$db_port"
else
    fail "DB_PORT wrong" "got '$db_port'"
fi

echo

# ============================================================
# Phase 5B: Live ConfigMap volume refresh
# ============================================================
echo "--- Phase 5B: Live ConfigMap volume refresh ---"

kubectl $KC create configmap vol-cm -n "$NS" --from-literal=app.conf="version=1"

cat <<YAML | kubectl $KC apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: vol-cm-refresh
  namespace: $NS
spec:
  nodeName: "$NODE"
  containers:
  - name: main
    image: $IMAGE
    command: ["/bin/sh", "-c", "sleep 3600"]
    volumeMounts:
    - name: config
      mountPath: /etc/config
      readOnly: true
  volumes:
  - name: config
    configMap:
      name: vol-cm
YAML

kubectl $KC wait --for=condition=Ready pod/vol-cm-refresh -n "$NS" --timeout=120s 2>/dev/null

echo "[9] Initial ConfigMap volume content"
v1=$(kubectl $KC exec vol-cm-refresh -n "$NS" -- cat /etc/config/app.conf 2>/dev/null)
if [[ "$v1" == "version=1" ]]; then
    pass "initial content=$v1"
else
    fail "initial content wrong" "got '$v1'"
fi

# Update the ConfigMap
kubectl $KC create configmap vol-cm -n "$NS" --from-literal=app.conf="version=2" --dry-run=client -o yaml \
    | kubectl $KC apply -f -

# Wait for informer + refresh (give it a few seconds)
echo "  Waiting for live refresh..."
sleep 5

echo "[10] ConfigMap volume refreshed in-place"
v2=$(kubectl $KC exec vol-cm-refresh -n "$NS" -- cat /etc/config/app.conf 2>/dev/null)
if [[ "$v2" == "version=2" ]]; then
    pass "refreshed content=$v2"
else
    fail "content not refreshed" "got '$v2' want 'version=2'"
fi

echo

# ============================================================
# Phase 5B: Live Secret volume refresh
# ============================================================
echo "--- Phase 5B: Live Secret volume refresh ---"

kubectl $KC create secret generic vol-secret -n "$NS" --from-literal=token="secret-v1"

cat <<YAML | kubectl $KC apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: vol-secret-refresh
  namespace: $NS
spec:
  nodeName: "$NODE"
  containers:
  - name: main
    image: $IMAGE
    command: ["/bin/sh", "-c", "sleep 3600"]
    volumeMounts:
    - name: creds
      mountPath: /etc/creds
      readOnly: true
  volumes:
  - name: creds
    secret:
      secretName: vol-secret
YAML

kubectl $KC wait --for=condition=Ready pod/vol-secret-refresh -n "$NS" --timeout=120s 2>/dev/null

echo "[11] Initial Secret volume content"
s1=$(kubectl $KC exec vol-secret-refresh -n "$NS" -- cat /etc/creds/token 2>/dev/null)
if [[ "$s1" == "secret-v1" ]]; then
    pass "initial secret=$s1"
else
    fail "initial secret wrong" "got '$s1'"
fi

# Update the Secret
kubectl $KC create secret generic vol-secret -n "$NS" --from-literal=token="secret-v2" --dry-run=client -o yaml \
    | kubectl $KC apply -f -

echo "  Waiting for live refresh..."
sleep 5

echo "[12] Secret volume refreshed in-place"
s2=$(kubectl $KC exec vol-secret-refresh -n "$NS" -- cat /etc/creds/token 2>/dev/null)
if [[ "$s2" == "secret-v2" ]]; then
    pass "refreshed secret=$s2"
else
    fail "secret not refreshed" "got '$s2' want 'secret-v2'"
fi

echo

# ============================================================
# Phase 5B: Stale key removal on CM update
# ============================================================
echo "--- Phase 5B: Stale key removal ---"

kubectl $KC create configmap vol-cm-keys -n "$NS" \
    --from-literal=key-a="alpha" --from-literal=key-b="bravo"

cat <<YAML | kubectl $KC apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: vol-cm-stale
  namespace: $NS
spec:
  nodeName: "$NODE"
  containers:
  - name: main
    image: $IMAGE
    command: ["/bin/sh", "-c", "sleep 3600"]
    volumeMounts:
    - name: config
      mountPath: /etc/config
      readOnly: true
  volumes:
  - name: config
    configMap:
      name: vol-cm-keys
YAML

kubectl $KC wait --for=condition=Ready pod/vol-cm-stale -n "$NS" --timeout=120s 2>/dev/null

echo "[13] Both keys present initially"
ka=$(kubectl $KC exec vol-cm-stale -n "$NS" -- cat /etc/config/key-a 2>&1)
kb=$(kubectl $KC exec vol-cm-stale -n "$NS" -- cat /etc/config/key-b 2>&1)
if [[ "$ka" == "alpha" && "$kb" == "bravo" ]]; then
    pass "key-a=$ka key-b=$kb"
else
    fail "initial keys wrong" "a='$ka' b='$kb'"
fi

# Remove key-b, update key-a (must use replace, not apply - apply merges and won't delete keys)
kubectl $KC create configmap vol-cm-keys -n "$NS" --from-literal=key-a="alpha-v2" --dry-run=client -o yaml \
    | kubectl $KC replace -f -

echo "  Waiting for live refresh (stale key removal)..."
sleep 15

echo "[14] key-a updated"
ka2=$(kubectl $KC exec vol-cm-stale -n "$NS" -- cat /etc/config/key-a 2>&1)
if [[ "$ka2" == "alpha-v2" ]]; then
    pass "key-a=$ka2"
else
    fail "key-a not updated" "got '$ka2'"
fi

echo "[15] key-b removed"
kb2=$(kubectl $KC exec vol-cm-stale -n "$NS" -- cat /etc/config/key-b 2>&1 || echo "__GONE__")
if [[ -z "$kb2" || "$kb2" == *"No such file"* || "$kb2" == "__GONE__" ]]; then
    pass "key-b removed or emptied"
else
    fail "key-b still has content" "got '$kb2'"
fi

echo

# ============================================================
# Phase 7: Queue depth metrics via varlink
# ============================================================
echo "--- Phase 7: Queue depth metrics ---"

if [[ -S "$SOCK" ]]; then
    echo "[16] Varlink Pawns response includes queue depths"
    # Use varlink CLI if available, otherwise socat with null-byte termination
    if command -v varlink &>/dev/null; then
        pawns_json=$(varlink call "unix:$SOCK/io.perigeos.Manager.Pawns" 2>&1)
    else
        # Varlink protocol: JSON + null byte, response is JSON + null byte
        pawns_json=$(printf '{"method":"io.perigeos.Manager.Pawns","parameters":{}}\0' \
            | socat -t2 - UNIX-CONNECT:"$SOCK" 2>/dev/null | tr -d '\0')
    fi

    if echo "$pawns_json" | grep -q "sync_queue_depth"; then
        pass "sync_queue_depth present in Pawns response"
    else
        fail "sync_queue_depth missing" "response: $pawns_json"
    fi

    echo "[17] delete_queue_depth present"
    if echo "$pawns_json" | grep -q "delete_queue_depth"; then
        pass "delete_queue_depth present"
    else
        fail "delete_queue_depth missing" "response: $pawns_json"
    fi

    echo "[18] status_queue_depth present"
    if echo "$pawns_json" | grep -q "status_queue_depth"; then
        pass "status_queue_depth present"
    else
        fail "status_queue_depth missing" "response: $pawns_json"
    fi
else
    echo "  SKIP: varlink socket $SOCK not found (perigeos not running?)"
fi

# ============================================================
# Phase 7: KUBERNETES_SERVICE_HOST injection
# ============================================================
echo
echo "--- Phase 5A: KUBERNETES_SERVICE_HOST injection ---"

echo "[19] KUBERNETES_SERVICE_HOST present in pod"
ksh=$(pod_env "$NS" env-fieldref KUBERNETES_SERVICE_HOST)
if [[ -n "$ksh" ]]; then
    pass "KUBERNETES_SERVICE_HOST=$ksh"
else
    fail "KUBERNETES_SERVICE_HOST empty" ""
fi

echo "[20] KUBERNETES_SERVICE_PORT present in pod"
ksp=$(pod_env "$NS" env-fieldref KUBERNETES_SERVICE_PORT)
if [[ -n "$ksp" ]]; then
    pass "KUBERNETES_SERVICE_PORT=$ksp"
else
    fail "KUBERNETES_SERVICE_PORT empty" ""
fi

# --- Summary ---
echo
echo "=== Results: $PASS/$TOTAL passed, $FAIL failed ==="
if [[ "$FAIL" -gt 0 ]]; then exit 1; fi
