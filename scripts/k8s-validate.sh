#!/bin/bash
# Validates DistKV on Kubernetes:
#   1. Quorum replication (write node-0, read node-1/2)
#   2. Pod-failure recovery (delete pod, verify data survives)
#   3. Scale-out behavior (3 -> 5 replicas, new nodes join)

set -e

NS=distkv
STS=distkv
CLIENT_BIN="./build/distkv-client"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
pass() { echo -e "${GREEN}[PASS]${NC} $1"; }
fail() { echo -e "${RED}[FAIL]${NC} $1"; exit 1; }
info() { echo -e "${YELLOW}[INFO]${NC} $1"; }

# Run client inside pod-0 (avoids port-forward complexity)
kv_put() { kubectl exec -n $NS ${STS}-0 -- ./distkv-client -server=localhost:8080 put "$1" "$2" 2>&1; }
kv_get_pod() {
    local pod=$1 key=$2
    kubectl exec -n $NS ${STS}-${pod} -- ./distkv-client -server=localhost:8080 -consistency=quorum get "$key" 2>&1
}
kv_status() { kubectl exec -n $NS ${STS}-0 -- ./distkv-client -server=localhost:8080 status 2>&1; }

wait_pods_ready() {
    local expected=$1
    info "Waiting for $expected pods to be Ready..."
    kubectl rollout status statefulset/$STS -n $NS --timeout=120s
    local ready
    ready=$(kubectl get pods -n $NS --field-selector=status.phase=Running \
        -o jsonpath='{.items[*].status.containerStatuses[0].ready}' | tr ' ' '\n' | grep -c true || true)
    [ "$ready" -eq "$expected" ] || fail "Expected $expected ready pods, got $ready"
    pass "$expected pods ready"
}

# ──────────────────────────────────────────────────────────────
echo ""
echo "=== Test 1: Quorum Replication ==="
info "Writing key via distkv-0..."
kv_put "resume-key" "distkv-quorum-test"

info "Reading from distkv-1 (different pod, quorum read)..."
VAL=$(kv_get_pod 1 "resume-key")
echo "  Got: $VAL"
echo "$VAL" | grep -q "distkv-quorum-test" && pass "Quorum replication: data consistent across pods" \
    || fail "Quorum replication: value not found on pod-1"

info "Reading from distkv-2..."
VAL=$(kv_get_pod 2 "resume-key")
echo "  Got: $VAL"
echo "$VAL" | grep -q "distkv-quorum-test" && pass "Quorum replication: data consistent on pod-2 as well" \
    || fail "Quorum replication: value not found on pod-2"

# ──────────────────────────────────────────────────────────────
echo ""
echo "=== Test 2: Pod-Failure Recovery ==="
info "Writing key before failure..."
kv_put "failure-key" "survives-restart"

info "Deleting pod distkv-0 (simulating failure)..."
kubectl delete pod -n $NS ${STS}-0

info "Waiting for pod to restart..."
sleep 5
kubectl wait --for=condition=Ready pod/${STS}-0 -n $NS --timeout=120s
pass "Pod distkv-0 restarted"

info "Reading key after recovery..."
VAL=$(kv_get_pod 0 "failure-key")
echo "  Got: $VAL"
echo "$VAL" | grep -q "survives-restart" && pass "Pod-failure recovery: data persisted across restart" \
    || fail "Pod-failure recovery: data lost after pod restart"

# ──────────────────────────────────────────────────────────────
echo ""
echo "=== Test 3: Scale-Out (3 -> 5 replicas) ==="
info "Writing data before scale-out..."
kv_put "scale-key" "visible-after-scale"

info "Scaling StatefulSet to 5 replicas..."
kubectl scale statefulset/$STS -n $NS --replicas=5
sleep 5
wait_pods_ready 5

info "Verifying new nodes can read existing data (gossip membership working)..."
VAL=$(kv_get_pod 3 "scale-key")
echo "  Got from pod-3: $VAL"
echo "$VAL" | grep -q "visible-after-scale" && pass "Scale-out: pod-3 (new node) serves existing data" \
    || fail "Scale-out: new node pod-3 cannot serve data"

info "Restoring to 3 replicas..."
kubectl scale statefulset/$STS -n $NS --replicas=3

# ──────────────────────────────────────────────────────────────
echo ""
echo "=== Cluster Status ==="
kv_status

echo ""
echo -e "${GREEN}All validations passed.${NC}"
echo "Scenarios confirmed: quorum replication, pod-failure recovery, scale-out."
