#!/bin/bash

# --- Configuration ---
TEST_NS="test-general-scaler"
SCALER_NAME="test-e2e-scaler"
TARGET_DEPLOYMENT="e2e-worker-app"
INITIAL_REPLICAS=11
FINAL_DESIRED=2
LOW_LOAD_METRIC=10.0 
MAX_RATE=1
SCALE_DOWN_COOLDOWN_SECONDS=60 # Use 60s for testing (instead of 10m)

get_replicas() {
    kubectl get deploy/$TARGET_DEPLOYMENT -n $TEST_NS -o=jsonpath='{.spec.replicas}' 2>/dev/null
}

# --- 1. Setup Environment ---
echo "--- 1. Setting up environment ---"
kubectl create ns $TEST_NS 

echo "Deploying target app at $INITIAL_REPLICAS replicas..."
cat <<EOF | kubectl apply -f -
apiVersion: apps/v1
kind: Deployment
metadata:
  name: $TARGET_DEPLOYMENT
  namespace: $TEST_NS
# ... rest of deployment spec ...
spec:
  replicas: $INITIAL_REPLICAS
# ...
EOF

echo "Applying GeneralScaler config with low load metric (10.0)..."
cat <<EOF | kubectl apply -f -
apiVersion: autoscaling.example.com/v1alpha1
kind: GeneralScaler
metadata:
  name: $SCALER_NAME
  namespace: $TEST_NS
spec:
  targetRef:
    name: $TARGET_DEPLOYMENT
    kind: Deployment
    apiVersion: apps/v1
  minReplicas: $FINAL_DESIRED
  maxReplicas: 15
  metrics:
    - plugin: redis
      config:
        # In a real E2E test, you would need to mock the RedisPlugin 
        # to return the 10.0 value here, or modify the running controller's code.
  policy:
    type: cost
    maxCostPerReplica: 5.0
  safety:
    maxScaleRate: "$MAX_RATE"
    scaleDownCooldown: "${SCALE_DOWN_COOLDOWN_SECONDS}s" # Use 60s for test speed
EOF

# --- 2. Verification - First Scale Down (Rate Limit) ---
echo "--- 2. Verifying initial rate-limited scale-down ($INITIAL_REPLICAS -> 10) ---"
EXPECTED_SCALE_1=$((INITIAL_REPLICAS - MAX_RATE)) # 11 -> 10

# Wait for the first reconciliation cycle to complete
sleep 40 # Wait for 30s cycle + some buffer

CURRENT_REPLICAS=$(get_replicas)
if [[ "$CURRENT_REPLICAS" -eq "$EXPECTED_SCALE_1" ]]; then
    echo "✅ SUCCESS: Initial scale-down confirmed: Replicas are at $CURRENT_REPLICAS."
else
    echo "❌ FAILURE: Expected $EXPECTED_SCALE_1 replicas, found $CURRENT_REPLICAS."
    exit 1
fi

# --- 3. Verification - Cooldown Enforcement ---
echo "--- 3. Verifying Scale-Down Cooldown is Active (Should remain at 10) ---"

# Wait for the second reconciliation cycle (another 30 seconds)
sleep 30 

CURRENT_REPLICAS=$(get_replicas)
if [[ "$CURRENT_REPLICAS" -eq "$EXPECTED_SCALE_1" ]]; then
    echo "✅ SUCCESS: Cooldown is enforced. Replicas still at $CURRENT_REPLICAS (Expected: Cooldown active log)."
else
    echo "❌ FAILURE: Cooldown failed. Replicas scaled unexpectedly to $CURRENT_REPLICAS."
    exit 1
fi

# --- 4. Cleanup ---
echo "--- 4. Cleanup ---"
kubectl delete ns $TEST_NS --force --grace-period=0