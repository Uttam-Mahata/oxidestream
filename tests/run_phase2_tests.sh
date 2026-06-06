#!/bin/bash
set -e

WORKSPACE_DIR="/home/neutrino/oxidestream"
TEST_DIR="$WORKSPACE_DIR/tests"
DATA_DIR="$TEST_DIR/data"

echo "=== OxideStream Phase 2 Enterprise Features Integration Test ==="

# Clean directories
rm -rf "$TEST_DIR/operator_output"
rm -rf "$TEST_DIR/streaming_input"
rm -rf "$TEST_DIR/streaming_output"
rm -rf "$TEST_DIR/checkpoint_status.json"
rm -rf "$TEST_DIR/output_dpp"
rm -rf "$TEST_DIR/output_lr"
rm -rf "$TEST_DIR/output_pr"

mkdir -p "$TEST_DIR/streaming_input"
mkdir -p "$TEST_DIR/streaming_output"

# Re-compile Go
echo "Recompiling Go Control Plane..."
cd "$WORKSPACE_DIR/control-plane"
/usr/local/go/bin/go build -o control-plane-master
cp control-plane-master "$WORKSPACE_DIR/"
cd "$WORKSPACE_DIR"

# Clean log files
rm -f "$TEST_DIR"/master_p2.log "$TEST_DIR"/worker-1_p2.log "$TEST_DIR"/worker-2_p2.log

# ----------------- Helper Functions -----------------
start_cluster() {
    echo "Starting cluster..."
    cd "$WORKSPACE_DIR/control-plane"
    ./control-plane-master --grpc-port=50050 --http-port=8080 > "$TEST_DIR/master_p2.log" 2>&1 &
    MASTER_PID=$!
    cd "$WORKSPACE_DIR"
    sleep 2

    # Worker 1
    ./data-plane/target/debug/data-plane \
      --worker-id="worker-1" \
      --host="127.0.0.1" \
      --control-port=50051 \
      --flight-port=50052 \
      --master-address="http://127.0.0.1:50050" \
      --data-dir="$TEST_DIR/data-dir-worker-1" > "$TEST_DIR/worker-1_p2.log" 2>&1 &
    WORKER1_PID=$!

    # Worker 2
    ./data-plane/target/debug/data-plane \
      --worker-id="worker-2" \
      --host="127.0.0.1" \
      --control-port=50053 \
      --flight-port=50054 \
      --master-address="http://127.0.0.1:50050" \
      --data-dir="$TEST_DIR/data-dir-worker-2" > "$TEST_DIR/worker-2_p2.log" 2>&1 &
    WORKER2_PID=$!

    sleep 3
    echo "Cluster started. Master PID: $MASTER_PID, Worker-1: $WORKER1_PID, Worker-2: $WORKER2_PID"
}

stop_cluster() {
    echo "Stopping cluster..."
    kill $WORKER1_PID || true
    kill $WORKER2_PID || true
    kill $MASTER_PID || true
    wait $WORKER1_PID $WORKER2_PID $MASTER_PID 2>/dev/null || true
    echo "Cluster stopped."
}

# ----------------- Test 1: Operator Simulator Mode -----------------
echo ""
echo "=============================================="
echo "TEST 1: Kubernetes Operator Simulator Mode"
echo "=============================================="
cd "$WORKSPACE_DIR"
./control-plane/control-plane-master --operator="$TEST_DIR/job_operator.yaml"

if [ -f "$TEST_DIR/operator_output/part_0.csv" ]; then
    echo "Test 1 SUCCESS: Operator Simulator deployed cluster and successfully executed batch job."
    echo "=== Operator Output Preview ==="
    cat "$TEST_DIR/operator_output/part_0.csv"
else
    echo "Test 1 FAILED: Output file not found."
    exit 1
fi

# ----------------- Test 2: Structured Streaming, Watermarks, Checkpointing -----------------
echo ""
echo "=============================================="
echo "TEST 2: Structured Streaming & Watermarks & Checkpointing"
echo "=============================================="
start_cluster

# Copy category lookup to streaming input so it joins
cp "$DATA_DIR/category_lookup.csv" "$TEST_DIR/streaming_input/category_lookup.csv"

# Write Batch 1 csv
cat <<EOF > "$TEST_DIR/streaming_input/batch1.csv"
user_id,rating,timestamp,category
user_1,4,1780598000,action
user_2,5,1780598001,comedy
EOF

echo "Submitting Structured Streaming job to Master REST API..."
curl -s -X POST -H "Content-Type: application/json" -d '{
  "input_dir": "/home/neutrino/oxidestream/tests/streaming_input",
  "checkpoint_file": "/home/neutrino/oxidestream/tests/checkpoint_status.json",
  "map_sql": "SELECT r.user_id, c.category_name, COUNT(1) as rating_count, SUM(r.rating) as rating_sum, MAX(r.timestamp) as timestamp FROM input r JOIN category_lookup c ON r.category = c.category GROUP BY r.user_id, c.category_name",
  "reduce_sql": "SELECT user_id, category_name, SUM(rating_count) as total_ratings, SUM(rating_sum) as total_rating_sum FROM input GROUP BY user_id, category_name",
  "num_partitions": 2,
  "output_dir": "/home/neutrino/oxidestream/tests/streaming_output"
}' http://localhost:8080/submit_streaming

echo "Waiting for Batch 1 to process..."
sleep 4.5

# Write Batch 2 csv (contains a normal row and a late row)
# Current max timestamp will be 1780598001 (from batch1).
# In batch2, the max timestamp will rise to 1780598004.
# Cutoff watermark is max_timestamp - 5 = 1780597999.
# The row for user_3 has timestamp 1780597990 which is < 1780597999, so it must be dropped!
cat <<EOF > "$TEST_DIR/streaming_input/batch2.csv"
user_id,rating,timestamp,category
user_1,5,1780598003,action
user_2,3,1780598004,comedy
user_3,4,1780597990,action
EOF

echo "Copying Batch 2 containing late record (user_3)..."
sleep 4.5

checkpoint_file="$TEST_DIR/streaming_output/checkpoint_state.json"
if [ -f "$checkpoint_file" ]; then
    echo "Test 2 SUCCESS: Checkpoint state file created."
    echo "=== Checkpoint State (Aggregated counts) ==="
    cat "$checkpoint_file"
    
    # Verify that user_3 (late data) is not in the checkpoint state
    if grep -q "user_3" "$checkpoint_file"; then
        echo "Test 2 FAILED: Watermarking failed. Late record user_3 was not filtered out."
        stop_cluster
        exit 1
    else
        echo "Test 2 SUCCESS: Watermarking correctly filtered out late record user_3."
    fi
else
    echo "Test 2 FAILED: Checkpoint file not found."
    stop_cluster
    exit 1
fi

stop_cluster

# ----------------- Test 3: Dynamic Partition Pruning (DPP) -----------------
echo ""
echo "=============================================="
echo "TEST 3: Dynamic Partition Pruning (DPP)"
echo "=============================================="
start_cluster

echo "Submitting DPP Job with dim_user.csv filter (active = true)..."
curl -s -X POST -H "Content-Type: application/json" -d '{
  "map_sql": "SELECT user_id, COUNT(1) as rating_count, SUM(rating) as rating_sum FROM input GROUP BY user_id",
  "reduce_sql": "SELECT user_id, SUM(rating_count) as total_ratings, SUM(rating_sum) as total_rating_sum FROM input GROUP BY user_id",
  "input_files": ["/home/neutrino/oxidestream/tests/data/ratings_dpp.csv"],
  "num_partitions": 2,
  "output_dir": "/home/neutrino/oxidestream/tests/output_dpp",
  "dpp_dim_file": "/home/neutrino/oxidestream/tests/data/dim_user.csv",
  "dpp_filter_col": "active",
  "dpp_filter_val": "true",
  "dpp_join_key": "user_id"
}' http://localhost:8080/submit

sleep 3.5

# Verify in master log that filter was injected
if grep -q "DPP: Found 2 matching keys" "$TEST_DIR/master_p2.log" && grep -q "AND user_id IN" "$TEST_DIR/master_p2.log"; then
    echo "Test 3 SUCCESS: Master injected DPP filter condition."
else
    echo "Test 3 FAILED: Master did not log DPP filter injection."
    stop_cluster
    exit 1
fi

if [ -f "$TEST_DIR/output_dpp/part_0.csv" ]; then
    echo "Test 3 SUCCESS: DPP job output generated successfully."
    echo "=== DPP Output preview (Only user_1 and user_3 should appear) ==="
    cat "$TEST_DIR/output_dpp/part_0.csv"
else
    echo "Test 3 FAILED: Output file not found."
    stop_cluster
    exit 1
fi

stop_cluster

# ----------------- Test 4: Distributed OLS Linear Regression (ML) -----------------
echo ""
echo "=============================================="
echo "TEST 4: Distributed Linear Regression (MLlib equivalent)"
echo "=============================================="
start_cluster

echo "Submitting Linear Regression (OLS closed-form) Job to solve: y = 2*x1 + 3*x2..."
curl -s -X POST -H "Content-Type: application/json" -d '{
  "input_files": ["/home/neutrino/oxidestream/tests/data/lr_data.csv"],
  "num_partitions": 2,
  "output_dir": "/home/neutrino/oxidestream/tests/output_lr",
  "iterations": 1
}' http://localhost:8080/submit_lr

sleep 4.5

lr_out="$TEST_DIR/output_lr/part_0.csv"
if [ -f "$lr_out" ]; then
    echo "Test 4 SUCCESS: Linear Regression output generated."
    echo "=== Trained Coefficients (expect index 0 ~ 2.0, index 1 ~ 3.0) ==="
    cat "$lr_out"
else
    echo "Test 4 FAILED: ML output file not found."
    stop_cluster
    exit 1
fi

stop_cluster

# ----------------- Test 5: Distributed PageRank (GraphX equivalent) -----------------
echo ""
echo "=============================================="
echo "TEST 5: Distributed PageRank Graph Job"
echo "=============================================="
start_cluster

echo "Submitting PageRank Graph Job..."
curl -s -X POST -H "Content-Type: application/json" -d '{
  "input_files": ["/home/neutrino/oxidestream/tests/data/pagerank_data.csv"],
  "num_partitions": 2,
  "output_dir": "/home/neutrino/oxidestream/tests/output_pr",
  "iterations": 1
}' http://localhost:8080/submit_pagerank

sleep 4.5

pr_out="$TEST_DIR/output_pr/part_0.csv"
if [ -f "$pr_out" ]; then
    echo "Test 5 SUCCESS: PageRank output generated."
    echo "=== PageRank Results ==="
    cat "$pr_out"
else
    echo "Test 5 FAILED: PageRank output file not found."
    stop_cluster
    exit 1
fi

stop_cluster

echo ""
echo "=============================================="
echo "ALL PHASE 2 INTEGRATION TESTS PASSED SUCCESSFULLY!"
echo "=============================================="
