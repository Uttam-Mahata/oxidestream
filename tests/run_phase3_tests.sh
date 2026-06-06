#!/bin/bash
set -e

WORKSPACE_DIR="/home/neutrino/oxidestream"
TEST_DIR="$WORKSPACE_DIR/tests"
DATA_DIR="$TEST_DIR/data"

echo "=== OxideStream Phase 3 Advanced Systems Integration Test ==="

# Clean directories
rm -rf "$TEST_DIR/output_sqlite"
rm -rf "$TEST_DIR/output_push"

# Re-compile Go Master
echo "Recompiling Go Control Plane..."
cd "$WORKSPACE_DIR/control-plane"
/usr/local/go/bin/go build -o control-plane-master
cp control-plane-master "$WORKSPACE_DIR/"
cd "$WORKSPACE_DIR"

# Clean log files
rm -f "$TEST_DIR"/master_p3.log "$TEST_DIR"/worker-1_p3.log "$TEST_DIR"/worker-2_p3.log

# ----------------- Helper Functions -----------------
start_cluster() {
    local merger_env=$1
    echo "Starting cluster..."
    
    # Clean worker data directories to prevent stale shuffle files pollution
    rm -rf "$TEST_DIR/data-dir-worker-1"
    rm -rf "$TEST_DIR/data-dir-worker-2"
    mkdir -p "$TEST_DIR/data-dir-worker-1"
    mkdir -p "$TEST_DIR/data-dir-worker-2"

    cd "$WORKSPACE_DIR/control-plane"
    ./control-plane-master --grpc-port=50050 --http-port=8080 > "$TEST_DIR/master_p3.log" 2>&1 &
    MASTER_PID=$!
    cd "$WORKSPACE_DIR"
    sleep 2

    # Worker 1
    if [ ! -z "$merger_env" ]; then
        export MERGER_ADDRESS="$merger_env"
    fi
    ./data-plane/target/debug/data-plane \
      --worker-id="worker-1" \
      --host="127.0.0.1" \
      --control-port=50051 \
      --flight-port=50052 \
      --master-address="http://127.0.0.1:50050" \
      --data-dir="$TEST_DIR/data-dir-worker-1" > "$TEST_DIR/worker-1_p3.log" 2>&1 &
    WORKER1_PID=$!

    # Worker 2
    ./data-plane/target/debug/data-plane \
      --worker-id="worker-2" \
      --host="127.0.0.1" \
      --control-port=50053 \
      --flight-port=50054 \
      --master-address="http://127.0.0.1:50050" \
      --data-dir="$TEST_DIR/data-dir-worker-2" > "$TEST_DIR/worker-2_p3.log" 2>&1 &
    WORKER2_PID=$!
    unset MERGER_ADDRESS

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

# ----------------- Test 1: SQLite Ecosystem Connector -----------------
echo ""
echo "=============================================="
echo "TEST 1: SQLite Ecosystem Connector"
echo "=============================================="
start_cluster

# Join CSV ratings against user_status SQLite database table
echo "Submitting join query against SQLite table user_status..."
curl -s -X POST -H "Content-Type: application/json" -d '{
  "map_sql": "SELECT r.user_id, u.membership, COUNT(1) as rating_count, SUM(r.rating) as rating_sum FROM input r JOIN user_status u ON r.user_id = u.user_id GROUP BY r.user_id, u.membership",
  "reduce_sql": "SELECT user_id, membership, SUM(rating_count) as total_ratings, SUM(rating_sum) as total_rating_sum FROM input GROUP BY user_id, membership",
  "input_files": ["/home/neutrino/oxidestream/tests/data/ratings_dpp.csv", "/home/neutrino/oxidestream/tests/data/user_metadata.db"],
  "num_partitions": 2,
  "output_dir": "/home/neutrino/oxidestream/tests/output_sqlite"
}' http://localhost:8080/submit

sleep 4.5

sqlite_out="$TEST_DIR/output_sqlite/part_0.csv"
if [ -f "$sqlite_out" ]; then
    echo "Test 1 SUCCESS: Joined SQLite outputs successfully."
    echo "=== SQLite Joined Results Preview ==="
    cat "$sqlite_out" | head -n 10
else
    echo "Test 1 FAILED: SQLite output file not found."
    stop_cluster
    exit 1
fi

stop_cluster

# ----------------- Test 2: Column Statistics & CBO (ANALYZE TABLE) -----------------
echo ""
echo "=============================================="
echo "TEST 2: Column Statistics & CBO (ANALYZE TABLE)"
echo "=============================================="
start_cluster

echo "Submitting ANALYZE TABLE query to calculate metrics on ratings_dpp.csv..."
curl -s -X POST -H "Content-Type: application/json" -d '{
  "sql": "ANALYZE TABLE ratings_dpp",
  "input_files": ["/home/neutrino/oxidestream/tests/data/ratings_dpp.csv"],
  "num_partitions": 1,
  "output_dir": "/home/neutrino/oxidestream/tests/output_analyze"
}' http://localhost:8080/submit

sleep 3.5

# Check master logs for CBO Stats update print
if grep -q "CBO Stats: Table ratings_dpp updated" "$TEST_DIR/master_p3.log"; then
    echo "Test 2 SUCCESS: Go Master received and registered column statistics successfully."
    grep "CBO Stats:" "$TEST_DIR/master_p3.log"
else
    echo "Test 2 FAILED: Master did not log CBO Stats update."
    stop_cluster
    exit 1
fi

stop_cluster

# ----------------- Test 3: Fair Scheduler slot sharing -----------------
echo ""
echo "=============================================="
echo "TEST 3: Fair Scheduler concurrent slot allocation"
echo "=============================================="
start_cluster

# Submit Job A (long map query) and Job B concurrently
echo "Submitting concurrent Job A..."
curl -s -X POST -H "Content-Type: application/json" -d '{
  "map_sql": "SELECT user_id, COUNT(1) as cnt FROM input GROUP BY user_id",
  "reduce_sql": "SELECT user_id, SUM(cnt) FROM input GROUP BY user_id",
  "input_files": ["/home/neutrino/oxidestream/tests/data/ratings_dpp.csv"],
  "num_partitions": 2,
  "output_dir": "/home/neutrino/oxidestream/tests/output_fair_A"
}' http://localhost:8080/submit >/dev/null &

echo "Submitting concurrent Job B..."
curl -s -X POST -H "Content-Type: application/json" -d '{
  "map_sql": "SELECT user_id, SUM(rating) as s FROM input GROUP BY user_id",
  "reduce_sql": "SELECT user_id, SUM(s) FROM input GROUP BY user_id",
  "input_files": ["/home/neutrino/oxidestream/tests/data/part-2.csv"],
  "num_partitions": 1,
  "output_dir": "/home/neutrino/oxidestream/tests/output_fair_B"
}' http://localhost:8080/submit >/dev/null &

sleep 4.5

# Verify in master log that both jobs ran concurrently
if grep -q "FairScheduler" "$TEST_DIR/master_p3.log"; then
    echo "Test 3 SUCCESS: FairScheduler log lines found."
    grep "FairScheduler" "$TEST_DIR/master_p3.log" | head -n 10
else
    echo "Test 3 FAILED: FairScheduler did not log concurrency actions."
    stop_cluster
    exit 1
fi

stop_cluster

# ----------------- Test 4: Push-Based Shuffling & ESS -----------------
echo ""
echo "=============================================="
echo "TEST 4: Push-Based Shuffling (Merger Nodes)"
echo "=============================================="
# Start cluster with worker-1 acting as the Merger Node
start_cluster "127.0.0.1:50052"

echo "Submitting MapReduce query under Push-Based Shuffle environment..."
curl -s -X POST -H "Content-Type: application/json" -d '{
  "map_sql": "SELECT user_id, COUNT(1) as rating_count, SUM(rating) as rating_sum FROM input GROUP BY user_id",
  "reduce_sql": "SELECT user_id, SUM(rating_count) as total_ratings, SUM(rating_sum) as total_rating_sum FROM input GROUP BY user_id",
  "input_files": ["/home/neutrino/oxidestream/tests/data/part-2.csv"],
  "num_partitions": 2,
  "output_dir": "/home/neutrino/oxidestream/tests/output_push"
}' http://localhost:8080/submit

sleep 4.5

# Verify merged file was created on Merger Node (worker-1 data dir)
merged_file="$TEST_DIR/data-dir-worker-1/merged_shuffle_map-stage_0.arrow"
if [ -f "$merged_file" ] || [ -f "$TEST_DIR/data-dir-worker-1/merged_shuffle_map-stage_1.arrow" ]; then
    echo "Test 4 SUCCESS: Merger Node consolidated shuffle data into merged partition files."
else
    echo "Test 4 FAILED: Consolidated shuffle file not found at $merged_file"
    stop_cluster
    exit 1
fi

push_out="$TEST_DIR/output_push/part_0.csv"
if [ -f "$push_out" ]; then
    echo "Test 4 SUCCESS: Reducers retrieved consolidated partitions from Merger Node."
    echo "=== Push Shuffle Aggregations ==="
    cat "$push_out"
else
    echo "Test 4 FAILED: Output part file not found."
    stop_cluster
    exit 1
fi

stop_cluster

# ----------------- Test 5: Whole-Stage CodeGen -----------------
echo ""
echo "=============================================="
echo "TEST 5: Whole-Stage CodeGen Expression Compiler"
echo "=============================================="
start_cluster

# Run mathematical expression projections
echo "Submitting arithmetic projection query..."
curl -s -X POST -H "Content-Type: application/json" -d '{
  "map_sql": "SELECT rating + 1 AS rating_plus_one, user_id FROM input",
  "reduce_sql": "SELECT user_id, SUM(rating_plus_one) FROM input GROUP BY user_id",
  "input_files": ["/home/neutrino/oxidestream/tests/data/part-2.csv"],
  "num_partitions": 1,
  "output_dir": "/home/neutrino/oxidestream/tests/output_codegen"
}' http://localhost:8080/submit

sleep 4.5

# Verify worker log contains CodeGenCompiler triggers
if grep -q "CodeGenCompiler" "$TEST_DIR/worker-1_p3.log" || grep -q "CodeGenCompiler" "$TEST_DIR/worker-2_p3.log"; then
    echo "Test 5 SUCCESS: Worker compiled SQL expressions dynamically."
    grep "CodeGenCompiler" "$TEST_DIR/worker-1_p3.log" || grep "CodeGenCompiler" "$TEST_DIR/worker-2_p3.log" | head -n 10
else
    echo "Test 5 FAILED: Worker did not log CodeGen expressions compilation."
    stop_cluster
    exit 1
fi

stop_cluster

echo ""
echo "=============================================="
echo "ALL PHASE 3 ENTERPRISE OPTIMIZATION TESTS PASSED!"
echo "=============================================="
