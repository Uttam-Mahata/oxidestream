#!/bin/bash
set -e

WORKSPACE_DIR="/home/neutrino/oxidestream"
TEST_DIR="$WORKSPACE_DIR/tests"
OUTPUT_DIR="$TEST_DIR/output"

# Clean up past run files
echo "Cleaning up past run files..."
rm -rf "$OUTPUT_DIR"
rm -rf "$TEST_DIR/data-dir-worker-1"
rm -rf "$TEST_DIR/data-dir-worker-2"
mkdir -p "$OUTPUT_DIR"
mkdir -p "$TEST_DIR/data-dir-worker-1"
mkdir -p "$TEST_DIR/data-dir-worker-2"

# Start the Go Master in background
echo "Starting Go Master..."
cd "$WORKSPACE_DIR/control-plane"
./control-plane-master --grpc-port=50050 --http-port=8080 > "$TEST_DIR/master.log" 2>&1 &
MASTER_PID=$!

# Give Master a moment to bind to gRPC port
sleep 2

# Trap signals to clean up background tasks
cleanup() {
    echo "Shutting down Go Master and Rust Workers..."
    kill $MASTER_PID || true
    kill $WORKER1_PID || true
    kill $WORKER2_PID || true
    wait $MASTER_PID $WORKER1_PID $WORKER2_PID 2>/dev/null || true
    echo "Cleanup complete."
}
trap cleanup EXIT

# Start Worker 1
echo "Starting Worker 1..."
cd "$WORKSPACE_DIR/data-plane"
./target/debug/data-plane \
  --worker-id="worker-1" \
  --host="127.0.0.1" \
  --control-port=50051 \
  --flight-port=50052 \
  --master-address="http://127.0.0.1:50050" \
  --data-dir="$TEST_DIR/data-dir-worker-1" > "$TEST_DIR/worker-1.log" 2>&1 &
WORKER1_PID=$!

# Start Worker 2
echo "Starting Worker 2..."
./target/debug/data-plane \
  --worker-id="worker-2" \
  --host="127.0.0.1" \
  --control-port=50053 \
  --flight-port=50054 \
  --master-address="http://127.0.0.1:50050" \
  --data-dir="$TEST_DIR/data-dir-worker-2" > "$TEST_DIR/worker-2.log" 2>&1 &
WORKER2_PID=$!

# Sleep for 3 seconds to let them register
echo "Sleeping for 3 seconds to allow registration and heartbeat updates..."
sleep 3

# Verify registration
echo "Listing registered workers from Master REST API..."
curl -s http://localhost:8080/workers

# Run the job submission script
echo "Running submit_job.go..."
cd "$WORKSPACE_DIR"
/usr/local/go/bin/go run "$TEST_DIR/submit_job.go"

# Verify outputs
echo "Verifying final outputs..."
if [ ! -d "$OUTPUT_DIR" ] || [ -z "$(ls -A "$OUTPUT_DIR")" ]; then
    echo "Error: Output directory is empty or does not exist!"
    exit 1
fi

echo "Output files found:"
ls -la "$OUTPUT_DIR"

for f in "$OUTPUT_DIR"/part_*.csv; do
    echo "=== Content of $f ==="
    cat "$f"
    echo "======================"
done

echo "Integration test run succeeded!"
