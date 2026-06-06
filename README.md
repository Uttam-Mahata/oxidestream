# OxideStream

A distributed data processing engine built with a strict language boundary — a **Go Control Plane** for orchestration and a **Rust Data Plane** for execution. Conceptually analogous to Apache Spark, but designed around Go's concurrency model and Rust's zero-GC, SIMD-friendly execution.

All inter-node data is represented as Apache Arrow `RecordBatch`es. Shuffle transport uses **Apache Arrow Flight** (gRPC streaming), and SQL execution on workers uses **DataFusion**.

---

## Architecture

```
          ┌─────────────────────────────────────────┐
          │        Go Control Plane (Master)         │
          │                                          │
          │  REST API  │  DAG Scheduler  │  Raft KV  │
          │            │  Fair Scheduler │  Metadata │
          │            │  Streaming Sched│  Store    │
          └────────────────────┬────────────────────┘
                               │ gRPC (SubmitTask / CancelTask)
              ┌────────────────┼────────────────┐
              ▼                ▼                ▼
     ┌────────────────┐  ┌────────────────┐  ...
     │ Rust Worker 1  │  │ Rust Worker 2  │
     │                │  │                │
     │ DataFusion SQL │  │ DataFusion SQL │
     │ Arrow Flight ◄─┼──┼─► Arrow Flight │  ← shuffle transport
     └────────────────┘  └────────────────┘
```

**Communication:**
- Master → Workers: gRPC `SubmitTask` / `CancelTask` (defined in `proto/control.proto`)
- Workers → Master: gRPC `UpdateTaskStatus` with partition metadata and table statistics
- Workers ↔ Workers: Apache Arrow Flight for shuffle data transfer

---

## Prerequisites

- Go 1.25+
- Rust (with `cargo`, edition 2024)
- Python 3 (for test data generation)
- `protoc` + Go/Rust gRPC plugins (only needed to regenerate bindings from `proto/control.proto`)

---

## Build

```bash
# Control Plane (Go)
cd control-plane
go build -o control-plane-master

# Data Plane (Rust)
cd data-plane
cargo build
```

---

## Running a Cluster

### 1. Generate test data

```bash
python3 tests/generate_data.py         # Phase 1 data
python3 tests/generate_phase2_data.py  # Phase 2 data
python3 tests/generate_phase3_data.py  # Phase 3 data
```

### 2. Start the master

```bash
cd control-plane
./control-plane-master --grpc-port=50050 --http-port=8080
```

### 3. Start workers

```bash
cd data-plane
# Worker 1
./target/debug/data-plane \
  --worker-id=worker-1 --host=127.0.0.1 \
  --control-port=50051 --flight-port=50052 \
  --master-address=http://127.0.0.1:50050 \
  --data-dir=./tests/data-dir-worker-1

# Worker 2
./target/debug/data-plane \
  --worker-id=worker-2 --host=127.0.0.1 \
  --control-port=50053 --flight-port=50054 \
  --master-address=http://127.0.0.1:50050 \
  --data-dir=./tests/data-dir-worker-2
```

---

## Submitting Jobs

### Batch SQL (Map-Reduce)

```bash
curl -X POST http://localhost:8080/submit \
  -H 'Content-Type: application/json' \
  -d '{
    "map_sql":    "SELECT user_id, category_name, COUNT(1) as cnt, SUM(rating) as s FROM input GROUP BY user_id, category_name",
    "reduce_sql": "SELECT user_id, category_name, SUM(cnt) as total, SUM(s) as sum FROM input GROUP BY user_id, category_name",
    "input_files": ["tests/data/part-0.csv", "tests/data/part-1.csv"],
    "num_partitions": 2,
    "output_dir": "tests/output"
  }'
```

### Poll status

```bash
curl "http://localhost:8080/status?job_id=<job_id>"
```

### Distributed ML

```bash
# Linear Regression
curl -X POST http://localhost:8080/submit_lr \
  -H 'Content-Type: application/json' \
  -d '{"input_files": ["tests/data/ml_data.csv"], "num_partitions": 2, "output_dir": "tests/output_lr", "iterations": 3}'

# PageRank
curl -X POST http://localhost:8080/submit_pagerank \
  -H 'Content-Type: application/json' \
  -d '{"input_files": ["tests/data/graph_edges.csv"], "num_partitions": 2, "output_dir": "tests/output_pr", "iterations": 3}'
```

### Structured Streaming

```bash
curl -X POST http://localhost:8080/submit_streaming \
  -H 'Content-Type: application/json' \
  -d '{
    "input_dir": "tests/streaming_input",
    "checkpoint_file": "tests/checkpoint_status.json",
    "map_sql": "SELECT user_id, category_name, COUNT(1) as cnt FROM input GROUP BY user_id, category_name",
    "reduce_sql": "SELECT user_id, category_name, SUM(cnt) as total FROM input GROUP BY user_id, category_name",
    "num_partitions": 2,
    "output_dir": "tests/streaming_output"
  }'
```

### Kubernetes Operator Mode

```bash
./control-plane/control-plane-master --operator=tests/job_operator.yaml
```

The operator reads an `OxideStreamApplication` YAML, spawns the master and workers as child processes, monitors heartbeats, auto-restarts failed workers, and shuts everything down when the job completes.

---

## Integration Tests

```bash
bash tests/run_cluster.sh        # Phase 1: basic Map-Reduce + broadcast joins + AQE
bash tests/run_phase2_tests.sh   # Phase 2: operator, streaming, DPP, ML, graph
bash tests/run_phase3_tests.sh   # Phase 3: SQLite connector, push-based shuffle, codegen
```

> **Note:** The test scripts hardcode `WORKSPACE_DIR=/home/neutrino/oxidestream`. Update this variable to match your local path before running.

---

## Features

### Phase 1 — Core Distributed SQL
- **Map-Reduce DAG execution** — SQL queries are decomposed into parallel map tasks (one per input partition) followed by shuffle-reduce tasks
- **Apache Arrow shuffle** — shuffle files written as Arrow IPC on disk and served via Arrow Flight gRPC; reducers pull only their assigned partitions
- **Broadcast joins** — the cost-based optimizer detects small tables and broadcasts them to all map workers, avoiding shuffle entirely
- **Adaptive Query Execution (AQE)** — after map tasks report partition sizes, the scheduler coalesces small reduce partitions to avoid skew
- **Index shuffling** — mappers write an index file alongside the shuffle Arrow file for O(1) partition seeks

### Phase 2 — Enterprise Features
- **Structured streaming** — micro-batch scheduler polls an input directory every 2 seconds; supports event-time watermarking (late data filtering) and stateful checkpoint merging across batches
- **Dynamic Partition Pruning (DPP)** — pre-queries dimension tables, extracts matching join keys, and injects them as `IN (...)` filters into map tasks to skip irrelevant partitions
- **Distributed Linear Regression** — workers compute local partial matrices (XᵀX, XᵀY); master aggregates and solves the normal equations iteratively
- **Distributed PageRank** — iterative rank propagation across graph edge partitions with configurable convergence iterations
- **Kubernetes Operator Simulator** — lifecycle controller that spawns, monitors, and auto-heals worker processes based on a YAML CRD spec

### Phase 3 — Advanced Systems
- **Cost-Based Optimizer (CBO)** — workers compute column statistics (cardinality, null count, min/max) and report them to the master's metadata catalog; the scheduler uses these to order multi-way joins by estimated output size
- **Whole-stage code generation** — `codegen.rs` compiles simple arithmetic expressions into tight Arrow buffer loops, bypassing DataFusion's per-row virtual dispatch
- **Push-based shuffling + External Shuffle Service (ESS)** — mappers push partitions via Arrow Flight `DoPut` to a designated Merger Node; reducers read pre-merged sequential blocks, converting M random fetches into one sequential read
- **Fair Scheduler** — divides available worker task slots equally among concurrent jobs so short queries are not starved by large batch jobs
- **SQLite connector** — reads SQLite tables into Arrow `RecordBatch`es and registers them as DataFusion `MemTable`s for joining against distributed CSV data

---

## Project Structure

```
oxidestream/
├── control-plane/          # Go Master
│   ├── main.go             # gRPC + REST server, CLI entrypoint
│   └── pkg/
│       ├── scheduler/      # DAG scheduler, fair scheduler, DPP, ML/graph, streaming
│       ├── raft/           # In-memory metadata store (worker registry, table catalog)
│       ├── worker/         # Heartbeat tracker + failure detection
│       ├── operator/       # Kubernetes Operator Simulator
│       └── proto/          # Generated gRPC bindings
├── data-plane/             # Rust Workers
│   ├── src/
│   │   ├── main.rs         # Entry point
│   │   ├── control.rs      # gRPC client (register/heartbeat) + WorkerControl server
│   │   ├── executor.rs     # DataFusion SQL execution + Arrow shuffle partitioning
│   │   ├── flight.rs       # Arrow Flight server (shuffle data serving / ESS)
│   │   ├── streaming.rs    # Watermarking + stateful checkpoint merge
│   │   ├── connectors.rs   # SQLite → Arrow connector
│   │   ├── codegen.rs      # Whole-stage code generation
│   │   └── ml_graph.rs     # Local ML/graph partial computation
│   └── build.rs            # Compiles proto/control.proto via tonic-build
├── proto/
│   └── control.proto       # Shared gRPC service definitions
└── tests/
    ├── generate_data.py         # Phase 1 test data
    ├── generate_phase2_data.py  # Phase 2 test data
    ├── generate_phase3_data.py  # Phase 3 test data
    ├── run_cluster.sh           # Phase 1 integration test
    ├── run_phase2_tests.sh      # Phase 2 integration test
    ├── run_phase3_tests.sh      # Phase 3 integration test
    ├── submit_job.go            # Example job submission client
    └── job_operator.yaml        # Example OxideStreamApplication CRD
```
