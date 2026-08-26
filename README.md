# OxideStream

A distributed data processing engine built with a strict language boundary — a **Go Control Plane** for orchestration and a **Rust Data Plane** for execution. Conceptually analogous to Apache Spark, but designed around Go's concurrency model and Rust's zero-GC, SIMD-friendly execution.

All inter-node data is represented as Apache Arrow `RecordBatch`es. Shuffle transport uses **Apache Arrow Flight** (gRPC streaming), and SQL execution on workers uses **DataFusion**.

---

## Architecture

```
                         ┌──────────────────────────────────────┐
                         │       Go Control Plane (Master)       │
                         │                                       │
                         │  REST API  │  DAG Scheduler  │ Raft  │
                         │            │  Fair Scheduler  │ Meta  │
                         │            │  Streaming Sched │ Store │
                         └──────────────────────┬───────────────┘
                                                │ gRPC
                    ┌───────────────────────────┼───────────────────────────┐
                    ▼                           ▼                           ▼
           ┌────────────────┐          ┌────────────────┐          ┌────────────────┐
           │ Rust Worker 1  │          │ Rust Worker 2  │          │ Rust Worker N  │
           │                │          │                │          │                │
           │ DataFusion SQL │          │ DataFusion SQL │          │ DataFusion SQL │
           │ Arrow Flight ◄─┼──────────┼─► Arrow Flight │          │ Arrow Flight   │
           │ CodeGen        │  shuffle │ ESS            │          │ CodeGen        │
           └────────────────┘          └────────────────┘          └────────────────┘
```

**Communication Protocols:**
- **Master to Workers:** gRPC `SubmitTask` / `CancelTask` (defined in `proto/control.proto`)
- **Workers to Master:** gRPC `UpdateTaskStatus` with partition metadata and table statistics
- **Workers to Workers:** Apache Arrow Flight for shuffle data transfer

---

## Features

### Phase 1 — Core Distributed SQL
- **Map-Reduce DAG Execution** — SQL queries decomposed into parallel map tasks (one per input partition) followed by shuffle-reduce tasks
- **Apache Arrow Shuffle** — Shuffle files written as Arrow IPC on disk and served via Arrow Flight gRPC; reducers pull only their assigned partitions
- **Index Shuffling** — Mappers consolidate partitions into a single `.arrow` file with a JSON `.index` for O(1) partition seeks
- **Broadcast Joins** — Cost-based optimizer detects small tables and broadcasts them to all map workers, avoiding shuffle entirely
- **Adaptive Query Execution (AQE)** — After map tasks report partition sizes, the scheduler coalesces small reduce partitions to avoid skew

### Phase 2 — Enterprise Features
- **Structured Streaming** — Micro-batch scheduler polls an input directory every 2 seconds; supports event-time watermarking (late data filtering) and stateful checkpoint merging across batches
- **Dynamic Partition Pruning (DPP)** — Pre-queries dimension tables, extracts matching join keys, and injects them as `IN (...)` filters into map tasks to skip irrelevant partitions
- **Distributed Linear Regression** — Workers compute local partial matrices (X^T X, X^T Y); master aggregates and solves the normal equations iteratively
- **Distributed PageRank** — Iterative rank propagation across graph edge partitions with configurable convergence iterations
- **Kubernetes Operator Simulator** — Lifecycle controller that spawns, monitors, and auto-heals worker processes based on a YAML CRD spec

### Phase 3 — Advanced Systems
- **Cost-Based Optimizer (CBO)** — Workers compute column statistics (cardinality, null count, min/max) and report them to the master's metadata catalog; the scheduler uses these to order multi-way joins by estimated output size
- **Whole-Stage Code Generation** — `codegen.rs` compiles simple arithmetic expressions into tight Arrow buffer loops, bypassing DataFusion's per-row virtual dispatch
- **Push-based Shuffling + External Shuffle Service (ESS)** — Mappers push partitions via Arrow Flight `DoPut` to a designated Merger Node; reducers read pre-merged sequential blocks, converting M random fetches into one sequential read
- **Fair Scheduler** — Divides available worker task slots equally among concurrent jobs so short queries are not starved by large batch jobs
- **SQLite Connector** — Reads SQLite tables into Arrow `RecordBatch`es and registers them as DataFusion `MemTable`s for joining against distributed CSV data

---

## Prerequisites

- **Go** 1.25+
- **Rust** (with `cargo`, edition 2024)
- **Python 3** (for test data generation scripts)
- **protoc** + Go/Rust gRPC plugins (only needed to regenerate bindings from `proto/control.proto`)
- **Node.js** + npm (for the Web UI dashboard)

---

## Quick Start

### Build

```bash
# Control Plane (Go)
cd control-plane
go build -o control-plane-master
cd ..

# Data Plane (Rust)
cd data-plane
cargo build
cd ..
```

### Generate Test Data

```bash
python3 tests/generate_data.py         # Phase 1 data
python3 tests/generate_phase2_data.py  # Phase 2 data
python3 tests/generate_phase3_data.py  # Phase 3 data
```

### Start the Master

```bash
cd control-plane
./control-plane-master --grpc-port=50050 --http-port=8080
```

### Start Workers

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

### Submit a Job

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

---

## Docker Quick Start

```bash
docker-compose up --build
```

This builds and starts the control plane container. Expose worker ports via `docker-compose.yml` or run workers natively against the containerized master.

---

## Web UI

The project includes a React + TypeScript + Vite dashboard in the `ui/` directory.

### Start the Dashboard

```bash
cd ui
npm install
npm run dev
```

The dashboard is served at `http://localhost:5173` and provides:
- Job submission and monitoring
- Real-time worker status and heartbeats
- System metrics visualization
- Task-level progress tracking

---

## API Reference

The control plane exposes a REST HTTP API on the configured `--http-port` (default `8080`).

For the full API reference with request/response schemas and curl examples, see [docs/API.md](docs/API.md).

### Endpoints at a Glance

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/submit` | Submit a batch MapReduce SQL job |
| `POST` | `/submit_lr` | Submit a distributed linear regression job |
| `POST` | `/submit_pagerank` | Submit a distributed PageRank job |
| `POST` | `/submit_streaming` | Submit a structured streaming job |
| `GET` | `/status?job_id=X` | Get the status of a specific job |
| `GET` | `/jobs` | List all jobs |
| `GET` | `/jobs/{job_id}/tasks` | List tasks for a specific job |
| `POST` | `/jobs/{job_id}/cancel` | Cancel a running or pending job |
| `GET` | `/workers` | List registered workers |
| `GET` | `/queue_depth` | Get the number of pending tasks |
| `GET` | `/metrics` | Get system metrics (JSON) |
| `GET` | `/health` | Health check |
| `GET` | `/metrics/prometheus` | Prometheus-compatible metrics |

---

## Configuration

### Control Plane (Go Master)

| Flag | Default | Description |
|------|---------|-------------|
| `--grpc-port` | `50050` | Port for the gRPC Control Plane server |
| `--http-port` | `8080` | Port for the REST HTTP API server |
| `--submit` | `false` | Run in client mode to submit a job |
| `--sql` | `""` | SQL query to execute (client mode) |
| `--inputs` | `""` | Comma-separated input CSV files (client mode) |
| `--partitions` | `1` | Number of partitions (client mode) |
| `--output` | `""` | Output directory (client mode) |
| `--operator` | `""` | Path to OxideStreamApplication YAML for operator mode |

### Data Plane (Rust Worker)

| Flag | Default | Description |
|------|---------|-------------|
| `--worker-id` | `worker-1` | Unique identifier for this worker |
| `--host` | `127.0.0.1` | Hostname or IP of this worker |
| `--control-port` | `50051` | Port for the worker's gRPC control server |
| `--flight-port` | `50052` | Port for the Arrow Flight shuffle server |
| `--master-address` | `http://127.0.0.1:50050` | Address of the control plane master |
| `--data-dir` | `./data` | Local directory for shuffle and output data |

### Operator Mode

```bash
./control-plane/control-plane-master --operator=tests/job_operator.yaml
```

The operator reads an `OxideStreamApplication` YAML CRD, spawns the master and workers as child processes, monitors heartbeats, auto-restarts failed workers, and shuts everything down when the job completes.

---

## Project Structure

```
oxidestream/
├── control-plane/              # Go Control Plane (Master)
│   ├── main.go                 # gRPC + REST server, CLI entrypoint
│   └── pkg/
│       ├── scheduler/          # DAG scheduler, fair scheduler, DPP, ML/graph, streaming
│       ├── raft/               # In-memory metadata store (worker registry, table catalog)
│       ├── worker/             # Heartbeat tracker + failure detection
│       ├── operator/           # Kubernetes Operator Simulator
│       ├── metrics/            # Prometheus metrics collectors
│       ├── auth/               # API key auth, CORS, rate limiting, request logging
│       ├── config/             # Environment-based configuration management
│       └── proto/              # Generated gRPC bindings
├── data-plane/                 # Rust Workers
│   ├── src/
│   │   ├── main.rs             # Entry point, CLI parsing
│   │   ├── control.rs          # gRPC client (register/heartbeat) + WorkerControl server
│   │   ├── executor.rs         # DataFusion SQL execution + Arrow shuffle partitioning
│   │   ├── flight.rs           # Arrow Flight server (shuffle data serving / ESS)
│   │   ├── streaming.rs        # Watermarking + stateful checkpoint merge
│   │   ├── connectors.rs       # SQLite to Arrow connector
│   │   ├── codegen.rs          # Whole-stage code generation
│   │   ├── ml_graph.rs         # Local ML/graph partial computation
│   │   └── http_server.rs      # Axum HTTP server (health, metrics, tasks)
│   └── build.rs                # Compiles proto/control.proto via tonic-build
├── proto/
│   └── control.proto           # Shared gRPC service definitions
├── ui/                         # React + TypeScript + Vite Web Dashboard
│   ├── src/
│   │   ├── api.ts              # REST API client
│   │   ├── types.ts            # TypeScript interfaces
│   │   ├── components/         # Sidebar, shared components
│   │   └── pages/              # Dashboard, SubmitJob, Jobs, JobDetail, Workers, Streaming
│   └── package.json
├── tests/
│   ├── generate_data.py         # Phase 1 test data generation
│   ├── generate_phase2_data.py  # Phase 2 test data generation
│   ├── generate_phase3_data.py  # Phase 3 test data generation
│   ├── run_cluster.sh           # Phase 1 integration tests
│   ├── run_phase2_tests.sh      # Phase 2 integration tests
│   ├── run_phase3_tests.sh      # Phase 3 integration tests
│   ├── submit_job.go            # Example job submission client
│   └── job_operator.yaml        # Example OxideStreamApplication CRD
├── docs/
│   ├── API.md                   # Detailed API documentation
│   └── ARCHITECTURE.md          # Architecture deep dive
├── Dockerfile.control-plane     # Multi-stage Docker build for master
├── Dockerfile.data-plane        # Multi-stage Docker build for workers
├── Dockerfile.ui                # Multi-stage Docker build for UI (nginx)
├── docker-compose.yml           # Full stack composition
├── docker-compose.dev.yml       # Development override with live reload
├── nginx.conf                   # SPA routing + API proxy for UI
├── .dockerignore                # Docker build exclusions
├── .env.example                 # Documented environment variables
├── LICENSE                      # MIT License
└── README.md
```

---

## Testing

### Integration Test Scripts

```bash
bash tests/run_cluster.sh        # Phase 1: Map-Reduce + broadcast joins + AQE
bash tests/run_phase2_tests.sh   # Phase 2: operator, streaming, DPP, ML, graph
bash tests/run_phase3_tests.sh   # Phase 3: SQLite connector, push-based shuffle, codegen
```

Each test script starts the master, two executor workers, and a co-located shuffle service per worker. Scripts derive `WORKSPACE_DIR` from their own location and run from any checkout without editing.

### Unit Tests

```bash
# Control Plane
cd control-plane && go test ./...

# Data Plane
cd data-plane && cargo test
```

---

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Commit your changes (`git commit -am 'Add new feature'`)
4. Push to the branch (`git push origin feature/my-feature`)
5. Open a Pull Request

### Code Style

- **Go:** Follow standard `gofmt` conventions. Run `gofmt -w .` before committing.
- **Rust:** Follow `cargo fmt` conventions. Run `cargo fmt` before committing.
- **Protobuf:** Regenerate bindings after editing `proto/control.proto` using `make proto` or the tonic-build / protoc-gen-go plugins.
- **TypeScript:** Run `npm run lint` in the `ui/` directory before committing.

### Pull Request Guidelines

- Keep PRs focused on a single feature or bugfix
- Include integration test coverage for new functionality
- Update documentation in `docs/` for API or architectural changes
- Ensure all CI checks pass before requesting review

---

## License

MIT License. See [LICENSE](LICENSE) for details.
