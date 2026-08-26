# OxideStream Architecture

OxideStream is a distributed data processing engine with a strict language boundary: a **Go Control Plane** for cluster orchestration and a **Rust Data Plane** for high-performance data execution. This document describes the internal architecture, communication protocols, and data flow through the system.

---

## System Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Go Control Plane                             │
│                                                                     │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐             │
│  │   REST API    │  │ DAG Scheduler │  │ Raft Metadata│             │
│  │  (HTTP/JSON)  │  │ Fair Scheduler│  │    Store     │             │
│  │               │  │ Streaming     │  │  (Worker     │             │
│  │  /submit      │  │ Scheduler     │  │   Registry,  │             │
│  │  /status      │  │              │  │   Table      │             │
│  │  /jobs        │  │  DPP Engine   │  │   Catalog)   │             │
│  │  /workers     │  │  CBO Engine   │  │              │             │
│  │  /metrics     │  │              │  │              │             │
│  └──────┬───────┘  └──────┬───────┘  └──────────────┘             │
│         │                  │                                        │
│  ┌──────┴───────┐  ┌──────┴───────┐  ┌──────────────┐             │
│  │ Worker Tracker│  │   Operator   │  │  Prometheus   │             │
│  │  (Heartbeats, │  │  (K8s Sim)   │  │  Metrics     │             │
│  │   Failure     │  │              │  │              │             │
│  │   Detection)  │  │              │  │              │             │
│  └──────────────┘  └──────────────┘  └──────────────┘             │
└────────────────────────────┬────────────────────────────────────────┘
                             │ gRPC
            ┌────────────────┼────────────────┐
            ▼                ▼                ▼
   ┌────────────────┐ ┌────────────────┐ ┌────────────────┐
   │  Rust Worker 1  │ │  Rust Worker 2  │ │  Rust Worker N  │
   │                 │ │                 │ │                 │
   │ ┌─────────────┐ │ │ ┌─────────────┐ │ │ ┌─────────────┐ │
   │ │ DataFusion   │ │ │ │ DataFusion   │ │ │ │ DataFusion   │ │
   │ │ SessionCtx  │ │ │ │ SessionCtx  │ │ │ │ SessionCtx  │ │
   │ └─────────────┘ │ │ └─────────────┘ │ │ └─────────────┘ │
   │ ┌─────────────┐ │ │ ┌─────────────┐ │ │ ┌─────────────┐ │
   │ │ Arrow Flight │ │ │ │ Arrow Flight │ │ │ │ Arrow Flight │ │
   │ │   Server    │ │ │ │   Server    │ │ │ │   Server    │ │
   │ │ (ESS)       │ │ │ │ (ESS)       │ │ │ │ (ESS)       │ │
   │ └─────────────┘ │ │ └─────────────┘ │ │ └─────────────┘ │
   │ ┌─────────────┐ │ │ ┌─────────────┐ │ │ ┌─────────────┐ │
   │ │  CodeGen    │ │ │ │  CodeGen    │ │ │ │  CodeGen    │ │
   │ └─────────────┘ │ │ └─────────────┘ │ │ └─────────────┘ │
   └────────────────┘ └────────────────┘ └────────────────┘
            ▲                ▲                ▲
            │  Arrow Flight   │                │
            └────────────────┴────────────────┘
                     Shuffle Transport
```

---

## Control Plane Internals

The control plane is implemented in Go and runs as a single master process.

### Scheduler

The scheduler manages the full lifecycle of job execution. It lives in `control-plane/pkg/scheduler/`.

**DAG Scheduler** — The core scheduling engine. When a job is submitted, it:
1. Decomposes the query into a Map stage and a Reduce stage
2. Creates one map task per input partition
3. Schedules map tasks on available workers using the fair scheduler
4. After map completion, collects partition sizes from task status updates
5. Applies AQE partition coalescing to merge small partitions
6. Schedules reduce tasks on workers, specifying shuffle input sources

**Fair Scheduler** — Divides available worker task slots equally among concurrent jobs. Prevents short interactive queries from being starved by large batch jobs. Implemented in `control-plane/pkg/scheduler/fair_scheduler.go`.

**Streaming Scheduler** — Polls an input directory every 2 seconds for new files. Groups new files into micro-batches and triggers map-reduce iterations. Supports event-time watermarking and stateful checkpoint merging between batches.

### Metadata Store

An in-memory Raft-based metadata store (`control-plane/pkg/raft/`) that maintains:

- **Worker Registry** — Worker ID, host, ports, CPU/memory capacity, last heartbeat timestamp, and active status
- **Table Catalog** — Column statistics (cardinality, null count, min/max) collected from `ANALYZE TABLE` operations, used by the CBO for join ordering

The store simulates multi-node consensus by registering peer metadata stores, providing the foundation for future distributed metadata replication.

### Worker Tracker

The worker tracker (`control-plane/pkg/worker/`) monitors worker liveness:

- Receives periodic gRPC heartbeats with CPU usage, memory usage, and active task counts
- Tracks `last_active` timestamps per worker
- Triggers failure detection when a worker misses heartbeats beyond the configured timeout (default: 10 seconds)
- Notifies the scheduler of worker failures, which reassigns pending and running tasks to surviving workers

### Cost-Based Optimizer (CBO)

The CBO operates at two levels:

1. **Job-level broadcast detection** — Before scheduling, evaluates input file sizes. Files below a broadcast threshold (e.g., 50KB) are classified as broadcast tables and registered directly on all workers, bypassing shuffle entirely.
2. **Join ordering** — After `ANALYZE TABLE` collects column statistics, the CBO orders multi-way joins by estimated output size, ensuring the most selective joins execute first.

### Dynamic Partition Pruning (DPP)

When joining a partitioned fact table against a dimension table:

1. The scheduler runs a lightweight pre-query on the dimension file with the specified filter
2. Collects matching join keys from the pre-query result
3. Injects `AND <join_key> IN (...)` predicates into the map SQL for the fact table
4. Mappers skip reading partitions that cannot match, reducing I/O

---

## Data Plane Internals

Each worker is a Rust process running in `data-plane/`.

### Executor

The executor (`data-plane/src/executor.rs`) handles all SQL execution:

- Creates a DataFusion `SessionContext` and registers input CSV files as `MemTable`s
- Executes the map SQL query, producing Arrow `RecordBatch` results
- Hash-partitions the output by the specified key columns
- Writes consolidated Arrow IPC files to the local data directory
- Generates JSON index files for O(1) partition seeks during shuffle retrieval

### Arrow Flight Server

Each worker runs an Arrow Flight gRPC server (`data-plane/src/flight.rs`) on a dedicated port:

- Serves partition data for shuffle retrieval by other workers
- Supports range-based reads using index files (offset + length)
- Operates as an External Shuffle Service (ESS) on an isolated thread, remaining available even if the executor crashes or runs out of memory
- Supports push-based shuffling via `DoPut`, where mappers send partitions to a designated Merger Node

### Shuffle Mechanism

OxideStream implements two shuffle strategies:

**Pull-based (default):**
1. Mappers write consolidated `.arrow` files with `.index` metadata
2. Reducers request specific partition ranges via Arrow Flight `Get`
3. The Flight server reads the index, seeks to the offset, and streams the byte range

**Push-based (Phase 3):**
1. Mappers push partition data via Arrow Flight `DoPut` to a Merger Node assigned by the master
2. The Merger Node appends record batches to consolidated files: `merged_shuffle_<stage_id>_<partition_id>.arrow`
3. Reducers read a single pre-merged sequential block, eliminating random I/O from multiple mapper fetches

### Code Generation

The code generator (`data-plane/src/codegen.rs`) compiles AST expressions into optimized Rust closures that operate directly over Arrow `PrimitiveArray` buffers:

- Bypasses DataFusion's per-row virtual function dispatch
- Compiles arithmetic expressions, filters, and projections into unified CPU register loops
- Particularly effective for simple expressions evaluated across large datasets

### Streaming and Watermarking

The streaming executor (`data-plane/src/streaming.rs`) handles micro-batch processing:

- **Watermarking** — Parses the event-time column from each record and maintains a running maximum. Records older than `max_timestamp - watermark_threshold` are dropped as late data.
- **Checkpoint merging** — After each micro-batch, workers save cumulative aggregates (counts, sums) to a JSON checkpoint file. The next micro-batch loads the checkpoint, merges it with new results, and writes the updated state.

### SQLite Connector

The connector (`data-plane/src/connectors.rs`) enables joining distributed data with local database tables:

- Reads rows from a SQLite table using `rusqlite`
- Packs rows into Arrow `RecordBatch` streams
- Registers them as DataFusion `MemTable`s in the session context
- Allows SQL queries to join CSV-based distributed tables against SQLite tables

### ML and Graph Computation

The ML/graph module (`data-plane/src/ml_graph.rs`) implements:

- **Linear Regression** — Mappers compute local X^T X and X^T Y partial matrices. Reducers aggregate matrices and solve the normal equations using Gauss-Jordan elimination with partial pivoting.
- **PageRank** — Mappers compute per-node rank contributions (rank / out_degree). Reducers sum contributions and apply the damping factor (0.15 + 0.85 * sum).

---

## Communication Protocols

### gRPC (Control Plane <-> Data Plane)

Defined in `proto/control.proto`. Two services:

**ControlPlane** (implemented by Go Master):
- `RegisterWorker` — Workers register on startup with their ID, host, ports, and hardware capacity
- `Heartbeat` — Workers send periodic heartbeats with CPU/memory usage and active task counts
- `UpdateTaskStatus` — Workers report task completion/failure with partition metadata and table statistics

**WorkerControl** (implemented by Rust Workers):
- `SubmitTask` — Master sends task definitions (SQL, input files, shuffle sources, broadcast tables)
- `CancelTask` — Master requests task cancellation
- `PlanQuery` — Master asks a worker to compile a single SQL statement into a 2-stage map/reduce plan

### Arrow Flight (Data Plane <-> Data Plane)

Used for inter-worker shuffle data transfer:

- **Get** — Reducers fetch specific partition data from mapper workers using ticket-based byte range requests
- **DoPut** — Push-based shuffle sends partition data to Merger Nodes
- **ListFlights** — Workers can enumerate available partitions on a peer

### REST HTTP (External <-> Control Plane)

JSON-based REST API for job submission, monitoring, and cluster management. See [API.md](API.md) for the complete endpoint reference.

---

## Data Flow

### Batch SQL Execution

```
Client                 Master                    Worker 1              Worker 2
  │                      │                         │                     │
  │  POST /submit        │                         │                     │
  │─────────────────────>│                         │                     │
  │                      │  SubmitTask (MAP)       │                     │
  │                      │────────────────────────>│                     │
  │                      │  SubmitTask (MAP)       │                     │
  │                      │──────────────────────────────────────────────>│
  │  {job_id, status}    │                         │                     │
  │<─────────────────────│                         │                     │
  │                      │                         │                     │
  │                      │                         │ Read CSV + SQL      │
  │                      │                         │ Write .arrow + .index
  │                      │  UpdateTaskStatus       │                     │
  │                      │<────────────────────────│                     │
  │                      │                         │                     │
  │                      │                         │ Read CSV + SQL      │
  │                      │                         │ Write .arrow + .index
  │                      │  UpdateTaskStatus       │                     │
  │                      │<─────────────────────────────────────────────│
  │                      │                         │                     │
  │                      │  [AQE: coalesce]        │                     │
  │                      │                         │                     │
  │                      │  SubmitTask (REDUCE)    │                     │
  │                      │────────────────────────>│                     │
  │                      │  SubmitTask (REDUCE)    │                     │
  │                      │──────────────────────────────────────────────>│
  │                      │                         │                     │
  │                      │                         │ Arrow Flight Get    │
  │                      │                         │<────────────────────│
  │                      │                         │ Fetch partitions    │
  │                      │                         │ from Worker 2       │
  │                      │                         │                     │
  │                      │                         │ Run reduce SQL      │
  │                      │                         │ Write output CSV    │
  │  {status: COMPLETED} │  UpdateTaskStatus       │                     │
  │<─────────────────────│<────────────────────────│                     │
```

### Structured Streaming Flow

```
Input Directory          Master                    Workers
  │                       │                         │
  │ New files appear      │                         │
  │                       │ Scan directory           │
  │                       │ Group into micro-batch   │
  │                       │                         │
  │                       │ SubmitTask (MAP)         │
  │                       │────────────────────────>│
  │                       │                         │ Apply watermark filter
  │                       │                         │ Execute SQL
  │                       │                         │ Merge with checkpoint
  │                       │  UpdateTaskStatus        │
  │                       │<────────────────────────│
  │                       │                         │
  │                       │ SubmitTask (REDUCE)      │
  │                       │────────────────────────>│
  │                       │                         │ Write output + state
  │                       │                         │
  │ [wait 2 seconds]      │                         │
  │                       │ Scan again              │
  │                       │ [repeat]                │
```

---

## Kubernetes Operator Mode

The operator (`control-plane/pkg/operator/`) simulates a Kubernetes controller:

1. Parses an `OxideStreamApplication` YAML CRD specifying master ports, worker replica count, and job configuration
2. Spawns the master process and worker processes as OS child processes
3. Monitors worker heartbeats and console output
4. Auto-restarts workers that terminate unexpectedly on their assigned ports
5. Monitors job progress via the REST API
6. Cleans up all processes when the job completes or fails

The operator can also respond to queue depth signals: when the pending task count exceeds a threshold, it can spawn additional workers to handle the backlog.
