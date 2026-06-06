# OxideStream Phase 3: Advanced Systems Design Specification

This document outlines the architecture and execution specifications for Phase 3 enterprise systems.

---

## 1. Advanced CBO Statistics (`ANALYZE TABLE`)
- **Action**: Introduce `ANALYZE TABLE <table_name> COMPUTE STATISTICS`.
- **Flow**:
  - The Go Master schedules an Analysis job on the input files.
  - Workers read CSV/Parquet tables, compute:
    - Row count.
    - Column statistics: null count, min/max, distinct values (cardinality).
  - Workers return this schema statistics mapping via `UpdateTaskStatus`.
  - Go Master saves stats inside the Raft `MetadataStore`.
  - CBO selects Join Order: joins with the lowest estimated rows after filtering are scheduled first to minimize intermediate data size.

---

## 2. Whole-Stage CodeGen (Expression Compiler)
- **Design**: Implemented inside the Rust Worker (`data-plane/src/codegen.rs`).
- **Concept**: Standard execution engines traverse ASTs at runtime, incurring virtual function calls per row/batch. CodeGen compiles operations (e.g. `(x + 1) * y`) into a single loop.
- **OxideStream CodeGen**:
  - We compile AST expressions into optimized Rust closures or pipeline loops directly executing over Arrow PrimitiveArray buffers.
  - This bypasses standard expression evaluation, compiling filtering and projection into a single unified CPU register loop.

---

## 3. Push-Based Shuffling & ESS
- **ESS (External Shuffle Service)**: Spawns the Arrow Flight server on a dedicated thread with an isolated, protected memory boundary. If the worker's DataFusion executor runs out of memory (OOM) or crashes, the Flight service remains active.
- **Push-based Shuffling**:
  - Mappers push partition data via Flight `DoPut` to a designated **Merger Node** (assigned by the Go Master).
  - The Merger Node appends record batches to a consolidated stream: `merged_shuffle_<stage_id>_<partition_id>.arrow`.
  - Reducers read a single pre-merged sequential block from the Merger Node, avoiding the random I/O of fetching from $M$ different mappers.

---

## 4. Fair Scheduler & Dynamic Scaling
- **Fair Scheduler**:
  - Implemented in `control-plane/pkg/scheduler/fair_scheduler.go`.
  - Divides available worker task slots equally among active jobs (active user queries), ensuring short jobs are not starved by huge batch jobs.
- **Dynamic Scaling**:
  - The Go Master monitors task queue backlog. If queue wait time exceeds 2 seconds, Master logs a scaling signal.
  - The Operator detects this signal and spawns new Rust Worker processes on incremental ports.

---

## 5. Ecosystem Connectors (SQLite DB Reader)
- **Database Connector**:
  - Registers database connections (SQLite).
  - Pulls rows from SQLite table, packs them into Arrow `RecordBatch` streams, and registers them inside the DataFusion SessionContext.
  - Allows queries to join distributed CSVs directly with local relational database tables.
