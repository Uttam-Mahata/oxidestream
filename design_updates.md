# OxideStream Advanced Feature Design Specification

This document details the functional specifications for the 4 new features to be implemented:

## 1. Index Shuffling (Shuffle Scalability)
**Problem**: The $M \times R$ file problem creates excessive file descriptors and random disk I/O.
**Solution**: Consolidate all partitions written by a single Map task into a single `.arrow` file, accompanied by a JSON `.index` file.

- **Storage Format**:
  - Combined data file: `shuffle_<stage_id>_<task_id>.arrow`
  - Index file: `shuffle_<stage_id>_<task_id>.index` containing partition offset mapping:
    ```json
    {
      "partitions": {
        "0": {"offset": 0, "length": 4500},
        "1": {"offset": 4500, "length": 9000}
      }
    }
    ```
- **Flight Server Retrieval**:
  - The Flight server reads the request Ticket.
  - It parses the stage, task, and target partition ID.
  - It reads the `.index` file, resolves `offset` and `length`.
  - It opens the consolidated `.arrow` file, reads the specified byte range, and streams it over Flight.

---

## 2. Adaptive Query Execution (AQE: Partition Coalescing)
**Problem**: Static partitioning schedules many small tasks for sparse partitions, creating heavy scheduling latency.
**Solution**: Automatically coalesce small shuffle partitions into single Reduce tasks at runtime.

- **Execution Flow**:
  1. Workers track written partition sizes (using file size) and report them via `UpdateTaskStatus` in `PartitionMetadata.size_bytes`.
  2. The Go Master stores partition sizes in the consensus `MetadataStore`.
  3. When scheduling the Reduce stage, Go calculates the total size of each partition across all mapper nodes:
     $$\text{TotalSize}(p) = \sum_{\text{mappers}} \text{Size}(p, \text{mapper})$$
  4. If $\text{TotalSize}(p) < \text{CoalesceThreshold}$ (e.g., 20,000 bytes in testing), the Master coalesces partition $p$ with partition $p+1$ (up to a max combined size).
  5. The Master creates a single `ReduceTask` for the coalesced partitions (assigned in `SubmitTaskRequest.coalesced_partitions`).
  6. The assigned worker fetches all coalesced partition IDs from the mappers, merges them, and runs the Reduce SQL.

---

## 3. Broadcast Joins & Query Planning (CBO)
**Problem**: Large shuffles are executed even for tiny dimension tables.
**Solution**: Auto-detect tiny inputs and broadcast them to all workers, executing map-side joins without shuffles.

- **Query Planner (CBO) in Go Master**:
  - When submitting a job, Go Master evaluates the size of input datasets.
  - If any input dataset size is below a `broadcast_threshold` (e.g., 100KB), it is classified as a **Broadcast Table**.
  - Map tasks for the main large datasets are scheduled with `broadcast_files` and `broadcast_table_names` mapped in the `SubmitTaskRequest`.
  - The master avoids scheduling distinct Map/Shuffle/Reduce steps for the broadcast table, bypassing shuffles.
- **Worker Execution**:
  - Before executing the `map_sql` or query, the worker reads the `broadcast_files` and registers them as local tables in the DataFusion SessionContext under the specified `broadcast_table_names`.
  - This allows joining the primary partition directly against the broadcasted tables locally.
