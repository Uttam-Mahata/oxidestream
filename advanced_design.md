# OxideStream Phase 2: Enterprise Features & K8s Operator Spec

This document details the system design for Phase 2, which introduces structured streaming, query pruning, fault-tolerant checkpointing, ML/Graph algorithms, and a Kubernetes-style controller.

---

## 1. Kubernetes Operator (`control-plane/pkg/operator/`)
We will implement an **OxideStream K8s Operator Simulator** in Go.
- **CRD Definition**: Defines `OxideStreamApplication` specs inside a `job.yaml`:
  ```yaml
  apiVersion: "oxidestream.io/v1alpha1"
  kind: "OxideStreamApplication"
  metadata:
    name: "ratings-streaming"
  spec:
    master:
      grpcPort: 50050
      httpPort: 8080
    workers:
      replicas: 2
      baseControlPort: 50051
      baseFlightPort: 50052
    job:
      type: "Streaming" // Streaming | Batch | ML | Graph
      queryConfig: "/home/neutrino/oxidestream/tests/streaming_config.json"
      checkpointDir: "/home/neutrino/oxidestream/tests/checkpoint"
  ```
- **Operator Lifecycle Loop**:
  - Monitors the YAML config.
  - Spawns the Master process and Worker processes in the background.
  - Monitors worker heartbeats and console outputs.
  - If a worker process exits prematurely, the operator restarts it on its assigned port, demonstrating auto-healing.
  - Automatically cleans up all processes when the job completes.

---

## 2. Structured Streaming & Checkpointing (`control-plane/` & `data-plane/`)
- **Micro-Batch Scheduler**:
  - The Go Master monitors a directory (`tests/streaming_input/`) for new files.
  - Every 2 seconds, files that arrived since the last offset are grouped into a micro-batch.
  - The Master triggers a Map-Shuffle-Reduce iteration on this micro-batch.
- **Event-Time Watermarking**:
  - SQL query configuration specifies an event time column (`timestamp`) and a late threshold (watermark) of 5 seconds.
  - Workers parse records, determine the max event time seen so far, update the watermark, and drop rows with `timestamp < (max_timestamp - 5s)`.
- **State Checkpointing**:
  - Workers save running aggregates (e.g. cumulative counts/sums) to the checkpoint directory (`tests/checkpoint/checkpoint_state.json`) after each micro-batch completes.
  - The next micro-batch loads the checkpoint file, merges it with the new micro-batch output, and updates the checkpoint file.

---

## 3. Dynamic Partition Pruning (DPP)
- **Problem**: Joins between large fact tables and small dimension tables scan all partitions of the fact table.
- **Solution**: Go scheduler runs a lightweight pre-query on the dimension table, collects the matching join keys, and injects them as standard filter conditions (`AND user_id IN (...)`) into the Map tasks scheduled for the fact table partitions. This bypasses reading partitions that will ultimately be filtered out during the join step.

---

## 4. ML & Graph Algorithms (MLlib & GraphX equivalent)
We will add support for specialized distributed ML and Graph stages:
- **Distributed Linear Regression (ML)**:
  - Map tasks compute local matrices $X^T X$ and $X^T Y$ on their data partitions and serialize them.
  - Reduce task aggregates $X^T X$ and $X^T Y$ matrices from all mappers, computes the weights using matrix inversion: $\theta = (X^T X)^{-1} X^T Y$, and outputs the trained model coefficients.
- **Distributed PageRank (Graph)**:
  - Map tasks compute rank contributions: for each node $u$, contribution $= \text{rank}(u) / \text{out\_degree}(u)$.
  - Reduce task sums the contributions for each node $v$ to compute the new PageRank: $\text{rank}(v) = 0.15 + 0.85 \times \sum \text{contributions}$, writing out the updated ranks for the next iteration.
