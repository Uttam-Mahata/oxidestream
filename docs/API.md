# OxideStream API Reference

All endpoints are served by the Go Control Plane on the configured `--http-port` (default `8080`).

Base URL: `http://localhost:8080`

---

## POST /submit

Submit a batch MapReduce SQL job to the cluster.

**Request Body:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `sql` | `string` | No | Single SQL statement (compiled into map/reduce automatically). Mutually exclusive with `map_sql`/`reduce_sql`. |
| `map_sql` | `string` | No | SQL for the map stage. Must be paired with `reduce_sql`. |
| `reduce_sql` | `string` | No | SQL for the reduce stage. Must be paired with `map_sql`. |
| `input_files` | `string[]` | Yes | List of input CSV file paths. |
| `num_partitions` | `int` | Yes | Number of output partitions for the reduce stage. Must be > 0. |
| `output_dir` | `string` | Yes | Directory where output files are written. |
| `dpp_dim_file` | `string` | No | Dimension table file path for Dynamic Partition Pruning. |
| `dpp_filter_col` | `string` | No | Column name to filter the dimension table on. |
| `dpp_filter_val` | `string` | No | Value to filter the dimension table on. |
| `dpp_join_key` | `string` | No | Join key column to inject as a filter into map tasks. |

**Response Body:**

```json
{
  "job_id": "job-1780598299993036206",
  "status": "PENDING"
}
```

**Example:**

```bash
curl -X POST http://localhost:8080/submit \
  -H 'Content-Type: application/json' \
  -d '{
    "map_sql": "SELECT user_id, category_name, COUNT(1) as cnt, SUM(rating) as s FROM input GROUP BY user_id, category_name",
    "reduce_sql": "SELECT user_id, category_name, SUM(cnt) as total, SUM(s) as sum FROM input GROUP BY user_id, category_name",
    "input_files": ["tests/data/part-0.csv", "tests/data/part-1.csv"],
    "num_partitions": 2,
    "output_dir": "tests/output"
  }'
```

---

## POST /submit_lr

Submit a distributed linear regression job. Workers compute local partial matrices (X^T X, X^T Y); the master aggregates them and solves the normal equations iteratively.

**Request Body:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `input_files` | `string[]` | Yes | List of input CSV files containing feature columns and a label column. |
| `num_partitions` | `int` | Yes | Number of partitions. Must be > 0. |
| `output_dir` | `string` | Yes | Directory where model coefficients are written. |
| `iterations` | `int` | No | Number of OLS iterations (default: `2`). |

**Response Body:**

```json
{
  "message": "Linear Regression job started"
}
```

**Example:**

```bash
curl -X POST http://localhost:8080/submit_lr \
  -H 'Content-Type: application/json' \
  -d '{
    "input_files": ["tests/data/ml_data.csv"],
    "num_partitions": 2,
    "output_dir": "tests/output_lr",
    "iterations": 3
  }'
```

---

## POST /submit_pagerank

Submit a distributed PageRank job. Iterative rank propagation across graph edge partitions with configurable convergence iterations.

**Request Body:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `input_files` | `string[]` | Yes | List of input CSV files containing edge lists (source, target). |
| `num_partitions` | `int` | Yes | Number of partitions. Must be > 0. |
| `output_dir` | `string` | Yes | Directory where final rank scores are written. |
| `iterations` | `int` | No | Number of PageRank iterations (default: `2`). |

**Response Body:**

```json
{
  "message": "PageRank job started"
}
```

**Example:**

```bash
curl -X POST http://localhost:8080/submit_pagerank \
  -H 'Content-Type: application/json' \
  -d '{
    "input_files": ["tests/data/graph_edges.csv"],
    "num_partitions": 2,
    "output_dir": "tests/output_pr",
    "iterations": 3
  }'
```

---

## POST /submit_streaming

Submit a structured streaming job. The micro-batch scheduler polls an input directory every 2 seconds and processes new files through map-reduce iterations with stateful checkpointing.

**Request Body:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `input_dir` | `string` | Yes | Directory to watch for new input files. |
| `checkpoint_file` | `string` | Yes | Path to the JSON checkpoint state file. |
| `map_sql` | `string` | Yes | SQL for the map stage. |
| `reduce_sql` | `string` | Yes | SQL for the reduce stage. |
| `num_partitions` | `int` | Yes | Number of partitions. Must be > 0. |
| `output_dir` | `string` | Yes | Directory where streaming output is written. |

**Response Body:**

```json
{
  "message": "Streaming job started"
}
```

**Example:**

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

---

## GET /status

Get the status of a specific job.

**Query Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `job_id` | `string` | Yes | The job ID to query. |

**Response Body:**

```json
{
  "job_id": "job-1780598299993036206",
  "status": "COMPLETED"
}
```

Possible status values: `PENDING`, `RUNNING`, `COMPLETED`, `FAILED`, `CANCELLED`.

**Example:**

```bash
curl "http://localhost:8080/status?job_id=job-1780598299993036206"
```

---

## GET /jobs

List all jobs known to the scheduler.

**Response Body:**

```json
[
  {
    "job_id": "job-1780598299993036206",
    "status": "COMPLETED",
    "map_tasks": 2,
    "reduce_tasks": 2,
    "created_at": "2026-06-05T00:08:19Z"
  }
]
```

**Example:**

```bash
curl http://localhost:8080/jobs
```

---

## GET /jobs/{job_id}/tasks

List all tasks (map and reduce) for a specific job.

**Path Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `job_id` | `string` | The job ID to query. |

**Response Body:**

```json
[
  {
    "task_id": "job-1780598299993036206-map-0",
    "stage_type": "MAP",
    "worker_id": "worker-1",
    "status": "COMPLETED",
    "start_time": "2026-06-05T00:08:19Z",
    "duration_ms": 342
  },
  {
    "task_id": "job-1780598299993036206-reduce-0",
    "stage_type": "REDUCE",
    "worker_id": "worker-2",
    "status": "COMPLETED",
    "start_time": "2026-06-05T00:08:20Z",
    "duration_ms": 501
  }
]
```

**Example:**

```bash
curl http://localhost:8080/jobs/job-1780598299993036206/tasks
```

---

## POST /jobs/{job_id}/cancel

Cancel a running or pending job. Pending and running tasks are marked as failed and cancellation signals are sent to workers.

**Path Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `job_id` | `string` | The job ID to cancel. |

**Response Body:**

```json
{
  "status": "cancelled"
}
```

**Example:**

```bash
curl -X POST http://localhost:8080/jobs/job-1780598299993036206/cancel
```

---

## GET /workers

List all workers registered with the control plane.

**Response Body:**

```json
[
  {
    "worker_id": "worker-1",
    "host": "127.0.0.1",
    "control_port": 50051,
    "flight_port": 50052,
    "num_cores": 4,
    "total_memory_mb": 8192,
    "last_active": "2026-06-05T00:08:20Z",
    "active": true
  },
  {
    "worker_id": "worker-2",
    "host": "127.0.0.1",
    "control_port": 50053,
    "flight_port": 50054,
    "num_cores": 4,
    "total_memory_mb": 8192,
    "last_active": "2026-06-05T00:08:20Z",
    "active": true
  }
]
```

**Example:**

```bash
curl http://localhost:8080/workers
```

---

## GET /queue_depth

Get the number of tasks currently waiting in the scheduler queue. Used by the operator autoscaler to determine when to scale out or scale in.

**Response Body:**

```json
{
  "pending_tasks": 4
}
```

**Example:**

```bash
curl http://localhost:8080/queue_depth
```

---

## GET /metrics

Get aggregate system metrics as JSON.

**Response Body:**

```json
{
  "total_jobs_submitted": 12,
  "jobs_completed": 10,
  "jobs_failed": 1,
  "jobs_active": 1,
  "active_workers": 2,
  "tasks_running": 2,
  "tasks_pending": 4,
  "tasks_completed": 20,
  "tasks_failed": 2,
  "uptime_seconds": 3600
}
```

**Example:**

```bash
curl http://localhost:8080/metrics
```

---

## GET /health

Health check endpoint. Returns `200 OK` when the control plane is running.

**Response Body:**

```json
{
  "status": "ok"
}
```

**Example:**

```bash
curl http://localhost:8080/health
```

---

## GET /metrics/prometheus

Prometheus-compatible metrics endpoint for scraping. Returns metrics in the standard Prometheus exposition format.

**Registered Metrics:**

| Metric | Type | Description |
|--------|------|-------------|
| `oxidestream_jobs_total` | Counter | Total jobs submitted, labeled by status (`completed`, `failed`) |
| `oxidestream_tasks_total` | Counter | Total tasks executed, labeled by status (`completed`, `failed`) |
| `oxidestream_workers_active` | Gauge | Number of currently active workers |
| `oxidestream_queue_depth` | Gauge | Number of pending tasks in the scheduler queue |
| `oxidestream_job_duration_seconds` | Histogram | Job execution duration in seconds |
| `oxidestream_task_duration_seconds` | Histogram | Task execution duration in seconds |
| `oxidestream_cpu_usage_percent` | Gauge | CPU usage percentage per worker |
| `oxidestream_memory_usage_mb` | Gauge | Memory usage in MB per worker |

**Example:**

```bash
curl http://localhost:8080/metrics/prometheus
```

---

## Error Responses

All endpoints return standard HTTP status codes:

| Code | Description |
|------|-------------|
| `200` | Success |
| `400` | Bad request — missing or invalid parameters |
| `404` | Resource not found — job or task ID does not exist |
| `405` | Method not allowed — incorrect HTTP method for the endpoint |
| `500` | Internal server error |

Error responses include a plain-text message body describing the error.
