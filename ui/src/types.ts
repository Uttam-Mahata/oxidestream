export interface Worker {
  worker_id: string
  host: string
  control_port: number
  flight_port: number
  num_cores: number
  total_memory_mb: number
  last_active: string
  active: boolean
  cpu_usage_pct: number
  memory_used_mb: number
}

export interface JobStatusResponse {
  job_id: string
  status: string
}

export interface JobSummary {
  job_id: string
  status: string
  map_tasks: number
  reduce_tasks: number
  created_at?: string
}

export interface SystemMetrics {
  total_jobs_submitted: number
  jobs_completed: number
  jobs_failed: number
  jobs_active: number
  active_workers: number
  tasks_running: number
  tasks_pending: number
  tasks_completed: number
  tasks_failed: number
  uptime_seconds: number
}

export interface QueueDepthResponse {
  pending_tasks: number
}

export interface TaskInfo {
  TaskID: string
  StageID: string
  StageType: string
  WorkerID: string
  InputFiles: string[]
  OutputPartitionID: number
  Status: string
  ErrorMsg: string
  LastUpdated: string
  StartTime: string
  IsSpeculative: boolean
  SpeculativeOf: string
}

export interface JobDetail {
  job_id: string
  sql: string
  map_sql: string
  reduce_sql: string
  input_files: string[]
  num_partitions: number
  output_dir: string
  status: string
  map_tasks: Record<string, TaskInfo>
  reduce_tasks: Record<string, TaskInfo>
}
