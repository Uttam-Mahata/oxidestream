import type { JobStatusResponse, JobSummary, QueueDepthResponse, SystemMetrics, TaskInfo, Worker } from './types'

const BASE = ''

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${path}`, init)
  if (!res.ok) {
    const text = await res.text()
    throw new Error(text || `HTTP ${res.status}`)
  }
  return res.json()
}

export function getWorkers(): Promise<Worker[]> {
  return request<Worker[]>('/workers')
}

export function getQueueDepth(): Promise<QueueDepthResponse> {
  return request<QueueDepthResponse>('/queue_depth')
}

export function getJobStatus(jobId: string): Promise<JobStatusResponse> {
  return request<JobStatusResponse>(`/status?job_id=${encodeURIComponent(jobId)}`)
}

export function getAllJobs(): Promise<JobSummary[]> {
  return request<JobSummary[]>('/jobs')
}

export function getJobTasks(jobId: string): Promise<TaskInfo[]> {
  return request<TaskInfo[]>(`/jobs/${encodeURIComponent(jobId)}/tasks`)
}

export function cancelJob(jobId: string): Promise<{ status: string }> {
  return request<{ status: string }>(`/jobs/${encodeURIComponent(jobId)}/cancel`, {
    method: 'POST',
  })
}

export function getMetrics(): Promise<SystemMetrics> {
  return request<SystemMetrics>('/metrics')
}

export function submitJob(body: {
  map_sql?: string
  reduce_sql?: string
  sql?: string
  input_files: string[]
  num_partitions: number
  output_dir: string
}): Promise<{ job_id: string; status: string }> {
  return request('/submit', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
}

export function submitStreamingJob(body: {
  input_dir: string
  checkpoint_file: string
  map_sql: string
  reduce_sql: string
  num_partitions: number
  output_dir: string
}): Promise<{ message: string }> {
  return request('/submit_streaming', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
}
