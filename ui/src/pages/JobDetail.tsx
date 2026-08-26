import { useEffect, useState, useCallback } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { getJobStatus } from '../api'

const STATUS_BADGE: Record<string, string> = {
  PENDING: 'bg-yellow-500/20 text-yellow-400',
  RUNNING: 'bg-blue-500/20 text-blue-400',
  COMPLETED: 'bg-green-500/20 text-green-400',
  FAILED: 'bg-red-500/20 text-red-400',
}

const TASK_STATUS_BADGE: Record<string, string> = {
  PENDING: 'bg-yellow-500/20 text-yellow-400',
  RUNNING: 'bg-blue-500/20 text-blue-400',
  COMPLETED: 'bg-green-500/20 text-green-400',
  FAILED: 'bg-red-500/20 text-red-400',
}

interface TaskInfo {
  TaskID: string
  StageID: string
  WorkerID: string
  Status: string
  OutputPartitionID: number
  IsSpeculative: boolean
  ErrorMsg: string
}

interface JobDetail {
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

export default function JobDetailPage() {
  const { jobId } = useParams<{ jobId: string }>()
  const navigate = useNavigate()
  const [detail, setDetail] = useState<JobDetail | null>(null)
  const [error, setError] = useState('')

  const load = useCallback(async () => {
    if (!jobId) return
    try {
      const r = await getJobStatus(jobId)
      setDetail((prev) =>
        prev
          ? { ...prev, status: r.status }
          : {
              job_id: r.job_id,
              status: r.status,
              sql: '',
              map_sql: '',
              reduce_sql: '',
              input_files: [],
              num_partitions: 0,
              output_dir: '',
              map_tasks: {},
              reduce_tasks: {},
            }
      )
      setError('')
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : 'Failed to load job')
    }
  }, [jobId])

  useEffect(() => {
    load()
    const id = setInterval(load, 2000)
    return () => clearInterval(id)
  }, [load])

  if (error) {
    return (
      <div className="text-center py-12">
        <p className="text-red-400 mb-4">{error}</p>
        <button
          onClick={() => navigate('/jobs')}
          className="text-emerald-400 hover:text-emerald-300 text-sm"
        >
          Back to Jobs
        </button>
      </div>
    )
  }

  if (!detail) {
    return (
      <div className="text-center py-12">
        <p className="text-gray-400">Loading...</p>
      </div>
    )
  }

  const mapTasks = Object.values(detail.map_tasks)
  const reduceTasks = Object.values(detail.reduce_tasks)

  const renderTaskRow = (t: TaskInfo) => (
    <tr key={t.TaskID} className="border-b border-gray-700/50">
      <td className="px-4 py-2.5 font-mono text-xs text-gray-300">{t.TaskID}</td>
      <td className="px-4 py-2.5">
        <span
          className={`text-xs font-semibold px-2 py-0.5 rounded-full ${
            TASK_STATUS_BADGE[t.Status] ?? 'bg-gray-600 text-gray-300'
          }`}
        >
          {t.Status}
        </span>
      </td>
      <td className="px-4 py-2.5 text-xs text-gray-400">{t.WorkerID || '-'}</td>
      <td className="px-4 py-2.5 text-xs text-gray-400">{t.OutputPartitionID}</td>
      <td className="px-4 py-2.5 text-xs text-gray-400">
        {t.IsSpeculative ? 'Yes' : ''}
      </td>
      {t.ErrorMsg && (
        <td className="px-4 py-2.5 text-xs text-red-400 truncate max-w-[200px]">
          {t.ErrorMsg}
        </td>
      )}
    </tr>
  )

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-3">
        <button
          onClick={() => navigate('/jobs')}
          className="text-gray-400 hover:text-white text-sm"
        >
          &larr; Jobs
        </button>
        <h1 className="text-2xl font-bold text-white">Job Detail</h1>
      </div>

      <div className="bg-gray-800 rounded-xl p-5 border border-gray-700 space-y-3">
        <div className="flex items-center gap-3">
          <span className="font-mono text-sm text-gray-300">{detail.job_id}</span>
          <span
            className={`text-xs font-semibold px-2.5 py-0.5 rounded-full ${
              STATUS_BADGE[detail.status] ?? 'bg-gray-600 text-gray-300'
            }`}
          >
            {detail.status}
          </span>
        </div>
        {detail.map_sql && (
          <div>
            <p className="text-xs text-gray-500 mb-1">Map SQL</p>
            <pre className="bg-gray-900 rounded-lg p-3 text-xs text-gray-300 font-mono overflow-x-auto">
              {detail.map_sql}
            </pre>
          </div>
        )}
        {detail.reduce_sql && (
          <div>
            <p className="text-xs text-gray-500 mb-1">Reduce SQL</p>
            <pre className="bg-gray-900 rounded-lg p-3 text-xs text-gray-300 font-mono overflow-x-auto">
              {detail.reduce_sql}
            </pre>
          </div>
        )}
        {!detail.map_sql && detail.sql && (
          <div>
            <p className="text-xs text-gray-500 mb-1">SQL</p>
            <pre className="bg-gray-900 rounded-lg p-3 text-xs text-gray-300 font-mono overflow-x-auto">
              {detail.sql}
            </pre>
          </div>
        )}
        <div className="flex gap-6 text-xs text-gray-400">
          <span>Partitions: {detail.num_partitions}</span>
          <span>Input: {detail.input_files.join(', ')}</span>
          <span>Output: {detail.output_dir}</span>
        </div>
      </div>

      {mapTasks.length > 0 && (
        <div className="bg-gray-800 rounded-xl border border-gray-700 overflow-hidden">
          <div className="px-5 py-3 border-b border-gray-700">
            <h2 className="text-sm font-semibold text-white">
              Map Tasks ({mapTasks.filter((t) => t.Status === 'COMPLETED').length}/
              {mapTasks.length})
            </h2>
          </div>
          <table className="w-full">
            <thead>
              <tr className="border-b border-gray-700 text-left">
                <th className="px-4 py-2 text-xs text-gray-400 font-semibold">Task ID</th>
                <th className="px-4 py-2 text-xs text-gray-400 font-semibold">Status</th>
                <th className="px-4 py-2 text-xs text-gray-400 font-semibold">Worker</th>
                <th className="px-4 py-2 text-xs text-gray-400 font-semibold">Partition</th>
                <th className="px-4 py-2 text-xs text-gray-400 font-semibold">Spec.</th>
              </tr>
            </thead>
            <tbody>{mapTasks.map(renderTaskRow)}</tbody>
          </table>
        </div>
      )}

      {reduceTasks.length > 0 && (
        <div className="bg-gray-800 rounded-xl border border-gray-700 overflow-hidden">
          <div className="px-5 py-3 border-b border-gray-700">
            <h2 className="text-sm font-semibold text-white">
              Reduce Tasks ({reduceTasks.filter((t) => t.Status === 'COMPLETED').length}/
              {reduceTasks.length})
            </h2>
          </div>
          <table className="w-full">
            <thead>
              <tr className="border-b border-gray-700 text-left">
                <th className="px-4 py-2 text-xs text-gray-400 font-semibold">Task ID</th>
                <th className="px-4 py-2 text-xs text-gray-400 font-semibold">Status</th>
                <th className="px-4 py-2 text-xs text-gray-400 font-semibold">Worker</th>
                <th className="px-4 py-2 text-xs text-gray-400 font-semibold">Partition</th>
                <th className="px-4 py-2 text-xs text-gray-400 font-semibold">Spec.</th>
              </tr>
            </thead>
            <tbody>{reduceTasks.map(renderTaskRow)}</tbody>
          </table>
        </div>
      )}

      {mapTasks.length === 0 && reduceTasks.length === 0 && (
        <div className="bg-gray-800 rounded-xl p-6 border border-gray-700 text-center">
          <p className="text-gray-400 text-sm">
            No task data available yet. Tasks will appear once the job starts executing.
          </p>
        </div>
      )}
    </div>
  )
}
