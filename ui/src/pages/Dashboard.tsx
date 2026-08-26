import { useEffect, useState, useCallback } from 'react'
import { getWorkers, getQueueDepth, getJobStatus } from '../api'
import type { Worker } from '../types'

const STATUS_COLORS: Record<string, string> = {
  PENDING: 'bg-yellow-500/20 text-yellow-400',
  RUNNING: 'bg-blue-500/20 text-blue-400',
  COMPLETED: 'bg-green-500/20 text-green-400',
  FAILED: 'bg-red-500/20 text-red-400',
}

interface TrackedJob {
  jobId: string
  status: string
}

export default function Dashboard() {
  const [workers, setWorkers] = useState<Worker[]>([])
  const [queueDepth, setQueueDepth] = useState(0)
  const [jobs, setJobs] = useState<TrackedJob[]>([])
  const [newJobId, setNewJobId] = useState('')

  const load = useCallback(async () => {
    try {
      const [w, q] = await Promise.all([getWorkers(), getQueueDepth()])
      setWorkers(w)
      setQueueDepth(q.pending_tasks)
    } catch {
      // backend may be offline
    }
  }, [])

  useEffect(() => {
    load()
    const id = setInterval(load, 2000)
    return () => clearInterval(id)
  }, [load])

  useEffect(() => {
    if (jobs.length === 0) return
    const id = setInterval(async () => {
      const updated = await Promise.all(
        jobs.map(async (j) => {
          try {
            const r = await getJobStatus(j.jobId)
            return { jobId: j.jobId, status: r.status }
          } catch {
            return j
          }
        })
      )
      setJobs(updated)
    }, 2000)
    return () => clearInterval(id)
  }, [jobs.length])

  const trackJob = async () => {
    if (!newJobId.trim()) return
    try {
      const r = await getJobStatus(newJobId.trim())
      setJobs((prev) => [...prev, { jobId: newJobId.trim(), status: r.status }])
      setNewJobId('')
    } catch {
      alert('Job not found')
    }
  }

  const activeWorkers = workers.filter((w) => w.active)

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold text-white">Dashboard</h1>

      <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
        <div className="bg-gray-800 rounded-xl p-5 border border-gray-700">
          <p className="text-sm text-gray-400 mb-1">Active Workers</p>
          <p className="text-3xl font-bold text-emerald-400">{activeWorkers.length}</p>
          <p className="text-xs text-gray-500 mt-1">{workers.length} total</p>
        </div>
        <div className="bg-gray-800 rounded-xl p-5 border border-gray-700">
          <p className="text-sm text-gray-400 mb-1">Pending Tasks</p>
          <p className="text-3xl font-bold text-amber-400">{queueDepth}</p>
        </div>
        <div className="bg-gray-800 rounded-xl p-5 border border-gray-700">
          <p className="text-sm text-gray-400 mb-1">Tracked Jobs</p>
          <p className="text-3xl font-bold text-blue-400">{jobs.length}</p>
        </div>
      </div>

      <div className="bg-gray-800 rounded-xl p-5 border border-gray-700">
        <div className="flex items-center gap-3 mb-4">
          <h2 className="text-lg font-semibold text-white">Track Job</h2>
          <input
            value={newJobId}
            onChange={(e) => setNewJobId(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && trackJob()}
            placeholder="Enter job ID..."
            className="flex-1 bg-gray-900 border border-gray-600 rounded-lg px-3 py-1.5 text-sm text-white placeholder-gray-500 focus:outline-none focus:border-emerald-500"
          />
          <button
            onClick={trackJob}
            className="px-4 py-1.5 bg-emerald-600 hover:bg-emerald-500 text-white text-sm font-medium rounded-lg transition-colors"
          >
            Track
          </button>
        </div>

        {jobs.length > 0 ? (
          <div className="space-y-2">
            {jobs.map((j) => (
              <div
                key={j.jobId}
                className="flex items-center justify-between bg-gray-900 rounded-lg px-4 py-3 border border-gray-700"
              >
                <span className="text-sm font-mono text-gray-300">{j.jobId}</span>
                <span
                  className={`text-xs font-semibold px-2.5 py-0.5 rounded-full ${
                    STATUS_COLORS[j.status] ?? 'bg-gray-600 text-gray-300'
                  }`}
                >
                  {j.status}
                </span>
              </div>
            ))}
          </div>
        ) : (
          <p className="text-sm text-gray-500">No jobs tracked yet.</p>
        )}
      </div>

      <div className="bg-gray-800 rounded-xl p-5 border border-gray-700">
        <h2 className="text-lg font-semibold text-white mb-4">Worker Overview</h2>
        {workers.length === 0 ? (
          <p className="text-sm text-gray-500">No workers registered.</p>
        ) : (
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
            {workers.map((w) => (
              <div
                key={w.worker_id}
                className={`rounded-lg px-4 py-3 border ${
                  w.active
                    ? 'bg-gray-900 border-gray-700'
                    : 'bg-gray-900/50 border-gray-800 opacity-60'
                }`}
              >
                <div className="flex items-center gap-2 mb-2">
                  <span
                    className={`h-2 w-2 rounded-full ${
                      w.active ? 'bg-emerald-400' : 'bg-red-500'
                    }`}
                  />
                  <span className="text-sm font-medium text-white truncate">
                    {w.worker_id}
                  </span>
                </div>
                <div className="text-xs text-gray-400 space-y-1">
                  <p>CPU: {w.cpu_usage_pct.toFixed(1)}%</p>
                  <p>Memory: {w.memory_used_mb} / {w.total_memory_mb} MB</p>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}
