import { useEffect, useState, useCallback } from 'react'
import { useSearchParams, useNavigate } from 'react-router-dom'
import { getJobStatus } from '../api'

const STATUS_BADGE: Record<string, string> = {
  PENDING: 'bg-yellow-500/20 text-yellow-400',
  RUNNING: 'bg-blue-500/20 text-blue-400',
  COMPLETED: 'bg-green-500/20 text-green-400',
  FAILED: 'bg-red-500/20 text-red-400',
}

interface JobEntry {
  jobId: string
  status: string
}

export default function Jobs() {
  const [searchParams] = useSearchParams()
  const navigate = useNavigate()
  const highlight = searchParams.get('highlight')

  const [jobs, setJobs] = useState<JobEntry[]>([])
  const [pollId, setPollId] = useState<string | null>(highlight)

  const load = useCallback(async () => {
    if (pollId) {
      try {
        const r = await getJobStatus(pollId)
        setJobs((prev) => {
          const existing = prev.filter((j) => j.jobId !== pollId)
          return [{ jobId: pollId!, status: r.status }, ...existing]
        })
      } catch {
        // ignore
      }
    }
  }, [pollId])

  useEffect(() => {
    load()
    if (!pollId) return
    const id = setInterval(load, 2000)
    return () => clearInterval(id)
  }, [load, pollId])

  const addTrackedJob = async () => {
    const id = prompt('Enter job ID to track:')
    if (!id?.trim()) return
    try {
      const r = await getJobStatus(id.trim())
      setJobs((prev) => {
        if (prev.some((j) => j.jobId === id.trim())) return prev
        return [{ jobId: id.trim(), status: r.status }, ...prev]
      })
      setPollId(id.trim())
    } catch {
      alert('Job not found')
    }
  }

  return (
    <div>
      <div className="flex items-center justify-between mb-6">
        <h1 className="text-2xl font-bold text-white">Jobs</h1>
        <button
          onClick={addTrackedJob}
          className="px-4 py-2 bg-emerald-600 hover:bg-emerald-500 text-white text-sm font-medium rounded-lg transition-colors"
        >
          Track Job
        </button>
      </div>

      {jobs.length === 0 ? (
        <div className="bg-gray-800 rounded-xl p-8 border border-gray-700 text-center">
          <p className="text-gray-400">No jobs tracked. Submit a job or track an existing job ID.</p>
        </div>
      ) : (
        <div className="bg-gray-800 rounded-xl border border-gray-700 overflow-hidden">
          <table className="w-full">
            <thead>
              <tr className="border-b border-gray-700 text-left">
                <th className="px-5 py-3 text-xs font-semibold text-gray-400 uppercase tracking-wider">
                  Job ID
                </th>
                <th className="px-5 py-3 text-xs font-semibold text-gray-400 uppercase tracking-wider">
                  Status
                </th>
                <th className="px-5 py-3 text-xs font-semibold text-gray-400 uppercase tracking-wider">
                  Actions
                </th>
              </tr>
            </thead>
            <tbody>
              {jobs.map((j) => (
                <tr
                  key={j.jobId}
                  className={`border-b border-gray-700/50 hover:bg-gray-750 transition-colors ${
                    j.jobId === highlight ? 'bg-emerald-900/20' : ''
                  }`}
                >
                  <td className="px-5 py-3.5 font-mono text-sm text-gray-200">
                    {j.jobId}
                  </td>
                  <td className="px-5 py-3.5">
                    <span
                      className={`text-xs font-semibold px-2.5 py-0.5 rounded-full ${
                        STATUS_BADGE[j.status] ?? 'bg-gray-600 text-gray-300'
                      }`}
                    >
                      {j.status}
                    </span>
                  </td>
                  <td className="px-5 py-3.5">
                    <button
                      onClick={() => navigate(`/jobs/${j.jobId}`)}
                      className="text-xs text-emerald-400 hover:text-emerald-300 font-medium"
                    >
                      Details
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
