import { useEffect, useState, useCallback } from 'react'
import { getWorkers } from '../api'
import type { Worker } from '../types'

export default function Workers() {
  const [workers, setWorkers] = useState<Worker[]>([])

  const load = useCallback(async () => {
    try {
      const w = await getWorkers()
      setWorkers(w)
    } catch {
      // backend may be offline
    }
  }, [])

  useEffect(() => {
    load()
    const id = setInterval(load, 2000)
    return () => clearInterval(id)
  }, [load])

  return (
    <div>
      <h1 className="text-2xl font-bold text-white mb-6">Workers</h1>
      {workers.length === 0 ? (
        <div className="bg-gray-800 rounded-xl p-8 border border-gray-700 text-center">
          <p className="text-gray-400">No workers registered.</p>
        </div>
      ) : (
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
          {workers.map((w) => (
            <div
              key={w.worker_id}
              className={`bg-gray-800 rounded-xl p-5 border transition-colors ${
                w.active ? 'border-gray-700' : 'border-gray-800 opacity-50'
              }`}
            >
              <div className="flex items-center justify-between mb-4">
                <div className="flex items-center gap-2">
                  <span
                    className={`h-2.5 w-2.5 rounded-full ${
                      w.active ? 'bg-emerald-400 animate-pulse' : 'bg-red-500'
                    }`}
                  />
                  <h3 className="text-sm font-semibold text-white">{w.worker_id}</h3>
                </div>
                <span
                  className={`text-[10px] font-semibold uppercase px-2 py-0.5 rounded ${
                    w.active
                      ? 'bg-emerald-500/20 text-emerald-400'
                      : 'bg-red-500/20 text-red-400'
                  }`}
                >
                  {w.active ? 'Active' : 'Inactive'}
                </span>
              </div>

              <div className="space-y-3 text-xs text-gray-400">
                <div>
                  <p className="mb-1">Host: {w.host}</p>
                  <p>Control: {w.control_port} | Flight: {w.flight_port}</p>
                </div>

                <div>
                  <div className="flex justify-between mb-1">
                    <span>CPU</span>
                    <span className="text-gray-300">{w.cpu_usage_pct.toFixed(1)}%</span>
                  </div>
                  <div className="w-full h-2 bg-gray-900 rounded-full overflow-hidden">
                    <div
                      className={`h-full rounded-full transition-all ${
                        w.cpu_usage_pct > 80
                          ? 'bg-red-500'
                          : w.cpu_usage_pct > 60
                            ? 'bg-amber-500'
                            : 'bg-emerald-500'
                      }`}
                      style={{ width: `${Math.min(w.cpu_usage_pct, 100)}%` }}
                    />
                  </div>
                </div>

                <div>
                  <div className="flex justify-between mb-1">
                    <span>Memory</span>
                    <span className="text-gray-300">
                      {w.memory_used_mb} / {w.total_memory_mb} MB
                    </span>
                  </div>
                  <div className="w-full h-2 bg-gray-900 rounded-full overflow-hidden">
                    <div
                      className="h-full bg-blue-500 rounded-full transition-all"
                      style={{
                        width: `${
                          w.total_memory_mb > 0
                            ? Math.min((w.memory_used_mb / w.total_memory_mb) * 100, 100)
                            : 0
                        }%`,
                      }}
                    />
                  </div>
                </div>

                <p className="text-[10px] text-gray-600">
                  Cores: {w.num_cores}
                </p>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
