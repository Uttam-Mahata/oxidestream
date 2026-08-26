import { useState } from 'react'
import { submitStreamingJob } from '../api'

export default function Streaming() {
  const [inputDir, setInputDir] = useState('')
  const [checkpointFile, setCheckpointFile] = useState('')
  const [mapSQL, setMapSQL] = useState('')
  const [reduceSQL, setReduceSQL] = useState('')
  const [numPartitions, setNumPartitions] = useState(4)
  const [outputDir, setOutputDir] = useState('')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [success, setSuccess] = useState('')

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    setError('')
    setSuccess('')
    setLoading(true)
    try {
      const res = await submitStreamingJob({
        input_dir: inputDir,
        checkpoint_file: checkpointFile,
        map_sql: mapSQL,
        reduce_sql: reduceSQL,
        num_partitions: numPartitions,
        output_dir: outputDir,
      })
      setSuccess(res.message)
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : 'Submission failed')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="max-w-2xl mx-auto">
      <h1 className="text-2xl font-bold text-white mb-6">Submit Streaming Job</h1>
      <form
        onSubmit={handleSubmit}
        className="bg-gray-800 rounded-xl p-6 border border-gray-700 space-y-5"
      >
        <div>
          <label className="block text-sm font-medium text-gray-300 mb-1.5">
            Input Directory
          </label>
          <input
            value={inputDir}
            onChange={(e) => setInputDir(e.target.value)}
            placeholder="/data/streaming/input"
            className="w-full bg-gray-900 border border-gray-600 rounded-lg px-3 py-2 text-sm text-white font-mono placeholder-gray-500 focus:outline-none focus:border-emerald-500"
          />
        </div>
        <div>
          <label className="block text-sm font-medium text-gray-300 mb-1.5">
            Checkpoint File
          </label>
          <input
            value={checkpointFile}
            onChange={(e) => setCheckpointFile(e.target.value)}
            placeholder="/data/checkpoint.json"
            className="w-full bg-gray-900 border border-gray-600 rounded-lg px-3 py-2 text-sm text-white font-mono placeholder-gray-500 focus:outline-none focus:border-emerald-500"
          />
        </div>
        <div>
          <label className="block text-sm font-medium text-gray-300 mb-1.5">
            Map SQL
          </label>
          <textarea
            value={mapSQL}
            onChange={(e) => setMapSQL(e.target.value)}
            rows={4}
            placeholder="SELECT key, value FROM stream_input WHERE ..."
            className="w-full bg-gray-900 border border-gray-600 rounded-lg px-3 py-2 text-sm text-white font-mono placeholder-gray-500 focus:outline-none focus:border-emerald-500 resize-y"
          />
        </div>
        <div>
          <label className="block text-sm font-medium text-gray-300 mb-1.5">
            Reduce SQL
          </label>
          <textarea
            value={reduceSQL}
            onChange={(e) => setReduceSQL(e.target.value)}
            rows={4}
            placeholder="SELECT key, COUNT(*) FROM grouped GROUP BY key"
            className="w-full bg-gray-900 border border-gray-600 rounded-lg px-3 py-2 text-sm text-white font-mono placeholder-gray-500 focus:outline-none focus:border-emerald-500 resize-y"
          />
        </div>
        <div className="grid grid-cols-2 gap-4">
          <div>
            <label className="block text-sm font-medium text-gray-300 mb-1.5">
              Partitions
            </label>
            <input
              type="number"
              min={1}
              value={numPartitions}
              onChange={(e) => setNumPartitions(Number(e.target.value))}
              className="w-full bg-gray-900 border border-gray-600 rounded-lg px-3 py-2 text-sm text-white focus:outline-none focus:border-emerald-500"
            />
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-300 mb-1.5">
              Output Directory
            </label>
            <input
              value={outputDir}
              onChange={(e) => setOutputDir(e.target.value)}
              placeholder="/output/streaming"
              className="w-full bg-gray-900 border border-gray-600 rounded-lg px-3 py-2 text-sm text-white font-mono placeholder-gray-500 focus:outline-none focus:border-emerald-500"
            />
          </div>
        </div>
        {error && (
          <p className="text-sm text-red-400 bg-red-500/10 rounded-lg px-3 py-2">
            {error}
          </p>
        )}
        {success && (
          <p className="text-sm text-green-400 bg-green-500/10 rounded-lg px-3 py-2">
            {success}
          </p>
        )}
        <button
          type="submit"
          disabled={loading}
          className="w-full py-2.5 bg-emerald-600 hover:bg-emerald-500 disabled:bg-emerald-800 text-white font-semibold rounded-lg transition-colors"
        >
          {loading ? 'Submitting...' : 'Start Streaming Job'}
        </button>
      </form>
    </div>
  )
}
