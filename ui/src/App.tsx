import { Routes, Route } from 'react-router-dom'
import Sidebar from './components/Sidebar'
import Dashboard from './pages/Dashboard'
import SubmitJob from './pages/SubmitJob'
import Jobs from './pages/Jobs'
import JobDetailPage from './pages/JobDetail'
import Workers from './pages/Workers'
import Streaming from './pages/Streaming'

export default function App() {
  return (
    <div className="flex min-h-screen bg-gray-900 text-white">
      <Sidebar />
      <main className="flex-1 p-6 overflow-y-auto">
        <Routes>
          <Route path="/" element={<Dashboard />} />
          <Route path="/submit" element={<SubmitJob />} />
          <Route path="/jobs" element={<Jobs />} />
          <Route path="/jobs/:jobId" element={<JobDetailPage />} />
          <Route path="/workers" element={<Workers />} />
          <Route path="/streaming" element={<Streaming />} />
        </Routes>
      </main>
    </div>
  )
}
