import { NavLink } from 'react-router-dom'

const links = [
  { to: '/', label: 'Dashboard' },
  { to: '/submit', label: 'Submit Job' },
  { to: '/jobs', label: 'Jobs' },
  { to: '/workers', label: 'Workers' },
  { to: '/streaming', label: 'Streaming' },
]

export default function Sidebar() {
  return (
    <aside className="w-56 min-h-screen bg-gray-950 border-r border-gray-800 flex flex-col">
      <div className="px-4 py-5 border-b border-gray-800">
        <span className="text-lg font-bold text-emerald-400 tracking-tight">
          OxideStream
        </span>
      </div>
      <nav className="flex-1 py-3">
        {links.map((l) => (
          <NavLink
            key={l.to}
            to={l.to}
            end={l.to === '/'}
            className={({ isActive }) =>
              `block px-4 py-2.5 text-sm font-medium transition-colors ${
                isActive
                  ? 'bg-gray-800 text-emerald-400 border-r-2 border-emerald-400'
                  : 'text-gray-400 hover:bg-gray-900 hover:text-gray-200'
              }`
            }
          >
            {l.label}
          </NavLink>
        ))}
      </nav>
    </aside>
  )
}
