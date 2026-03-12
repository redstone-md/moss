import type { PeerSummary, RoomSummary, RuntimeStatus } from '../lib/schemas'

type ChatHeaderProps = {
  room: RoomSummary
  peers: PeerSummary[]
  runtime: RuntimeStatus
  onToggleSidebar: () => void
}

export function ChatHeader({
  room,
  peers,
  runtime,
  onToggleSidebar,
}: ChatHeaderProps) {
  const subtitle =
    room.kind === 'system'
      ? 'Runtime events, bootstrap notices, and diagnostics.'
      : `${peers.length} active participant${peers.length === 1 ? '' : 's'}`

  return (
    <header className="chat-header">
      <div className="chat-header-main">
        <button
          className="ghost-action mobile-only"
          type="button"
          onClick={onToggleSidebar}
        >
          Channels
        </button>
        <div>
          <p className="eyebrow">Conversation</p>
          <h2>{room.label}</h2>
          <p className="runtime-summary">{subtitle}</p>
        </div>
      </div>

      <div className="chat-header-presence">
        {peers.slice(0, 3).map((peer) => (
          <div className="presence-chip" key={peer.id}>
            <span className="presence-avatar">{initials(peer.displayName)}</span>
            <div>
              <strong>{peer.displayName}</strong>
              <span>{peer.status}</span>
            </div>
          </div>
        ))}
        <div className="presence-runtime">
          <span className={`status-dot${runtime.state === 'Runtime online' ? ' online' : ''}`} />
          <strong>{runtime.state}</strong>
        </div>
      </div>
    </header>
  )
}

function initials(value: string): string {
  return value
    .split(/[\s._-]+/)
    .filter(Boolean)
    .slice(0, 2)
    .map((chunk) => chunk[0]?.toUpperCase() ?? '')
    .join('')
}
