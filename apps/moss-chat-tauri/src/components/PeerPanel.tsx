import type { PeerSummary } from '../lib/schemas'

type PeerPanelProps = {
  peers: PeerSummary[]
  onOpenDirectRoom: (target: string) => void
}

export function PeerPanel({ peers, onOpenDirectRoom }: PeerPanelProps) {
  return (
    <aside className="panel peer-panel channel-sidebar">
      <div className="panel-header">
        <div>
          <p className="eyebrow">Channel</p>
          <h2>Participants</h2>
        </div>
      </div>
      <div className="peer-list">
        {peers.length > 0 ? (
          peers.map((peer) => (
            <article className="peer-card" key={peer.id}>
              <div className="peer-topline">
                <div className="presence-avatar">{avatarLabel(peer.displayName)}</div>
                <div className="peer-copy">
                  <strong>{peer.displayName}</strong>
                  <span>{peer.status}</span>
                </div>
              </div>
              <p>{peer.route}</p>
              <div className="peer-footline">
                <span>{peer.latency}</span>
                <span>{peer.rooms.join(', ')}</span>
              </div>
              {peer.status !== 'self' ? (
                <button
                  className="secondary-action"
                  type="button"
                  onClick={() => onOpenDirectRoom(peer.displayName)}
                >
                  Direct message
                </button>
              ) : null}
            </article>
          ))
        ) : (
          <div className="empty-state compact">
            <strong>No participants yet</strong>
            <p>When peers join this channel, they will appear here.</p>
          </div>
        )}
      </div>
    </aside>
  )
}

function avatarLabel(displayName: string): string {
  return displayName
    .split(/[\s._-]+/)
    .filter(Boolean)
    .slice(0, 2)
    .map((chunk) => chunk[0]?.toUpperCase() ?? '')
    .join('')
}
