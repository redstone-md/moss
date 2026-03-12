import type { PeerSummary } from '../lib/schemas'

type PeerPanelProps = {
  peers: PeerSummary[]
}

export function PeerPanel({ peers }: PeerPanelProps) {
  return (
    <aside className="panel peer-panel">
      <div className="panel-header">
        <div>
          <p className="eyebrow">Channel</p>
          <h2>Participants</h2>
        </div>
      </div>
      <div className="peer-list">
        {peers.map((peer) => (
          <article className="peer-card" key={peer.id}>
            <div className="peer-topline">
              <strong>{peer.displayName}</strong>
              <span>{peer.status}</span>
            </div>
            <p>{peer.route}</p>
            <div className="peer-footline">
              <span>{peer.latency}</span>
              <span>{peer.rooms.join(', ')}</span>
            </div>
          </article>
        ))}
      </div>
    </aside>
  )
}
