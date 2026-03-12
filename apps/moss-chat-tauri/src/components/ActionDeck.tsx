type ActionDeckProps = {
  appName: string
  version: string
  branch: string
  stage: string
  roomDraft: string
  peerDraft: string
  onRoomDraftChange: (value: string) => void
  onPeerDraftChange: (value: string) => void
  onJoinRoom: () => void
  onConnectPeer: () => void
  busyAction?: string
  errorNote?: string
}

export function ActionDeck({
  appName,
  version,
  branch,
  stage,
  roomDraft,
  peerDraft,
  onRoomDraftChange,
  onPeerDraftChange,
  onJoinRoom,
  onConnectPeer,
  busyAction,
  errorNote,
}: ActionDeckProps) {
  return (
    <section className="panel action-deck">
      <div className="panel-header">
        <div>
          <p className="eyebrow">Action deck</p>
          <h2>{appName}</h2>
        </div>
      </div>
      <div className="hero-meta hero-meta-left">
        <span>{version}</span>
        <span>{branch}</span>
        <span>{stage}</span>
      </div>
      <div className="action-stack">
        <div className="inline-form">
          <label>
            <span>Join room</span>
            <input
              value={roomDraft}
              onChange={(event) => onRoomDraftChange(event.target.value)}
              placeholder="lobby"
            />
          </label>
          <button className="action-tile" onClick={onJoinRoom} type="button">
            {busyAction === 'join' ? 'Joining...' : 'Subscribe'}
          </button>
        </div>
        <div className="inline-form">
          <label>
            <span>Connect peer</span>
            <input
              value={peerDraft}
              onChange={(event) => onPeerDraftChange(event.target.value)}
              placeholder="host:port"
            />
          </label>
          <button className="action-tile" onClick={onConnectPeer} type="button">
            {busyAction === 'connect' ? 'Connecting...' : 'Connect'}
          </button>
        </div>
        <div className="hero-meta">
          <span>Publish and subscribe are already backed by libmoss.</span>
          <span>Message callbacks drive the room history.</span>
        </div>
      </div>
      {errorNote ? <p className="runtime-error">{errorNote}</p> : null}
    </section>
  )
}
