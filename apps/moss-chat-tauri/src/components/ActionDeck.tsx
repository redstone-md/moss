type ActionDeckProps = {
  appName: string
  version: string
  branch: string
  stage: string
  roomDraft: string
  peerDraft: string
  directDraft: string
  onRoomDraftChange: (value: string) => void
  onPeerDraftChange: (value: string) => void
  onDirectDraftChange: (value: string) => void
  onJoinRoom: () => void
  onConnectPeer: () => void
  onOpenDirectRoom: () => void
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
  directDraft,
  onRoomDraftChange,
  onPeerDraftChange,
  onDirectDraftChange,
  onJoinRoom,
  onConnectPeer,
  onOpenDirectRoom,
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
        <div className="inline-form">
          <label>
            <span>Open direct room</span>
            <input
              value={directDraft}
              onChange={(event) => onDirectDraftChange(event.target.value)}
              placeholder="nickname or peer id"
            />
          </label>
          <button className="action-tile" onClick={onOpenDirectRoom} type="button">
            {busyAction === 'dm' ? 'Opening...' : 'Open DM'}
          </button>
        </div>
        <div className="hero-meta">
          <span>Room messages use the shared chat payload format.</span>
          <span>Presence and direct-room invites flow over the control channel.</span>
        </div>
      </div>
      {errorNote ? <p className="runtime-error">{errorNote}</p> : null}
    </section>
  )
}
