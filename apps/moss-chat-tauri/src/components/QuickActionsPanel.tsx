type QuickActionsPanelProps = {
  peerDraft: string
  directDraft: string
  busyAction?: string
  errorNote?: string
  onPeerDraftChange: (value: string) => void
  onDirectDraftChange: (value: string) => void
  onConnectPeer: () => void
  onOpenDirectRoom: () => void
}

export function QuickActionsPanel({
  peerDraft,
  directDraft,
  busyAction,
  errorNote,
  onPeerDraftChange,
  onDirectDraftChange,
  onConnectPeer,
  onOpenDirectRoom,
}: QuickActionsPanelProps) {
  return (
    <section className="panel quick-actions-panel">
      <div className="panel-header">
        <div>
          <p className="eyebrow">Quick actions</p>
          <h2>Connect and direct chat</h2>
        </div>
      </div>
      <div className="quick-actions-grid">
        <div className="inline-form">
          <label>
            <span>Connect peer</span>
            <input
              value={peerDraft}
              onChange={(event) => onPeerDraftChange(event.target.value)}
              placeholder="host:port"
            />
          </label>
          <button className="secondary-action" onClick={onConnectPeer} type="button">
            {busyAction === 'connect' ? 'Connecting...' : 'Connect'}
          </button>
        </div>
        <div className="inline-form">
          <label>
            <span>Open direct message</span>
            <input
              value={directDraft}
              onChange={(event) => onDirectDraftChange(event.target.value)}
              placeholder="nickname or peer id"
            />
          </label>
          <button className="secondary-action" onClick={onOpenDirectRoom} type="button">
            {busyAction === 'dm' ? 'Opening...' : 'Open DM'}
          </button>
        </div>
      </div>
      {errorNote ? <p className="runtime-error">{errorNote}</p> : null}
    </section>
  )
}
