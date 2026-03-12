type RuntimePanelProps = {
  state: string
  summary: string
  route: string
  natHint: string
  sharedBridge: string
  onToggle: () => void
  isBusy: boolean
}

export function RuntimePanel({
  state,
  summary,
  route,
  natHint,
  sharedBridge,
  onToggle,
  isBusy,
}: RuntimePanelProps) {
  return (
    <section className="runtime-panel">
      <div>
        <p className="eyebrow">Runtime</p>
        <h2>{state}</h2>
        <p className="runtime-summary">{summary}</p>
      </div>
      <div className="runtime-grid">
        <div className="runtime-chip">
          <span>Route</span>
          <strong>{route}</strong>
        </div>
        <div className="runtime-chip">
          <span>NAT</span>
          <strong>{natHint}</strong>
        </div>
        <div className="runtime-chip">
          <span>Bridge</span>
          <strong>{sharedBridge}</strong>
        </div>
      </div>
      <button className="primary-action" onClick={onToggle} disabled={isBusy}>
        {isBusy ? 'Updating...' : 'Toggle runtime state'}
      </button>
    </section>
  )
}
