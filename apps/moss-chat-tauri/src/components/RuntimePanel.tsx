type RuntimePanelProps = {
  state: string
  summary: string
  route: string
  natHint: string
  sharedBridge: string
  isOnline: boolean
  errorNote?: string
  onToggle: () => void
  isBusy: boolean
}

export function RuntimePanel({
  state,
  summary,
  route,
  natHint,
  sharedBridge,
  isOnline,
  errorNote,
  onToggle,
  isBusy,
}: RuntimePanelProps) {
  return (
    <section className="runtime-panel runtime-strip">
      <div className="runtime-strip-main">
        <p className="eyebrow">Runtime</p>
        <div className="runtime-strip-copy">
          <h2>{state}</h2>
          <p className="runtime-summary">{summary}</p>
        </div>
      </div>
      <div className="runtime-strip-meta" aria-label="Runtime status details">
        <span className="status-pill">{natHint}</span>
        <span className="status-pill">{route}</span>
        <span className="status-pill">{summarizeBridge(sharedBridge)}</span>
      </div>
      {errorNote ? <p className="runtime-error">{errorNote}</p> : null}
      <button className="secondary-action runtime-toggle" onClick={onToggle} disabled={isBusy}>
        {isBusy ? 'Updating...' : isOnline ? 'Stop runtime' : 'Start runtime'}
      </button>
    </section>
  )
}

function summarizeBridge(value: string): string {
  const marker = 'Loaded from '
  if (!value.startsWith(marker)) {
    return value
  }
  const path = value.slice(marker.length)
  const normalized = path.replace(/\\/g, '/')
  return normalized.split('/').pop() ?? value
}
