type ActionDeckProps = {
  appName: string
  version: string
  branch: string
  stage: string
}

export function ActionDeck({ appName, version, branch, stage }: ActionDeckProps) {
  const actions = [
    'Join room',
    'Open DM',
    'Connect peer',
    'Attachments',
    'Diagnostics',
    'Notifications',
  ]

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
      <div className="action-grid">
        {actions.map((action) => (
          <button className="action-tile" key={action} type="button">
            {action}
          </button>
        ))}
      </div>
    </section>
  )
}
