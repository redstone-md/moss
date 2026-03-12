import { useQuery } from '@tanstack/react-query'
import { ArtifactList } from './components/ArtifactList'
import { MilestoneList } from './components/MilestoneList'
import { StatusCard } from './components/StatusCard'
import { desktopStatusClient } from './lib/desktopStatusClient'

export function App() {
  const snapshot = useQuery({
    queryKey: ['desktop-snapshot'],
    queryFn: () => desktopStatusClient.getSnapshot(),
  })

  if (snapshot.isPending) {
    return <main className="shell loading">Loading desktop runtime snapshot...</main>
  }

  if (snapshot.isError) {
    return (
      <main className="shell loading">
        <section className="error-panel">
          <p className="eyebrow">Bootstrap error</p>
          <h1>Desktop shell did not start cleanly</h1>
          <p>{snapshot.error.message}</p>
        </section>
      </main>
    )
  }

  const data = snapshot.data

  return (
    <main className="shell">
      <section className="hero">
        <div>
          <p className="eyebrow">Moss Chat Tauri</p>
          <h1>Desktop shell on the dev branch</h1>
          <p className="hero-copy">{data.summary}</p>
        </div>
        <div className="hero-actions">
          <button className="primary-action" onClick={() => snapshot.refetch()}>
            Refresh snapshot
          </button>
          <div className="hero-meta">
            <span>{data.appName}</span>
            <span>{data.version}</span>
            <span>{data.branch}</span>
          </div>
        </div>
      </section>

      <section className="status-grid">
        <StatusCard
          eyebrow="Stage"
          title="Migration state"
          value={data.stage}
          detail="Desktop app scaffold is separate from the current terminal chat and ready for iterative integration."
        />
        <StatusCard
          eyebrow="Shared integration"
          title="Bridge strategy"
          value="Embedded backend bridge"
          detail={data.sharedStrategy}
        />
        <StatusCard
          eyebrow="Branch policy"
          title="Release track"
          value="dev-only"
          detail="This app builds on dev workflows first, without destabilizing main branch binaries."
        />
      </section>

      <div className="content-grid">
        <ArtifactList artifacts={data.artifacts} />
        <MilestoneList milestones={data.milestones} />
      </div>
    </main>
  )
}
