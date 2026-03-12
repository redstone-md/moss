import type { Artifact } from '../lib/schemas'

type ArtifactListProps = {
  artifacts: Artifact[]
}

export function ArtifactList({ artifacts }: ArtifactListProps) {
  return (
    <section className="panel">
      <div className="panel-header">
        <div>
          <p className="eyebrow">Artifacts</p>
          <h2>Expected desktop outputs</h2>
        </div>
      </div>
      <div className="artifact-list">
        {artifacts.map((artifact) => (
          <article className="artifact-card" key={artifact.name}>
            <div>
              <h3>{artifact.name}</h3>
              <p>{artifact.platform}</p>
            </div>
            <p>{artifact.notes}</p>
          </article>
        ))}
      </div>
    </section>
  )
}
