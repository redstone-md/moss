import type { Milestone } from '../lib/schemas'

type MilestoneListProps = {
  milestones: Milestone[]
}

const labels: Record<Milestone['status'], string> = {
  ready: 'Ready',
  next: 'Next',
  blocked: 'Blocked',
}

export function MilestoneList({ milestones }: MilestoneListProps) {
  return (
    <section className="panel">
      <div className="panel-header">
        <div>
          <p className="eyebrow">Roadmap</p>
          <h2>Migration steps</h2>
        </div>
      </div>
      <div className="milestone-list">
        {milestones.map((milestone) => (
          <article className="milestone-card" key={milestone.title}>
            <div className="milestone-topline">
              <h3>{milestone.title}</h3>
              <span className={`pill pill-${milestone.status}`}>
                {labels[milestone.status]}
              </span>
            </div>
            <p>{milestone.detail}</p>
          </article>
        ))}
      </div>
    </section>
  )
}
