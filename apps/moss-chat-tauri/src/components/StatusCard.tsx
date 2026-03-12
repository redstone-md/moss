type StatusCardProps = {
  eyebrow: string
  title: string
  value: string
  detail: string
}

export function StatusCard({ eyebrow, title, value, detail }: StatusCardProps) {
  return (
    <article className="status-card">
      <p className="eyebrow">{eyebrow}</p>
      <h3>{title}</h3>
      <p className="status-value">{value}</p>
      <p className="status-detail">{detail}</p>
    </article>
  )
}
