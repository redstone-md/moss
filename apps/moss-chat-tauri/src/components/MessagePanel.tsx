import type { Message, RoomSummary } from '../lib/schemas'

type MessagePanelProps = {
  room: RoomSummary | undefined
  messages: Message[]
  draft: string
  onDraftChange: (value: string) => void
  onSend: () => void
  isSending: boolean
  errorNote?: string
}

export function MessagePanel({
  room,
  messages,
  draft,
  onDraftChange,
  onSend,
  isSending,
  errorNote,
}: MessagePanelProps) {
  return (
    <section className="panel message-panel">
      <div className="panel-header">
        <div>
          <p className="eyebrow">Messages</p>
          <h2>{room?.label ?? 'Room not found'}</h2>
        </div>
      </div>
      <div className="message-list">
        {messages.map((message) => (
          <article className={`message-card message-${message.emphasis}`} key={message.id}>
            <div className="message-topline">
              <strong>{message.author}</strong>
              <span>{message.timestamp}</span>
            </div>
            <p>{message.body}</p>
          </article>
        ))}
      </div>
      <div className="composer-shell">
        <div>
          <p className="eyebrow">Composer</p>
          <h3>Live publish path</h3>
          <p>Compose into the selected room and publish through the shared runtime.</p>
        </div>
        <div className="composer-form">
          <textarea
            value={draft}
            onChange={(event) => onDraftChange(event.target.value)}
            placeholder={`Write to ${room?.label ?? '#room'}`}
          />
          <button className="secondary-action" onClick={onSend} type="button">
            {isSending ? 'Sending...' : 'Send'}
          </button>
        </div>
      </div>
      {errorNote ? <p className="runtime-error">{errorNote}</p> : null}
    </section>
  )
}
