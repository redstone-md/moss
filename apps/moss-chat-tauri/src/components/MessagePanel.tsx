import type { Message, RoomSummary } from '../lib/schemas'

type MessagePanelProps = {
  room: RoomSummary | undefined
  messages: Message[]
}

export function MessagePanel({ room, messages }: MessagePanelProps) {
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
          <h3>Desktop input path is next</h3>
          <p>
            This shell already has room and peer context. The next iteration wires
            the shared runtime so compose, subscribe, and publish become live.
          </p>
        </div>
        <button className="secondary-action" type="button">
          Draft composer disabled
        </button>
      </div>
    </section>
  )
}
