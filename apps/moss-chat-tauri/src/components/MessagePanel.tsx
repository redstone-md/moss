import { useEffect, useRef } from 'react'
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
  const scrollRef = useRef<HTMLDivElement | null>(null)

  useEffect(() => {
    const container = scrollRef.current
    if (!container) {
      return
    }
    container.scrollTo({
      top: container.scrollHeight,
      behavior: 'smooth',
    })
  }, [messages.length, room?.id])

  return (
    <section className="message-panel-shell">
      <div className="message-scroll-region" ref={scrollRef}>
        {messages.length > 0 ? (
          <div className="message-list">
            {messages.map((message) => (
              <article className={`message-row message-${message.emphasis}`} key={message.id}>
                <div className="message-avatar" aria-hidden="true">
                  {avatarLabel(message.author)}
                </div>
                <div className="message-body">
                  <div className="message-topline">
                    <strong>{message.author}</strong>
                    <span>{message.timestamp}</span>
                  </div>
                  <p>{message.body}</p>
                </div>
              </article>
            ))}
          </div>
        ) : (
          <div className="empty-state">
            <strong>No messages yet</strong>
            <p>
              {room?.kind === 'system'
                ? 'System updates will appear here as soon as the runtime has something to report.'
                : `Start the conversation in ${room?.label ?? '#room'} and new messages will stream in here.`}
            </p>
          </div>
        )}
      </div>
      <div className="composer-bar">
        <label className="composer-input" aria-label={`Write to ${room?.label ?? '#room'}`}>
          <textarea
            value={draft}
            onChange={(event) => onDraftChange(event.target.value)}
            placeholder={`Message ${room?.label ?? '#room'}`}
          />
        </label>
        <button
          className="primary-action composer-send"
          onClick={onSend}
          type="button"
          aria-label={`Send message to ${room?.label ?? '#room'}`}
        >
          {isSending ? 'Sending...' : 'Send'}
        </button>
      </div>
      {errorNote ? <p className="runtime-error">{errorNote}</p> : null}
    </section>
  )
}

function avatarLabel(author: string): string {
  const trimmed = author.trim()
  if (!trimmed) {
    return 'MC'
  }
  return trimmed
    .split(/[\s._-]+/)
    .filter(Boolean)
    .slice(0, 2)
    .map((chunk) => chunk[0]?.toUpperCase() ?? '')
    .join('')
}
