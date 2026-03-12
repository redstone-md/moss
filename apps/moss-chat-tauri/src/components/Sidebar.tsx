import type { RoomSummary } from '../lib/schemas'

type SidebarProps = {
  rooms: RoomSummary[]
  selectedRoomId: string
  roomSearch: string
  roomDraft: string
  createMode: boolean
  onRoomSearchChange: (value: string) => void
  onRoomDraftChange: (value: string) => void
  onToggleCreateMode: () => void
  onCreateRoom: () => void
  onSelectRoom: (roomId: string) => void
  onOpenProfile: () => void
}

export function Sidebar({
  rooms,
  selectedRoomId,
  roomSearch,
  roomDraft,
  createMode,
  onRoomSearchChange,
  onRoomDraftChange,
  onToggleCreateMode,
  onCreateRoom,
  onSelectRoom,
  onOpenProfile,
}: SidebarProps) {
  return (
    <aside className="sidebar-shell">
      <div className="sidebar-header">
        <div>
          <p className="eyebrow">Workspace</p>
          <h2>Moss Chat</h2>
        </div>
        <button className="ghost-action" type="button" onClick={onOpenProfile}>
          Edit Profile
        </button>
      </div>

      <label className="search-shell" aria-label="Search channels">
        <span className="search-icon">Search</span>
        <input
          value={roomSearch}
          onChange={(event) => onRoomSearchChange(event.target.value)}
          placeholder="Search channels"
        />
      </label>

      <div className="sidebar-controls">
        <div>
          <p className="eyebrow">Channels</p>
          <h3>Active rooms</h3>
        </div>
        <button className="primary-action" type="button" onClick={onToggleCreateMode}>
          {createMode ? 'Close' : 'Create Channel'}
        </button>
      </div>

      {createMode ? (
        <section className="sidebar-card sidebar-inline-form">
          <label className="field-grid">
            <span>Channel name</span>
            <input
              value={roomDraft}
              onChange={(event) => onRoomDraftChange(event.target.value)}
              placeholder="design-reviews"
            />
          </label>
          <button className="secondary-action" type="button" onClick={onCreateRoom}>
            Join channel
          </button>
        </section>
      ) : null}

      <div className="sidebar-room-list" role="list" aria-label="Channels">
        {rooms.length > 0 ? (
          rooms.map((room) => {
            const selected = room.id === selectedRoomId
            return (
              <button
                className={`sidebar-room${selected ? ' sidebar-room-selected' : ''}`}
                key={room.id}
                type="button"
                onClick={() => onSelectRoom(room.id)}
              >
                <div>
                  <strong>{room.label}</strong>
                  <span>
                    {room.participants} member{room.participants === 1 ? '' : 's'}
                  </span>
                </div>
                <span className="room-pill">
                  {room.unread > 0 ? room.unread : room.kind}
                </span>
              </button>
            )
          })
        ) : (
          <div className="sidebar-empty">
            <strong>No channels yet</strong>
            <p>Create or join a room to start a conversation.</p>
          </div>
        )}
      </div>
    </aside>
  )
}
