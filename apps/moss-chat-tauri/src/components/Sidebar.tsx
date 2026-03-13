import type { RoomSummary } from '../lib/schemas'

type SidebarProps = {
  channels: RoomSummary[]
  utilityRooms: RoomSummary[]
  selectedRoomId: string
  roomSearch: string
  onRoomSearchChange: (value: string) => void
  onOpenCreateChannel: () => void
  onSelectRoom: (roomId: string) => void
  onOpenProfile: () => void
}

export function Sidebar({
  channels,
  utilityRooms,
  selectedRoomId,
  roomSearch,
  onRoomSearchChange,
  onOpenCreateChannel,
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
        <button className="primary-action" type="button" onClick={onOpenCreateChannel}>
          Create Channel
        </button>
      </div>

      <div className="sidebar-room-list" role="list" aria-label="Channels">
        {channels.length > 0 ? (
          channels.map((room) => renderRoom(room, selectedRoomId, onSelectRoom))
        ) : (
          <div className="sidebar-empty">
            <strong>No channels yet</strong>
            <p>Create or join a room to start a conversation.</p>
          </div>
        )}

        {utilityRooms.length > 0 ? (
          <div className="sidebar-section">
            <p className="eyebrow">Workspace</p>
            <div className="sidebar-room-stack">
              {utilityRooms.map((room) => renderRoom(room, selectedRoomId, onSelectRoom))}
            </div>
          </div>
        ) : null}
      </div>
    </aside>
  )
}

function renderRoom(
  room: RoomSummary,
  selectedRoomId: string,
  onSelectRoom: (roomId: string) => void,
) {
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
      <span className="room-pill">{room.unread > 0 ? room.unread : room.kind}</span>
    </button>
  )
}
