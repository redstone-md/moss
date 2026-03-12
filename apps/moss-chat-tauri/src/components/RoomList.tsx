import type { RoomSummary } from '../lib/schemas'

type RoomListProps = {
  rooms: RoomSummary[]
  selectedRoomId: string
  onSelect: (roomId: string) => void
}

export function RoomList({ rooms, selectedRoomId, onSelect }: RoomListProps) {
  return (
    <aside className="panel room-list-panel">
      <div className="panel-header">
        <div>
          <p className="eyebrow">Rooms</p>
          <h2>Desktop shell</h2>
        </div>
      </div>
      <div className="room-list">
        {rooms.map((room) => {
          const selected = room.id === selectedRoomId
          return (
            <button
              className={`room-item${selected ? ' room-item-selected' : ''}`}
              key={room.id}
              onClick={() => onSelect(room.id)}
            >
              <div>
                <strong>{room.label}</strong>
                <span>{room.participants} participants</span>
              </div>
              <span className="room-meta">
                {room.unread > 0 ? room.unread : room.kind}
              </span>
            </button>
          )
        })}
      </div>
    </aside>
  )
}
