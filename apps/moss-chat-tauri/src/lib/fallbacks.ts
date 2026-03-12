import type { RoomSummary } from './schemas'

export function getFallbackRoom(): RoomSummary {
  return {
    id: 'system',
    label: '#system',
    unread: 0,
    participants: 0,
    kind: 'system',
  }
}
