import { useEffect, useRef } from 'react'
import type { DesktopSnapshot } from '../lib/schemas'
import { notifyDesktop } from '../lib/desktopNotifications'

type UseDesktopNotificationsOptions = {
  snapshot: DesktopSnapshot
  selectedRoomId: string
}

export function useDesktopNotifications({
  snapshot,
  selectedRoomId,
}: UseDesktopNotificationsOptions) {
  const seenMessageIds = useRef<Set<string>>(new Set())

  useEffect(() => {
    for (const message of snapshot.messages) {
      if (seenMessageIds.current.has(message.id)) {
        continue
      }
      seenMessageIds.current.add(message.id)

      const author = message.author.trim().toLowerCase()
      if (author === 'you' || author === snapshot.settings.nickname.trim().toLowerCase()) {
        continue
      }

      const mentionNeedle = `@${snapshot.settings.nickname.trim().toLowerCase()}`
      const directRoom =
        message.roomId.startsWith('dm-') ||
        snapshot.rooms.find((room) => room.id === message.roomId)?.kind === 'dm'
      const mention = message.body.toLowerCase().includes(mentionNeedle)
      const activeRoom = message.roomId === selectedRoomId

      if (message.emphasis === 'system' && activeRoom) {
        continue
      }

      if (!directRoom && !mention) {
        continue
      }

      const title = directRoom
        ? `Direct message from ${message.author}`
        : `Mention from ${message.author}`
      void notifyDesktop(title, message.body)
    }
  }, [selectedRoomId, snapshot])
}
