import { invoke } from '@tauri-apps/api/core'
import {
  connectPeerInputSchema,
  desktopSnapshotSchema,
  openDirectRoomInputSchema,
  publishMessageInputSchema,
  subscribeRoomInputSchema,
  updateRuntimeSettingsInputSchema,
  type ConnectPeerInput,
  type DesktopSnapshot,
  type OpenDirectRoomInput,
  type PublishMessageInput,
  type SubscribeRoomInput,
  type UpdateRuntimeSettingsInput,
} from './schemas'

export class DesktopStatusClient {
  async getSnapshot(): Promise<DesktopSnapshot> {
    const payload = await invoke('desktop_snapshot')
    const result = desktopSnapshotSchema.safeParse(payload)
    if (!result.success) {
      throw new Error(`Invalid desktop snapshot: ${result.error.message}`)
    }
    return result.data
  }

  async toggleRuntime(): Promise<DesktopSnapshot> {
    const payload = await invoke('toggle_runtime')
    const result = desktopSnapshotSchema.safeParse(payload)
    if (!result.success) {
      throw new Error(`Invalid runtime toggle payload: ${result.error.message}`)
    }
    return result.data
  }

  async updateRuntimeSettings(
    input: UpdateRuntimeSettingsInput,
  ): Promise<DesktopSnapshot> {
    const parsed = updateRuntimeSettingsInputSchema.parse(input)
    const payload = await invoke('update_runtime_settings', { payload: parsed })
    const result = desktopSnapshotSchema.safeParse(payload)
    if (!result.success) {
      throw new Error(`Invalid runtime settings payload: ${result.error.message}`)
    }
    return result.data
  }

  async subscribeRoom(input: SubscribeRoomInput): Promise<DesktopSnapshot> {
    const parsed = subscribeRoomInputSchema.parse(input)
    const payload = await invoke('subscribe_room', parsed)
    const result = desktopSnapshotSchema.safeParse(payload)
    if (!result.success) {
      throw new Error(`Invalid subscribe payload: ${result.error.message}`)
    }
    return result.data
  }

  async connectPeer(input: ConnectPeerInput): Promise<DesktopSnapshot> {
    const parsed = connectPeerInputSchema.parse(input)
    const payload = await invoke('connect_peer', parsed)
    const result = desktopSnapshotSchema.safeParse(payload)
    if (!result.success) {
      throw new Error(`Invalid connect payload: ${result.error.message}`)
    }
    return result.data
  }

  async openDirectRoom(input: OpenDirectRoomInput): Promise<DesktopSnapshot> {
    const parsed = openDirectRoomInputSchema.parse(input)
    const payload = await invoke('open_direct_room', parsed)
    const result = desktopSnapshotSchema.safeParse(payload)
    if (!result.success) {
      throw new Error(`Invalid direct-room payload: ${result.error.message}`)
    }
    return result.data
  }

  async publishMessage(input: PublishMessageInput): Promise<DesktopSnapshot> {
    const parsed = publishMessageInputSchema.parse(input)
    const payload = await invoke('publish_message', parsed)
    const result = desktopSnapshotSchema.safeParse(payload)
    if (!result.success) {
      throw new Error(`Invalid publish payload: ${result.error.message}`)
    }
    return result.data
  }
}

export const desktopStatusClient = new DesktopStatusClient()
