import { invoke } from '@tauri-apps/api/core'
import {
  desktopSnapshotSchema,
  type DesktopSnapshot,
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
}

export const desktopStatusClient = new DesktopStatusClient()
