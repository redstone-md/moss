import { invoke } from '@tauri-apps/api/core'
import {
  desktopSnapshotSchema,
  type DesktopSnapshot,
} from './schemas'

export class DesktopStatusClient {
  async getSnapshot(): Promise<DesktopSnapshot> {
    const payload = await invoke('bootstrap_snapshot')
    const result = desktopSnapshotSchema.safeParse(payload)
    if (!result.success) {
      throw new Error(`Invalid desktop snapshot: ${result.error.message}`)
    }
    return result.data
  }
}

export const desktopStatusClient = new DesktopStatusClient()
