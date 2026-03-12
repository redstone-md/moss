import { message } from '@tauri-apps/plugin-dialog'

export async function showDesktopError(title: string, body: string) {
  await message(body, {
    title,
    kind: 'error',
  })
}
