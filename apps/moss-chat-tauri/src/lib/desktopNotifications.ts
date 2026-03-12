import {
  isPermissionGranted,
  requestPermission,
  sendNotification,
} from '@tauri-apps/plugin-notification'

let permissionChecked = false
let permissionGranted = false

async function ensurePermission(): Promise<boolean> {
  if (permissionChecked) {
    return permissionGranted
  }

  permissionChecked = true
  permissionGranted = await isPermissionGranted()
  if (!permissionGranted) {
    permissionGranted = (await requestPermission()) === 'granted'
  }
  return permissionGranted
}

export async function notifyDesktop(title: string, body: string) {
  if (!(await ensurePermission())) {
    return
  }
  sendNotification({ title, body })
}
