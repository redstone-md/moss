use tauri::State;

use crate::{models::DesktopSnapshot, state::SharedDesktopState};

#[tauri::command]
pub fn desktop_snapshot(state: State<'_, SharedDesktopState>) -> Result<DesktopSnapshot, String> {
    let mut state = state
        .lock()
        .map_err(|_| "desktop state lock poisoned".to_string())?;
    Ok(state.snapshot())
}

#[tauri::command]
pub fn toggle_runtime(state: State<'_, SharedDesktopState>) -> Result<DesktopSnapshot, String> {
    let mut state = state
        .lock()
        .map_err(|_| "desktop state lock poisoned".to_string())?;
    Ok(state.toggle_runtime()?)
}

#[tauri::command]
pub fn subscribe_room(
    state: State<'_, SharedDesktopState>,
    room: String,
) -> Result<DesktopSnapshot, String> {
    let room = room.trim().trim_start_matches('#').to_lowercase();
    if room.is_empty() {
        return Err("room is required".to_string());
    }
    let mut state = state
        .lock()
        .map_err(|_| "desktop state lock poisoned".to_string())?;
    Ok(state.subscribe_room(&room)?)
}

#[tauri::command]
pub fn connect_peer(
    state: State<'_, SharedDesktopState>,
    addr: String,
) -> Result<DesktopSnapshot, String> {
    let addr = addr.trim().to_string();
    if addr.is_empty() {
        return Err("peer address is required".to_string());
    }
    let mut state = state
        .lock()
        .map_err(|_| "desktop state lock poisoned".to_string())?;
    Ok(state.connect_peer(&addr)?)
}

#[tauri::command]
pub fn publish_message(
    state: State<'_, SharedDesktopState>,
    room: String,
    body: String,
) -> Result<DesktopSnapshot, String> {
    let room = room.trim().trim_start_matches('#').to_lowercase();
    let body = body.trim().to_string();
    if room.is_empty() {
        return Err("room is required".to_string());
    }
    if body.is_empty() {
        return Err("message body is required".to_string());
    }
    let mut state = state
        .lock()
        .map_err(|_| "desktop state lock poisoned".to_string())?;
    Ok(state.publish_message(&room, &body)?)
}
