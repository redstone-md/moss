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
