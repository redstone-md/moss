mod callback_state;
mod commands;
mod ffi;
mod models;
mod state;

pub fn run() {
    tauri::Builder::default()
        .manage(state::SharedDesktopState::new(state::DesktopShellState::new()))
        .invoke_handler(tauri::generate_handler![
            commands::desktop_snapshot,
            commands::toggle_runtime
        ])
        .run(tauri::generate_context!())
        .expect("failed to run moss chat dev app");
}
