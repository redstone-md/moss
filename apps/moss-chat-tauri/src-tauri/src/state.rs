use std::sync::Mutex;

use crate::{
    callback_state::shared_callback_state,
    ffi::{MeshInfo, MossLibrary},
    models::DesktopSnapshot,
    runtime_settings::{DesktopRuntimeConfig, RuntimeSettingsInput},
    snapshot_view,
};

const DEV_BRANCH: &str = "dev";

pub struct DesktopShellState {
    library: Option<MossLibrary>,
    library_error: Option<String>,
    handle: Option<i64>,
    settings: DesktopRuntimeConfig,
}

impl DesktopShellState {
    pub fn new() -> Self {
        let mut state = Self {
            library: None,
            library_error: None,
            handle: None,
            settings: DesktopRuntimeConfig::default(),
        };
        state.reload_library();
        if let Ok(mut callbacks) = shared_callback_state().lock() {
            callbacks.reset();
            callbacks.note_runtime("Desktop backend initialized. Waiting for runtime start.");
        }
        state
    }

    pub fn snapshot(&mut self) -> DesktopSnapshot {
        if self.library.is_none() {
            self.reload_library();
        }

        let live_mesh = self.live_mesh_info();
        let settings = self.settings.summary();
        let diagnostics = self.settings.diagnostics(live_mesh.as_ref().ok().and_then(|mesh| mesh.as_ref()));

        match live_mesh {
            Ok(Some(mesh)) => snapshot_view::online_snapshot(
                &mesh,
                settings,
                diagnostics,
                self.library_path(),
                DEV_BRANCH,
            ),
            Ok(None) => snapshot_view::offline_snapshot(
                settings,
                diagnostics,
                self.shared_bridge_summary(),
                DEV_BRANCH,
            ),
            Err(err) => snapshot_view::failed_snapshot(settings, diagnostics, err, DEV_BRANCH),
        }
    }

    pub fn toggle_runtime(&mut self) -> Result<DesktopSnapshot, String> {
        if let Some(handle) = self.handle.take() {
            let library = self
                .library
                .as_ref()
                .ok_or_else(|| "shared library is not loaded".to_string())?;
            library.clear_callbacks(handle);
            library.stop(handle)?;
            if let Ok(mut callbacks) = shared_callback_state().lock() {
                callbacks.note_runtime("Runtime stopped from desktop shell.");
            }
            return Ok(self.snapshot());
        }

        if self.library.is_none() {
            self.reload_library();
        }
        let library = self
            .library
            .as_ref()
            .ok_or_else(|| self.shared_bridge_summary())?;
        let config_json = self.settings.config_json()?;

        if let Ok(mut callbacks) = shared_callback_state().lock() {
            callbacks.reset();
            callbacks.note_runtime(format!(
                "Starting live runtime for mesh {}.",
                self.settings.mesh_id()
            ));
        }

        let handle = library.init_handle(self.settings.mesh_id(), &config_json)?;
        library.set_callbacks(handle)?;
        library.start(handle)?;
        library.subscribe(handle, self.settings.initial_room())?;
        if let Some(startup_peer) = self.settings.startup_peer() {
            if let Err(err) = library.connect(handle, startup_peer) {
                if let Ok(mut callbacks) = shared_callback_state().lock() {
                    callbacks.note_runtime(format!(
                        "Startup peer {startup_peer} did not connect immediately: {err}"
                    ));
                }
            }
        }
        self.handle = Some(handle);
        Ok(self.snapshot())
    }

    pub fn update_runtime_settings(
        &mut self,
        input: RuntimeSettingsInput,
    ) -> Result<DesktopSnapshot, String> {
        self.settings.apply(input)?;
        if let Ok(mut callbacks) = shared_callback_state().lock() {
            if self.handle.is_some() {
                callbacks.note_runtime(
                    "Updated desktop runtime settings. Restart the runtime to apply them.",
                );
            } else {
                callbacks.note_runtime("Updated desktop runtime settings.");
            }
        }
        Ok(self.snapshot())
    }

    pub fn subscribe_room(&mut self, room: &str) -> Result<DesktopSnapshot, String> {
        let handle = self
            .handle
            .ok_or_else(|| "runtime is offline; start it first".to_string())?;
        let library = self
            .library
            .as_ref()
            .ok_or_else(|| "shared library is not loaded".to_string())?;
        library.subscribe(handle, room)?;
        if let Ok(mut callbacks) = shared_callback_state().lock() {
            callbacks.note_runtime(format!("Subscribed desktop runtime to #{room}."));
        }
        Ok(self.snapshot())
    }

    pub fn connect_peer(&mut self, addr: &str) -> Result<DesktopSnapshot, String> {
        let handle = self
            .handle
            .ok_or_else(|| "runtime is offline; start it first".to_string())?;
        let library = self
            .library
            .as_ref()
            .ok_or_else(|| "shared library is not loaded".to_string())?;
        library.connect(handle, addr)?;
        if let Ok(mut callbacks) = shared_callback_state().lock() {
            callbacks.note_runtime(format!("Attempting direct connect to {addr}."));
        }
        Ok(self.snapshot())
    }

    pub fn publish_message(&mut self, room: &str, body: &str) -> Result<DesktopSnapshot, String> {
        let handle = self
            .handle
            .ok_or_else(|| "runtime is offline; start it first".to_string())?;
        let library = self
            .library
            .as_ref()
            .ok_or_else(|| "shared library is not loaded".to_string())?;
        library.publish(handle, room, body.as_bytes())?;
        if let Ok(mut callbacks) = shared_callback_state().lock() {
            callbacks.note_runtime(format!("Published desktop message to #{room}."));
        }
        Ok(self.snapshot())
    }

    fn live_mesh_info(&self) -> Result<Option<MeshInfo>, String> {
        let Some(handle) = self.handle else {
            return Ok(None);
        };
        let library = self
            .library
            .as_ref()
            .ok_or_else(|| "shared library not loaded".to_string())?;
        let mut mesh = library.mesh_info(handle)?;
        if let Ok(nat_type) = library.nat_type(handle) {
            mesh.nat_type = nat_type;
        }
        Ok(Some(mesh))
    }

    fn reload_library(&mut self) {
        match MossLibrary::load() {
            Ok(library) => {
                self.library = Some(library);
                self.library_error = None;
            }
            Err(err) => {
                self.library = None;
                self.library_error = Some(err);
            }
        }
    }

    fn shared_bridge_summary(&self) -> String {
        self.library
            .as_ref()
            .map(|library| format!("Loaded from {}", library.path_display()))
            .unwrap_or_else(|| {
                self.library_error
                    .clone()
                    .unwrap_or_else(|| "shared library not loaded".to_string())
            })
    }

    fn library_path(&self) -> String {
        self.library
            .as_ref()
            .map(|library| library.path_display())
            .unwrap_or_else(|| "not loaded".to_string())
    }
}

impl Drop for DesktopShellState {
    fn drop(&mut self) {
        if let (Some(handle), Some(library)) = (self.handle.take(), self.library.as_ref()) {
            library.clear_callbacks(handle);
            let _ = library.stop(handle);
        }
    }
}

pub type SharedDesktopState = Mutex<DesktopShellState>;
