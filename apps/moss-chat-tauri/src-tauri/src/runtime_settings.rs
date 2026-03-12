use serde::Deserialize;
use serde_json::json;

use crate::{
    ffi::MeshInfo,
    models::{RuntimeDiagnostics, RuntimeSettings},
};

const DEFAULT_MESH_ID: &str = "moss-chat-dev";
const DEFAULT_INITIAL_ROOM: &str = "lobby";

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeSettingsInput {
    pub mesh_id: String,
    pub listen_port: u16,
    pub initial_room: String,
    pub startup_peer: String,
    pub tracker_mode: String,
    pub lan_discovery_enabled: bool,
}

#[derive(Clone)]
pub struct DesktopRuntimeConfig {
    mesh_id: String,
    listen_port: u16,
    initial_room: String,
    startup_peer: Option<String>,
    tracker_mode: TrackerMode,
    lan_discovery_enabled: bool,
}

#[derive(Clone, Copy)]
enum TrackerMode {
    Default,
    Disabled,
}

impl Default for DesktopRuntimeConfig {
    fn default() -> Self {
        Self {
            mesh_id: DEFAULT_MESH_ID.to_string(),
            listen_port: 0,
            initial_room: DEFAULT_INITIAL_ROOM.to_string(),
            startup_peer: None,
            tracker_mode: TrackerMode::Default,
            lan_discovery_enabled: true,
        }
    }
}

impl DesktopRuntimeConfig {
    pub fn apply(&mut self, input: RuntimeSettingsInput) -> Result<(), String> {
        self.mesh_id = normalize_id(input.mesh_id, "mesh id")?;
        self.listen_port = input.listen_port;
        self.initial_room = normalize_id(input.initial_room, "initial room")?;
        self.startup_peer = normalize_peer(input.startup_peer)?;
        self.tracker_mode = TrackerMode::parse(&input.tracker_mode)?;
        self.lan_discovery_enabled = input.lan_discovery_enabled;
        Ok(())
    }

    pub fn summary(&self) -> RuntimeSettings {
        RuntimeSettings {
            mesh_id: self.mesh_id.clone(),
            listen_port: self.listen_port,
            initial_room: self.initial_room.clone(),
            startup_peer: self.startup_peer.clone().unwrap_or_default(),
            tracker_mode: self.tracker_mode.as_str().to_string(),
            lan_discovery_enabled: self.lan_discovery_enabled,
            config_preview: self.config_json().unwrap_or_else(|_| "{}".to_string()),
        }
    }

    pub fn diagnostics(&self, mesh: Option<&MeshInfo>) -> RuntimeDiagnostics {
        let (active_mesh_id, active_listen_port, peer_count, channel_count, active_channels, supernode_ready) =
            match mesh {
                Some(mesh) => (
                    mesh.mesh_id.clone(),
                    mesh.listen_port.to_string(),
                    mesh.peer_count,
                    mesh.channels.len(),
                    mesh.channels.clone(),
                    mesh.supernode_ready,
                ),
                None => (
                    "offline".to_string(),
                    "offline".to_string(),
                    0,
                    0,
                    Vec::new(),
                    false,
                ),
            };

        RuntimeDiagnostics {
            configured_mesh_id: self.mesh_id.clone(),
            configured_listen_port: port_label(self.listen_port),
            initial_room: format!("#{}", self.initial_room),
            startup_peer: self.startup_peer.clone().unwrap_or_else(|| "not set".to_string()),
            tracker_mode: self.tracker_mode.label().to_string(),
            lan_discovery: if self.lan_discovery_enabled {
                "enabled".to_string()
            } else {
                "disabled".to_string()
            },
            active_mesh_id,
            active_listen_port,
            peer_count,
            channel_count,
            active_channels,
            supernode_ready,
        }
    }

    pub fn config_json(&self) -> Result<String, String> {
        let mut config = json!({
            "listen_port": self.listen_port,
            "lan_discovery_enabled": self.lan_discovery_enabled,
        });
        if matches!(self.tracker_mode, TrackerMode::Disabled) {
            config["trackers"] = json!([]);
        }
        serde_json::to_string(&config).map_err(|err| format!("invalid runtime config json: {err}"))
    }

    pub fn mesh_id(&self) -> &str {
        &self.mesh_id
    }

    pub fn initial_room(&self) -> &str {
        &self.initial_room
    }

    pub fn startup_peer(&self) -> Option<&str> {
        self.startup_peer.as_deref()
    }

}

impl TrackerMode {
    fn parse(value: &str) -> Result<Self, String> {
        match value.trim() {
            "default" => Ok(Self::Default),
            "disabled" => Ok(Self::Disabled),
            _ => Err("tracker mode must be default or disabled".to_string()),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::Disabled => "disabled",
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Default => "Built-in tracker bootstrap",
            Self::Disabled => "Tracker bootstrap disabled",
        }
    }
}

fn normalize_id(value: String, field_name: &str) -> Result<String, String> {
    let normalized = value.trim().trim_start_matches('#').to_lowercase();
    if normalized.is_empty() {
        return Err(format!("{field_name} is required"));
    }
    if normalized.len() > 64 {
        return Err(format!("{field_name} must be 64 characters or fewer"));
    }
    if !normalized
        .chars()
        .all(|char| char.is_ascii_alphanumeric() || matches!(char, '-' | '_' | '.'))
    {
        return Err(format!(
            "{field_name} may only contain letters, numbers, dash, underscore, and dot"
        ));
    }
    Ok(normalized)
}

fn normalize_peer(value: String) -> Result<Option<String>, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let Some((host, port)) = trimmed.rsplit_once(':') else {
        return Err("startup peer must use HOST:PORT".to_string());
    };
    if host.is_empty() || host.contains(char::is_whitespace) {
        return Err("startup peer host is invalid".to_string());
    }
    let parsed_port = port
        .parse::<u16>()
        .map_err(|_| "startup peer port is invalid".to_string())?;
    if parsed_port == 0 {
        return Err("startup peer port must be greater than zero".to_string());
    }
    Ok(Some(trimmed.to_string()))
}

fn port_label(port: u16) -> String {
    if port == 0 {
        "auto".to_string()
    } else {
        port.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::{DesktopRuntimeConfig, RuntimeSettingsInput};

    #[test]
    fn runtime_settings_normalize_inputs() {
        let mut config = DesktopRuntimeConfig::default();
        config
            .apply(RuntimeSettingsInput {
                mesh_id: " Moss-Chat-Live ".to_string(),
                listen_port: 41030,
                initial_room: "#Lobby".to_string(),
                startup_peer: "example.com:41031".to_string(),
                tracker_mode: "disabled".to_string(),
                lan_discovery_enabled: false,
            })
            .expect("runtime settings should normalize");

        let summary = config.summary();
        assert_eq!(summary.mesh_id, "moss-chat-live");
        assert_eq!(summary.initial_room, "lobby");
        assert_eq!(summary.startup_peer, "example.com:41031");
        assert_eq!(summary.tracker_mode, "disabled");
        assert!(!summary.lan_discovery_enabled);
        assert!(summary.config_preview.contains("\"trackers\":[]"));
    }

    #[test]
    fn runtime_settings_reject_invalid_room() {
        let mut config = DesktopRuntimeConfig::default();
        let err = config
            .apply(RuntimeSettingsInput {
                mesh_id: "moss-chat-live".to_string(),
                listen_port: 0,
                initial_room: "release war room".to_string(),
                startup_peer: String::new(),
                tracker_mode: "default".to_string(),
                lan_discovery_enabled: true,
            })
            .expect_err("spaces must be rejected");

        assert!(err.contains("initial room"));
    }

    #[test]
    fn runtime_settings_reject_invalid_startup_peer() {
        let mut config = DesktopRuntimeConfig::default();
        let err = config
            .apply(RuntimeSettingsInput {
                mesh_id: "moss-chat-live".to_string(),
                listen_port: 0,
                initial_room: "lobby".to_string(),
                startup_peer: "not-a-peer".to_string(),
                tracker_mode: "default".to_string(),
                lan_discovery_enabled: true,
            })
            .expect_err("startup peer validation should fail");

        assert!(err.contains("HOST:PORT"));
    }
}
