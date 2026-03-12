use serde::Serialize;

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeStatus {
    pub state: String,
    pub summary: String,
    pub route: String,
    pub nat_hint: String,
    pub shared_bridge: String,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeSettings {
    pub mesh_id: String,
    pub listen_port: u16,
    pub initial_room: String,
    pub startup_peer: String,
    pub tracker_mode: String,
    pub lan_discovery_enabled: bool,
    pub config_preview: String,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeDiagnostics {
    pub configured_mesh_id: String,
    pub configured_listen_port: String,
    pub initial_room: String,
    pub startup_peer: String,
    pub tracker_mode: String,
    pub lan_discovery: String,
    pub active_mesh_id: String,
    pub active_listen_port: String,
    pub peer_count: usize,
    pub channel_count: usize,
    pub active_channels: Vec<String>,
    pub supernode_ready: bool,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RoomSummary {
    pub id: String,
    pub label: String,
    pub unread: u32,
    pub participants: u32,
    pub kind: String,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Message {
    pub id: String,
    pub room_id: String,
    pub author: String,
    pub body: String,
    pub timestamp: String,
    pub emphasis: String,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PeerSummary {
    pub id: String,
    pub display_name: String,
    pub route: String,
    pub latency: String,
    pub status: String,
    pub rooms: Vec<String>,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DesktopSnapshot {
    pub app_name: String,
    pub version: String,
    pub branch: String,
    pub stage: String,
    pub runtime: RuntimeStatus,
    pub settings: RuntimeSettings,
    pub diagnostics: RuntimeDiagnostics,
    pub rooms: Vec<RoomSummary>,
    pub messages: Vec<Message>,
    pub peers: Vec<PeerSummary>,
}
