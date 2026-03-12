use serde::Serialize;

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Artifact {
    pub name: String,
    pub platform: String,
    pub notes: String,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Milestone {
    pub title: String,
    pub detail: String,
    pub status: String,
}

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
    pub rooms: Vec<RoomSummary>,
    pub messages: Vec<Message>,
    pub peers: Vec<PeerSummary>,
    pub artifacts: Vec<Artifact>,
    pub milestones: Vec<Milestone>,
}
