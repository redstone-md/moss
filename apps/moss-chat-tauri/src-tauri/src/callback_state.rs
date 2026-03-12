use std::{collections::BTreeMap, sync::{Arc, Mutex, OnceLock}};

use serde_json::Value;

use crate::models::{Message, PeerSummary, RoomSummary};

#[derive(Default)]
pub struct CallbackState {
    next_id: u64,
    messages: Vec<Message>,
    rooms: BTreeMap<String, RoomSummary>,
    peers: BTreeMap<String, PeerSummary>,
}

impl CallbackState {
    pub fn new() -> Self {
        let mut state = Self::default();
        state.ensure_room("system", "System", "system");
        state.ensure_room("lobby", "#lobby", "channel");
        state
    }

    pub fn reset(&mut self) {
        self.next_id = 0;
        self.messages.clear();
        self.rooms.clear();
        self.peers.clear();
        self.ensure_room("system", "System", "system");
        self.ensure_room("lobby", "#lobby", "channel");
    }

    pub fn note_runtime(&mut self, body: impl Into<String>) {
        self.push_message("system", "System", body.into(), "system");
    }

    pub fn on_channel_message(&mut self, channel: String, sender_hex: String, data: Vec<u8>) {
        let room_id = normalize_room_id(&channel);
        let room_label = if room_id == "system" {
            "System".to_string()
        } else {
            format!("#{channel}")
        };
        self.ensure_room(&room_id, &room_label, if room_id == "system" { "system" } else { "channel" });
        let author = short_peer_label(&sender_hex);
        let body = String::from_utf8_lossy(&data).into_owned();
        self.push_message(&room_id, &author, body, "normal");
    }

    pub fn on_event(&mut self, event_type: i32, detail_json: String) {
        let detail: Value = serde_json::from_str(&detail_json).unwrap_or(Value::Null);
        match event_type {
            1 => {
                let peer = detail_field(&detail, "peer");
                let addr = detail_field(&detail, "addr");
                let label = if !addr.is_empty() { addr.clone() } else { short_peer_label(&peer) };
                self.peers.insert(
                    peer.clone(),
                    PeerSummary {
                        id: peer.clone(),
                        display_name: label,
                        route: addr,
                        latency: "live".to_string(),
                        status: "connected".to_string(),
                        rooms: vec!["#lobby".to_string()],
                    },
                );
                self.bump_room_participants();
                self.push_message(
                    "system",
                    "System",
                    format!("{} joined the mesh.", short_peer_label(&peer)),
                    "system",
                );
            }
            2 => {
                let peer = detail_field(&detail, "peer");
                self.peers.remove(&peer);
                self.bump_room_participants();
                self.push_message(
                    "system",
                    "System",
                    format!("{} left the mesh.", short_peer_label(&peer)),
                    "system",
                );
            }
            5 => {
                let candidates = detail.get("candidate_peers").and_then(Value::as_u64).unwrap_or(0);
                let connected = detail.get("connected_peers").and_then(Value::as_u64).unwrap_or(0);
                self.push_message(
                    "system",
                    "System",
                    format!("Tracker returned {candidates} candidates; connected now {connected}."),
                    "system",
                );
            }
            6 => {
                self.push_message(
                    "system",
                    "System",
                    format!("Tracker failure: {}", detail_field(&detail, "error")),
                    "system",
                );
            }
            7 => {
                self.push_message(
                    "system",
                    "System",
                    format!(
                        "Relay migrated to direct route for {} via {}.",
                        short_peer_label(&detail_field(&detail, "peer")),
                        detail_field(&detail, "via")
                    ),
                    "system",
                );
            }
            _ => {
                self.push_message("system", "System", detail_json, "system");
            }
        }
    }

    pub fn rooms(&self) -> Vec<RoomSummary> {
        self.rooms.values().cloned().collect()
    }

    pub fn messages(&self) -> Vec<Message> {
        self.messages.clone()
    }

    pub fn peers(&self) -> Vec<PeerSummary> {
        self.peers.values().cloned().collect()
    }

    fn ensure_room(&mut self, id: &str, label: &str, kind: &str) {
        self.rooms.entry(id.to_string()).or_insert(RoomSummary {
            id: id.to_string(),
            label: label.to_string(),
            unread: 0,
            participants: 1,
            kind: kind.to_string(),
        });
    }

    fn push_message(&mut self, room_id: &str, author: &str, body: String, emphasis: &str) {
        self.next_id += 1;
        self.messages.push(Message {
            id: format!("cb-{}", self.next_id),
            room_id: room_id.to_string(),
            author: author.to_string(),
            body,
            timestamp: "now".to_string(),
            emphasis: emphasis.to_string(),
        });
    }

    fn bump_room_participants(&mut self) {
        let participants = self.peers.len() as u32 + 1;
        for room in self.rooms.values_mut() {
            if room.kind != "system" {
                room.participants = participants.max(1);
            }
        }
    }
}

fn normalize_room_id(channel: &str) -> String {
    let trimmed = channel.trim().trim_start_matches('#');
    if trimmed.is_empty() {
        "lobby".to_string()
    } else {
        trimmed.to_lowercase()
    }
}

fn short_peer_label(peer_hex: &str) -> String {
    if peer_hex.len() <= 12 {
        peer_hex.to_string()
    } else {
        format!("{}..{}", &peer_hex[..6], &peer_hex[peer_hex.len() - 4..])
    }
}

fn detail_field(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

static CALLBACK_STATE: OnceLock<Arc<Mutex<CallbackState>>> = OnceLock::new();

pub fn shared_callback_state() -> Arc<Mutex<CallbackState>> {
    CALLBACK_STATE
        .get_or_init(|| Arc::new(Mutex::new(CallbackState::new())))
        .clone()
}
