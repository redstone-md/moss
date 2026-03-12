use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex, OnceLock},
};

use serde_json::Value;

use crate::{
    chat_protocol::{format_peer, normalize_room_id, ChatPayload, CONTROL_ROOM},
    models::{Message, PeerSummary, RoomSummary},
};

#[derive(Default)]
pub struct CallbackState {
    next_id: u64,
    local_peer_id: String,
    local_nickname: String,
    self_rooms: BTreeSet<String>,
    presence_seen: BTreeSet<String>,
    peer_names: BTreeMap<String, String>,
    peer_rooms: BTreeMap<String, BTreeSet<String>>,
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
        self.local_peer_id.clear();
        self.local_nickname.clear();
        self.self_rooms.clear();
        self.presence_seen.clear();
        self.peer_names.clear();
        self.peer_rooms.clear();
        self.messages.clear();
        self.rooms.clear();
        self.peers.clear();
        self.ensure_room("system", "System", "system");
        self.ensure_room("lobby", "#lobby", "channel");
    }

    pub fn note_runtime(&mut self, body: impl Into<String>) {
        self.push_message("system", "System", body.into(), "system");
    }

    pub fn configure_local_profile(&mut self, peer_id: String, nickname: String, rooms: &[String]) {
        self.local_peer_id = peer_id.clone();
        self.local_nickname = nickname.clone();
        self.peer_names.insert(peer_id.clone(), nickname.clone());
        self.self_rooms = rooms.iter().map(|room| normalize_room_id(room)).collect();

        let room_labels = self
            .self_rooms
            .iter()
            .map(|room| format!("#{room}"))
            .collect::<Vec<_>>();

        self.peers.insert(
            peer_id,
            PeerSummary {
                id: "peer-self".to_string(),
                display_name: format!("{nickname} (you)"),
                route: "local shell".to_string(),
                latency: "--".to_string(),
                status: "self".to_string(),
                rooms: room_labels,
            },
        );

        let self_rooms = self.self_rooms.iter().cloned().collect::<Vec<_>>();
        for room in self_rooms {
            self.ensure_room(&room, &format!("#{room}"), "channel");
        }
        self.bump_room_participants();
    }

    pub fn record_subscribed_room(&mut self, room: &str) {
        let normalized = normalize_room_id(room);
        self.self_rooms.insert(normalized.clone());
        self.ensure_room(&normalized, &format!("#{normalized}"), "channel");
        if let Some(peer) = self.self_peer_mut() {
            let label = format!("#{normalized}");
            if !peer.rooms.iter().any(|room| room == &label) {
                peer.rooms.push(label);
                peer.rooms.sort();
            }
        }
        self.bump_room_participants();
    }

    pub fn subscribed_rooms(&self) -> Vec<String> {
        self.self_rooms.iter().cloned().collect()
    }

    pub fn on_channel_message(&mut self, channel: String, sender_hex: String, data: Vec<u8>) {
        if channel == CONTROL_ROOM {
            self.handle_control_message(sender_hex, data);
            return;
        }

        let room_id = normalize_room_id(&channel);
        self.ensure_room(&room_id, &format!("#{room_id}"), "channel");

        let parsed_payload = serde_json::from_slice::<ChatPayload>(&data).ok();
        let author = parsed_payload
            .as_ref()
            .and_then(|payload| {
                let nick = payload.nick.trim();
                if nick.is_empty() {
                    None
                } else if sender_hex == self.local_peer_id {
                    Some("you".to_string())
                } else {
                    self.peer_names.insert(sender_hex.clone(), nick.to_string());
                    Some(nick.to_string())
                }
            })
            .unwrap_or_else(|| self.display_name_for_peer(&sender_hex));

        let body = parsed_payload
            .as_ref()
            .map(|payload| payload.text.trim().to_string())
            .filter(|text| !text.is_empty())
            .unwrap_or_else(|| String::from_utf8_lossy(&data).into_owned());

        let timestamp = parsed_payload
            .as_ref()
            .map(|payload| payload.sent_at.trim().to_string())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "now".to_string());

        let message_id = self.next_message_id();
        self.messages.push(Message {
            id: message_id,
            room_id,
            author,
            body,
            timestamp,
            emphasis: "normal".to_string(),
        });
    }

    pub fn on_event(&mut self, event_type: i32, detail_json: String) {
        let detail: Value = serde_json::from_str(&detail_json).unwrap_or(Value::Null);
        match event_type {
            1 => {
                let peer = detail_field(&detail, "peer");
                if peer.is_empty() || peer == self.local_peer_id {
                    return;
                }
                let addr = detail_field(&detail, "addr");
                self.peers.insert(
                    peer.clone(),
                    PeerSummary {
                        id: peer.clone(),
                        display_name: self.display_name_for_peer(&peer),
                        route: fallback_text(&addr, "connected peer"),
                        latency: "live".to_string(),
                        status: "connected".to_string(),
                        rooms: vec!["#lobby".to_string()],
                    },
                );
                self.bump_room_participants();
            }
            2 => {
                let peer = detail_field(&detail, "peer");
                if peer.is_empty() {
                    return;
                }
                let name = self.display_name_for_peer(&peer);
                self.peers.remove(&peer);
                self.peer_rooms.remove(&peer);
                self.presence_seen.remove(&peer);
                self.bump_room_participants();
                self.push_message(
                    "lobby",
                    "System",
                    format!("{name} left the chat."),
                    "system",
                );
            }
            5 => {
                let candidates = detail
                    .get("candidate_peers")
                    .and_then(Value::as_u64)
                    .unwrap_or(0);
                let connected = detail
                    .get("connected_peers")
                    .and_then(Value::as_u64)
                    .unwrap_or(0);
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
                        format_peer(&detail_field(&detail, "peer")),
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

    pub fn resolve_peer_target(&self, target: &str) -> Option<(String, String)> {
        let needle = target.trim().to_lowercase();
        if needle.is_empty() {
            return None;
        }
        for (peer_id, peer_name) in &self.peer_names {
            if peer_id == &self.local_peer_id {
                continue;
            }
            if peer_name.eq_ignore_ascii_case(target)
                || peer_id.to_lowercase().starts_with(&needle)
                || format_peer(peer_id).to_lowercase().starts_with(&needle)
            {
                return Some((peer_id.clone(), peer_name.clone()));
            }
        }
        None
    }

    fn handle_control_message(&mut self, sender_hex: String, data: Vec<u8>) {
        let Ok(payload) = serde_json::from_slice::<ChatPayload>(&data) else {
            return;
        };
        if !payload.target.is_empty() && payload.target != self.local_peer_id {
            return;
        }

        if !payload.nick.trim().is_empty() {
            self.peer_names
                .insert(sender_hex.clone(), payload.nick.trim().to_string());
        }

        match payload.kind.as_str() {
            "presence" => {
                if sender_hex == self.local_peer_id {
                    return;
                }
                let first_seen = self.presence_seen.insert(sender_hex.clone());
                let peer_name = self.display_name_for_peer(&sender_hex);
                let peer_rooms = payload
                    .rooms
                    .iter()
                    .map(|room| normalize_room_id(room))
                    .collect::<BTreeSet<_>>();
                self.peer_rooms.insert(sender_hex.clone(), peer_rooms.clone());
                self.peers
                    .entry(sender_hex.clone())
                    .and_modify(|peer| {
                        peer.display_name = peer_name.clone();
                        peer.rooms = peer_rooms
                            .iter()
                            .map(|room| format!("#{room}"))
                            .collect::<Vec<_>>();
                    })
                    .or_insert(PeerSummary {
                        id: sender_hex.clone(),
                        display_name: peer_name.clone(),
                        route: "connected peer".to_string(),
                        latency: "live".to_string(),
                        status: "connected".to_string(),
                        rooms: peer_rooms
                            .iter()
                            .map(|room| format!("#{room}"))
                            .collect::<Vec<_>>(),
                    });
                for room in peer_rooms {
                    self.ensure_room(&room, &format!("#{room}"), "channel");
                }
                self.bump_room_participants();
                if first_seen {
                    self.push_message(
                        "lobby",
                        "System",
                        format!("{peer_name} joined the chat."),
                        "system",
                    );
                }
            }
            "dm_invite" => {
                let room = normalize_room_id(&payload.room);
                let peer_name = self.display_name_for_peer(&sender_hex);
                self.ensure_room(&room, &format!("@{peer_name}"), "dm");
                if let Some(summary) = self.rooms.get_mut(&room) {
                    summary.label = format!("@{peer_name}");
                }
                self.peer_rooms
                    .entry(sender_hex.clone())
                    .or_default()
                    .insert(room.clone());
                if let Some(peer) = self.peers.get_mut(&sender_hex) {
                    let label = format!("#{room}");
                    if !peer.rooms.iter().any(|candidate| candidate == &label) {
                        peer.rooms.push(label);
                        peer.rooms.sort();
                    }
                }
                self.push_message(
                    "system",
                    "System",
                    format!("Direct chat ready with {peer_name}."),
                    "system",
                );
            }
            _ => {}
        }
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

    fn display_name_for_peer(&self, peer_id: &str) -> String {
        self.peer_names
            .get(peer_id)
            .filter(|name| !name.trim().is_empty())
            .cloned()
            .unwrap_or_else(|| format_peer(peer_id))
    }

    fn self_peer_mut(&mut self) -> Option<&mut PeerSummary> {
        self.peers.values_mut().find(|peer| peer.status == "self")
    }

    fn push_message(&mut self, room_id: &str, author: &str, body: String, emphasis: &str) {
        let message_id = self.next_message_id();
        self.messages.push(Message {
            id: message_id,
            room_id: room_id.to_string(),
            author: author.to_string(),
            body,
            timestamp: "now".to_string(),
            emphasis: emphasis.to_string(),
        });
    }

    fn bump_room_participants(&mut self) {
        for room in self.rooms.values_mut() {
            if room.kind == "system" {
                continue;
            }
            let room_name = room.id.clone();
            let peer_count = self
                .peer_rooms
                .values()
                .filter(|rooms| rooms.contains(&room_name))
                .count() as u32;
            let local_present = self.self_rooms.contains(&room_name) as u32;
            room.participants = (peer_count + local_present).max(1);
        }
    }

    fn next_message_id(&mut self) -> String {
        self.next_id += 1;
        format!("cb-{}", self.next_id)
    }
}

fn detail_field(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

fn fallback_text(value: &str, fallback: &str) -> String {
    if value.trim().is_empty() {
        fallback.to_string()
    } else {
        value.to_string()
    }
}

static CALLBACK_STATE: OnceLock<Arc<Mutex<CallbackState>>> = OnceLock::new();

pub fn shared_callback_state() -> Arc<Mutex<CallbackState>> {
    CALLBACK_STATE
        .get_or_init(|| Arc::new(Mutex::new(CallbackState::new())))
        .clone()
}
