use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

pub const CONTROL_ROOM: &str = "__moss_chat_control__";

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ChatPayload {
    #[serde(default)]
    pub kind: String,
    #[serde(default)]
    pub nick: String,
    #[serde(default)]
    pub text: String,
    #[serde(default)]
    pub sent_at: String,
    #[serde(default)]
    pub room: String,
    #[serde(default)]
    pub rooms: Vec<String>,
    #[serde(default)]
    pub target: String,
}

impl ChatPayload {
    pub fn room_message(nick: &str, text: &str) -> Self {
        Self {
            nick: nick.to_string(),
            text: text.to_string(),
            sent_at: hhmmss_now(),
            ..Self::default()
        }
    }

    pub fn presence(nick: &str, rooms: &[String]) -> Self {
        Self {
            kind: "presence".to_string(),
            nick: nick.to_string(),
            sent_at: hhmmss_now(),
            rooms: rooms.to_vec(),
            ..Self::default()
        }
    }

    pub fn dm_invite(nick: &str, room: &str, target: &str) -> Self {
        Self {
            kind: "dm_invite".to_string(),
            nick: nick.to_string(),
            room: room.to_string(),
            target: target.to_string(),
            sent_at: hhmmss_now(),
            ..Self::default()
        }
    }
}

pub fn direct_room_name(local_peer_id: &str, remote_peer_id: &str) -> String {
    let mut a = local_peer_id.trim().to_lowercase();
    let mut b = remote_peer_id.trim().to_lowercase();
    if a.is_empty() || b.is_empty() {
        return "dm".to_string();
    }
    if a > b {
        std::mem::swap(&mut a, &mut b);
    }
    format!("dm-{}-{}", truncate_peer(&a), truncate_peer(&b))
}

pub fn normalize_room_id(channel: &str) -> String {
    let trimmed = channel.trim().trim_start_matches('#');
    if trimmed.is_empty() {
        "lobby".to_string()
    } else {
        trimmed.to_lowercase()
    }
}

pub fn format_peer(peer_hex: &str) -> String {
    if peer_hex.len() <= 12 {
        peer_hex.to_string()
    } else {
        format!("{}..{}", &peer_hex[..6], &peer_hex[peer_hex.len() - 4..])
    }
}

fn truncate_peer(value: &str) -> &str {
    let upper = value.len().min(16);
    &value[..upper]
}

fn hhmmss_now() -> String {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let seconds = duration.as_secs() % 86_400;
    let hours = seconds / 3_600;
    let minutes = (seconds % 3_600) / 60;
    let secs = seconds % 60;
    format!("{hours:02}:{minutes:02}:{secs:02}")
}

#[cfg(test)]
mod tests {
    use super::{direct_room_name, format_peer, normalize_room_id};

    #[test]
    fn direct_room_name_is_stable() {
        let a = "abcdefabcdefabcdefabcdefabcdefab";
        let b = "0123456789abcdef0123456789abcdef";
        assert_eq!(direct_room_name(a, b), direct_room_name(b, a));
    }

    #[test]
    fn normalize_room_strips_hash_prefix() {
        assert_eq!(normalize_room_id("#Lobby"), "lobby");
    }

    #[test]
    fn format_peer_shortens_long_hex() {
        assert!(format_peer("abcdefabcdefabcdef").contains(".."));
    }
}
