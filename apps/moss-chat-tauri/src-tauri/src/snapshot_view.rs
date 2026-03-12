use crate::{
    callback_state::shared_callback_state,
    ffi::MeshInfo,
    models::{
        DesktopSnapshot, Message, PeerSummary, RoomSummary, RuntimeDiagnostics,
        RuntimeSettings, RuntimeStatus,
    },
};

pub fn online_snapshot(
    mesh: &MeshInfo,
    settings: RuntimeSettings,
    diagnostics: RuntimeDiagnostics,
    bridge_path: String,
    branch: &str,
) -> DesktopSnapshot {
    let (rooms, messages, peers) = live_callback_rows(mesh);
    DesktopSnapshot {
        app_name: "Moss Chat".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        branch: branch.to_string(),
        stage: "Desktop runtime".to_string(),
        runtime: RuntimeStatus {
            state: "Runtime online".to_string(),
            summary: format!(
                "Live Moss runtime is active on mesh {} with {} connected peers and {} subscribed channels.",
                mesh.mesh_id,
                mesh.peer_count,
                mesh.channels.len()
            ),
            route: if mesh.peer_count > 0 {
                format!("Connected peers: {}", mesh.peers.join(", "))
            } else {
                "Waiting for peer connections".to_string()
            },
            nat_hint: mesh.nat_type.clone(),
            shared_bridge: format!(
                "Loaded from {} as {}",
                bridge_path,
                shorten_public_key(&mesh.public_key)
            ),
        },
        diagnostics,
        settings,
        rooms,
        messages,
        peers,
    }
}

pub fn offline_snapshot(
    settings: RuntimeSettings,
    diagnostics: RuntimeDiagnostics,
    shared_bridge: String,
    branch: &str,
) -> DesktopSnapshot {
    DesktopSnapshot {
        app_name: "Moss Chat".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        branch: branch.to_string(),
        stage: "Desktop runtime".to_string(),
        runtime: RuntimeStatus {
            state: "Runtime offline".to_string(),
            summary: "The desktop shell is ready to start a real Moss node through the shared runtime.".to_string(),
            route: "No active transport".to_string(),
            nat_hint: "Unknown until runtime starts".to_string(),
            shared_bridge,
        },
        diagnostics,
        settings,
        rooms: base_rooms(),
        messages: base_messages(),
        peers: base_peers(),
    }
}

pub fn failed_snapshot(
    settings: RuntimeSettings,
    diagnostics: RuntimeDiagnostics,
    error: String,
    branch: &str,
) -> DesktopSnapshot {
    DesktopSnapshot {
        app_name: "Moss Chat".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        branch: branch.to_string(),
        stage: "Desktop runtime".to_string(),
        runtime: RuntimeStatus {
            state: "Runtime unavailable".to_string(),
            summary: "Desktop backend could not load or query libmoss. Place the shared library next to the executable or set MOSS_SHARED_PATH.".to_string(),
            route: "No active transport".to_string(),
            nat_hint: "Unknown".to_string(),
            shared_bridge: error.clone(),
        },
        diagnostics,
        settings,
        rooms: base_rooms(),
        messages: vec![Message {
            id: "m-failed-1".to_string(),
            room_id: "system".to_string(),
            author: "System".to_string(),
            body: error,
            timestamp: "now".to_string(),
            emphasis: "system".to_string(),
        }],
        peers: base_peers(),
    }
}

fn base_rooms() -> Vec<RoomSummary> {
    vec![
        RoomSummary {
            id: "system".to_string(),
            label: "System".to_string(),
            unread: 1,
            participants: 1,
            kind: "system".to_string(),
        },
        RoomSummary {
            id: "lobby".to_string(),
            label: "#lobby".to_string(),
            unread: 0,
            participants: 1,
            kind: "channel".to_string(),
        },
    ]
}

fn base_peers() -> Vec<PeerSummary> {
    vec![PeerSummary {
        id: "peer-self".to_string(),
        display_name: "Desktop operator".to_string(),
        route: "local shell".to_string(),
        latency: "--".to_string(),
        status: "self".to_string(),
        rooms: vec!["#lobby".to_string()],
    }]
}

fn base_messages() -> Vec<Message> {
    vec![
        Message {
            id: "m-offline-1".to_string(),
            room_id: "system".to_string(),
            author: "System".to_string(),
            body: "Configure mesh settings, then start the runtime to join a live Moss chat.".to_string(),
            timestamp: "now".to_string(),
            emphasis: "system".to_string(),
        },
        Message {
            id: "m-offline-2".to_string(),
            room_id: "lobby".to_string(),
            author: "Moss".to_string(),
            body: "Messages, rooms, and peers are driven by libmoss once the runtime is online.".to_string(),
            timestamp: "now".to_string(),
            emphasis: "normal".to_string(),
        },
    ]
}

fn live_callback_rows(mesh: &MeshInfo) -> (Vec<RoomSummary>, Vec<Message>, Vec<PeerSummary>) {
    let callback_state = shared_callback_state();
    if let Ok(state) = callback_state.lock() {
        let mut rooms = state.rooms();
        let mut messages = state.messages();
        let mut peers = state.peers();

        if rooms.is_empty() {
            rooms = live_room_rows(mesh);
        } else {
            merge_room_rows(&mut rooms, mesh);
        }

        if messages.is_empty() {
            messages = base_messages();
        }

        merge_peer_rows(&mut peers, mesh);
        return (rooms, messages, peers);
    }
    (live_room_rows(mesh), base_messages(), live_peer_rows(mesh))
}

fn live_room_rows(mesh: &MeshInfo) -> Vec<RoomSummary> {
    let mut rooms = vec![RoomSummary {
        id: "system".to_string(),
        label: "System".to_string(),
        unread: 1,
        participants: 1,
        kind: "system".to_string(),
    }];
    if mesh.channels.is_empty() {
        rooms.push(RoomSummary {
            id: "lobby".to_string(),
            label: "#lobby".to_string(),
            unread: 0,
            participants: (mesh.peer_count + 1) as u32,
            kind: "channel".to_string(),
        });
        return rooms;
    }
    for channel in &mesh.channels {
        rooms.push(RoomSummary {
            id: channel.to_lowercase(),
            label: format!("#{channel}"),
            unread: 0,
            participants: (mesh.peer_count + 1) as u32,
            kind: "channel".to_string(),
        });
    }
    rooms
}

fn live_peer_rows(mesh: &MeshInfo) -> Vec<PeerSummary> {
    let mut peers = vec![PeerSummary {
        id: "peer-self".to_string(),
        display_name: "Desktop operator".to_string(),
        route: format!("listen {}", mesh.listen_port),
        latency: "--".to_string(),
        status: "self".to_string(),
        rooms: mesh
            .channels
            .iter()
            .map(|channel| format!("#{channel}"))
            .collect(),
    }];
    for (index, peer) in mesh.peers.iter().enumerate() {
        peers.push(PeerSummary {
            id: format!("peer-{index}"),
            display_name: peer.clone(),
            route: if mesh.supernode_ready {
                "direct or relay candidate".to_string()
            } else {
                "connected peer".to_string()
            },
            latency: "runtime pending".to_string(),
            status: "connected".to_string(),
            rooms: mesh
                .channels
                .iter()
                .map(|channel| format!("#{channel}"))
                .collect(),
        });
    }
    peers
}

fn merge_room_rows(rooms: &mut Vec<RoomSummary>, mesh: &MeshInfo) {
    for channel in &mesh.channels {
        let id = channel.to_lowercase();
        if rooms.iter().any(|room| room.id == id) {
            continue;
        }
        rooms.push(RoomSummary {
            id,
            label: format!("#{channel}"),
            unread: 0,
            participants: (mesh.peer_count + 1) as u32,
            kind: "channel".to_string(),
        });
    }
}

fn merge_peer_rows(peers: &mut Vec<PeerSummary>, mesh: &MeshInfo) {
    if !peers.iter().any(|peer| peer.status == "self") {
        peers.insert(
            0,
            PeerSummary {
                id: "peer-self".to_string(),
                display_name: "Desktop operator".to_string(),
                route: format!("listen {}", mesh.listen_port),
                latency: "--".to_string(),
                status: "self".to_string(),
                rooms: mesh
                    .channels
                    .iter()
                    .map(|channel| format!("#{channel}"))
                    .collect(),
            },
        );
    }
    for (index, peer_addr) in mesh.peers.iter().enumerate() {
        if peers
            .iter()
            .any(|peer| peer.route == *peer_addr || peer.display_name == *peer_addr)
        {
            continue;
        }
        peers.push(PeerSummary {
            id: format!("peer-live-{index}"),
            display_name: peer_addr.clone(),
            route: "connected peer".to_string(),
            latency: "runtime pending".to_string(),
            status: "connected".to_string(),
            rooms: mesh
                .channels
                .iter()
                .map(|channel| format!("#{channel}"))
                .collect(),
        });
    }
}

fn shorten_public_key(value: &str) -> String {
    if value.len() <= 12 {
        value.to_string()
    } else {
        format!("{}..{}", &value[..8], &value[value.len() - 6..])
    }
}
