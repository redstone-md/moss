use std::sync::Mutex;

use crate::{
    ffi::{MeshInfo, MossLibrary},
    models::{Artifact, DesktopSnapshot, Message, Milestone, PeerSummary, RoomSummary, RuntimeStatus},
};

pub struct DesktopShellState {
    library: Option<MossLibrary>,
    library_error: Option<String>,
    handle: Option<i64>,
    mesh_id: String,
    config_json: String,
}

impl DesktopShellState {
    pub fn new() -> Self {
        let mut state = Self {
            library: None,
            library_error: None,
            handle: None,
            mesh_id: "moss-chat-dev".to_string(),
            config_json: "{\"listen_port\":0}".to_string(),
        };
        state.reload_library();
        state
    }

    pub fn snapshot(&mut self) -> DesktopSnapshot {
        if self.library.is_none() {
            self.reload_library();
        }

        let (runtime, rooms, messages, peers, stage) = match self.live_mesh_info() {
            Ok(Some(mesh)) => self.online_shell(mesh),
            Ok(None) => self.offline_shell(),
            Err(err) => self.failed_shell(err),
        };

        DesktopSnapshot {
            app_name: "Moss Chat Dev".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            branch: "dev".to_string(),
            stage,
            runtime,
            rooms,
            messages,
            peers,
            artifacts: artifact_rows(),
            milestones: milestone_rows(),
        }
    }

    pub fn toggle_runtime(&mut self) -> Result<DesktopSnapshot, String> {
        if let Some(handle) = self.handle.take() {
            let library = self
                .library
                .as_ref()
                .ok_or_else(|| "shared library is not loaded".to_string())?;
            library.stop(handle)?;
            return Ok(self.snapshot());
        }

        if self.library.is_none() {
            self.reload_library();
        }
        let library = self
            .library
            .as_ref()
            .ok_or_else(|| self.library_status())?;
        let handle = library.init_handle(&self.mesh_id, &self.config_json)?;
        library.start(handle)?;
        library.subscribe(handle, "lobby")?;
        self.handle = Some(handle);
        Ok(self.snapshot())
    }

    fn online_shell(
        &self,
        mesh: MeshInfo,
    ) -> (RuntimeStatus, Vec<RoomSummary>, Vec<Message>, Vec<PeerSummary>, String) {
        let peers = live_peer_rows(&mesh);
        let rooms = live_room_rows(&mesh);
        let messages = vec![
            Message {
                id: "m-live-1".to_string(),
                room_id: "system".to_string(),
                author: "System".to_string(),
                body: format!(
                    "Live runtime started from {} on mesh {}.",
                    self.library_path(),
                    mesh.mesh_id
                ),
                timestamp: "now".to_string(),
                emphasis: "system".to_string(),
            },
            Message {
                id: "m-live-2".to_string(),
                room_id: "system".to_string(),
                author: "System".to_string(),
                body: format!(
                    "Peer count: {}. Public key: {}. Supernode ready: {}.",
                    mesh.peer_count, mesh.public_key, mesh.supernode_ready
                ),
                timestamp: "now".to_string(),
                emphasis: "system".to_string(),
            },
            Message {
                id: "m-live-3".to_string(),
                room_id: "lobby".to_string(),
                author: "System".to_string(),
                body: "Desktop shell is now reading live runtime diagnostics from libmoss. Message send/receive is the next migration slice.".to_string(),
                timestamp: "now".to_string(),
                emphasis: "system".to_string(),
            },
        ];

        (
            RuntimeStatus {
                state: "Runtime online".to_string(),
                summary: format!(
                    "Live Moss runtime is active on listen port {} with {} connected peers.",
                    mesh.listen_port, mesh.peer_count
                ),
                route: if mesh.peer_count > 0 {
                    format!("Active mesh with {}", mesh.peers.join(", "))
                } else {
                    "Waiting for peer connections".to_string()
                },
                nat_hint: mesh.nat_type,
                shared_bridge: format!("Loaded from {}", self.library_path()),
            },
            rooms,
            messages,
            peers,
            "Live bridge".to_string(),
        )
    }

    fn offline_shell(
        &self,
    ) -> (RuntimeStatus, Vec<RoomSummary>, Vec<Message>, Vec<PeerSummary>, String) {
        let shared_bridge = match self.library.as_ref() {
            Some(_) => format!("Loaded from {}", self.library_path()),
            None => self.library_status(),
        };
        (
            RuntimeStatus {
                state: "Runtime offline".to_string(),
                summary: "The desktop backend can already load libmoss, but the node is not started. Use the runtime action to start a live handle.".to_string(),
                route: "No active transport".to_string(),
                nat_hint: "Unknown until runtime starts".to_string(),
                shared_bridge,
            },
            base_rooms(),
            vec![
                Message {
                    id: "m-offline-1".to_string(),
                    room_id: "system".to_string(),
                    author: "System".to_string(),
                    body: "The desktop shell is ready to start a real Moss node via the shared library.".to_string(),
                    timestamp: "now".to_string(),
                    emphasis: "system".to_string(),
                },
                Message {
                    id: "m-offline-2".to_string(),
                    room_id: "lobby".to_string(),
                    author: "Andrii".to_string(),
                    body: "Rooms, peers, and runtime state are now driven by the desktop backend instead of a static landing page.".to_string(),
                    timestamp: "now".to_string(),
                    emphasis: "normal".to_string(),
                },
            ],
            base_peers(),
            "Bridge ready".to_string(),
        )
    }

    fn failed_shell(
        &self,
        error: String,
    ) -> (RuntimeStatus, Vec<RoomSummary>, Vec<Message>, Vec<PeerSummary>, String) {
        (
            RuntimeStatus {
                state: "Runtime unavailable".to_string(),
                summary: "Desktop backend could not load or query libmoss. Place the shared library next to the executable or set MOSS_SHARED_PATH.".to_string(),
                route: "No active transport".to_string(),
                nat_hint: "Unknown".to_string(),
                shared_bridge: error.clone(),
            },
            base_rooms(),
            vec![Message {
                id: "m-failed-1".to_string(),
                room_id: "system".to_string(),
                author: "System".to_string(),
                body: error,
                timestamp: "now".to_string(),
                emphasis: "system".to_string(),
            }],
            base_peers(),
            "Bridge unavailable".to_string(),
        )
    }

    fn live_mesh_info(&mut self) -> Result<Option<MeshInfo>, String> {
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

    fn library_status(&self) -> String {
        self.library_error
            .clone()
            .unwrap_or_else(|| "shared library not loaded".to_string())
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
            let _ = library.stop(handle);
        }
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
            id: channel.clone(),
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
        rooms: mesh.channels.iter().map(|channel| format!("#{channel}")).collect(),
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
            rooms: mesh.channels.iter().map(|channel| format!("#{channel}")).collect(),
        });
    }
    peers
}

fn artifact_rows() -> Vec<Artifact> {
    vec![
        Artifact {
            name: "moss-chat-tauri-dev-linux-amd64".to_string(),
            platform: "Linux x86_64".to_string(),
            notes: "Desktop binary uploaded by the dev-only Tauri workflow.".to_string(),
        },
        Artifact {
            name: "moss-chat-tauri-dev-windows-amd64".to_string(),
            platform: "Windows x86_64".to_string(),
            notes: "Desktop binary paired with the shared Moss runtime for dev testing."
                .to_string(),
        },
        Artifact {
            name: "moss-chat-tauri-dev-macos-amd64".to_string(),
            platform: "macOS Intel".to_string(),
            notes: "Dedicated Intel artifact from the dev branch.".to_string(),
        },
        Artifact {
            name: "moss-chat-tauri-dev-macos-arm64".to_string(),
            platform: "macOS Apple Silicon".to_string(),
            notes: "Dedicated Apple Silicon artifact from the dev branch.".to_string(),
        },
    ]
}

fn milestone_rows() -> Vec<Milestone> {
    vec![
        Milestone {
            title: "Desktop shell scaffold".to_string(),
            detail: "Separate Tauri project, frontend build, Rust entrypoint, and branch-specific workflows are ready.".to_string(),
            status: "ready".to_string(),
        },
        Milestone {
            title: "Live runtime lifecycle".to_string(),
            detail: "Start, stop, subscribe, and diagnostics now come from libmoss instead of a fake backend state.".to_string(),
            status: "ready".to_string(),
        },
        Milestone {
            title: "Message and event bridge".to_string(),
            detail: "The next slice is wiring message callbacks and event callbacks into desktop subscriptions and live room updates.".to_string(),
            status: "next".to_string(),
        },
    ]
}

pub type SharedDesktopState = Mutex<DesktopShellState>;
