use std::sync::Mutex;

use crate::models::{
    Artifact, DesktopSnapshot, Message, Milestone, PeerSummary, RoomSummary, RuntimeStatus,
};

pub struct DesktopShellState {
    runtime_online: bool,
    stage: String,
}

impl DesktopShellState {
    pub fn new() -> Self {
        Self {
            runtime_online: false,
            stage: "Interactive shell".to_string(),
        }
    }

    pub fn snapshot(&self) -> DesktopSnapshot {
        let runtime = if self.runtime_online {
            RuntimeStatus {
                state: "Runtime online".to_string(),
                summary: "Desktop shell is ready to own node lifecycle, subscriptions, and message routing once the shared bridge is wired.".to_string(),
                route: "Direct + relay fallback planned".to_string(),
                nat_hint: "Read from Moss runtime once FFI bridge is attached".to_string(),
                shared_bridge: "Pending libmoss bridge".to_string(),
            }
        } else {
            RuntimeStatus {
                state: "Runtime offline".to_string(),
                summary: "This dev shell is running without a live Moss node yet. The next iteration will bind start, stop, subscribe, publish, and diagnostics to the shared runtime.".to_string(),
                route: "No active transport".to_string(),
                nat_hint: "Unknown until runtime starts".to_string(),
                shared_bridge: "Stub backend only".to_string(),
            }
        };

        DesktopSnapshot {
            app_name: "Moss Chat Dev".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            branch: "dev".to_string(),
            stage: self.stage.clone(),
            runtime,
            rooms: vec![
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
                    participants: 3,
                    kind: "channel".to_string(),
                },
                RoomSummary {
                    id: "release-war-room".to_string(),
                    label: "#release-war-room".to_string(),
                    unread: 2,
                    participants: 4,
                    kind: "channel".to_string(),
                },
                RoomSummary {
                    id: "dm-nevermore".to_string(),
                    label: "@nevermore".to_string(),
                    unread: 0,
                    participants: 2,
                    kind: "dm".to_string(),
                },
            ],
            messages: vec![
                Message {
                    id: "m1".to_string(),
                    room_id: "lobby".to_string(),
                    author: "Andrii".to_string(),
                    body: "Tauri shell is split from the terminal chat. Next step is wiring libmoss into desktop commands.".to_string(),
                    timestamp: "09:12".to_string(),
                    emphasis: "normal".to_string(),
                },
                Message {
                    id: "m2".to_string(),
                    room_id: "lobby".to_string(),
                    author: "System".to_string(),
                    body: "Use the runtime action to flip the backend state while the shared bridge is still a stub.".to_string(),
                    timestamp: "09:13".to_string(),
                    emphasis: "system".to_string(),
                },
                Message {
                    id: "m3".to_string(),
                    room_id: "release-war-room".to_string(),
                    author: "nevermore".to_string(),
                    body: "Desktop migration should keep CI green on dev while main stays on the stable terminal client.".to_string(),
                    timestamp: "09:14".to_string(),
                    emphasis: "normal".to_string(),
                },
                Message {
                    id: "m4".to_string(),
                    room_id: "system".to_string(),
                    author: "System".to_string(),
                    body: if self.runtime_online {
                        "Runtime toggled to online state.".to_string()
                    } else {
                        "Runtime is still offline. Bridge integration is the next milestone.".to_string()
                    },
                    timestamp: "09:15".to_string(),
                    emphasis: "system".to_string(),
                },
                Message {
                    id: "m5".to_string(),
                    room_id: "dm-nevermore".to_string(),
                    author: "nevermore".to_string(),
                    body: "When the bridge lands, DM, attachments, and diagnostics can move off the terminal path incrementally.".to_string(),
                    timestamp: "09:16".to_string(),
                    emphasis: "normal".to_string(),
                },
            ],
            peers: vec![
                PeerSummary {
                    id: "peer-andrii".to_string(),
                    display_name: "Andrii".to_string(),
                    route: if self.runtime_online {
                        "local desktop owner".to_string()
                    } else {
                        "not connected".to_string()
                    },
                    latency: "--".to_string(),
                    status: "self".to_string(),
                    rooms: vec!["#lobby".to_string(), "#release-war-room".to_string()],
                },
                PeerSummary {
                    id: "peer-nevermore".to_string(),
                    display_name: "nevermore".to_string(),
                    route: "planned relay/direct peer".to_string(),
                    latency: "42 ms".to_string(),
                    status: "expected peer".to_string(),
                    rooms: vec!["#lobby".to_string(), "@nevermore".to_string()],
                },
                PeerSummary {
                    id: "peer-supernode".to_string(),
                    display_name: "public supernode".to_string(),
                    route: "bootstrap + relay candidate".to_string(),
                    latency: "31 ms".to_string(),
                    status: "infrastructure".to_string(),
                    rooms: vec!["#release-war-room".to_string()],
                },
            ],
            artifacts: vec![
                Artifact {
                    name: "moss-chat-tauri-dev-linux-amd64".to_string(),
                    platform: "Linux x86_64".to_string(),
                    notes: "Desktop binary uploaded by the dev-only Tauri workflow.".to_string(),
                },
                Artifact {
                    name: "moss-chat-tauri-dev-windows-amd64".to_string(),
                    platform: "Windows x86_64".to_string(),
                    notes: "Primary local developer artifact for the desktop migration.".to_string(),
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
            ],
            milestones: vec![
                Milestone {
                    title: "Desktop shell scaffold".to_string(),
                    detail: "Separate Tauri project, frontend build, Rust entrypoint, and branch-specific workflows are ready.".to_string(),
                    status: "ready".to_string(),
                },
                Milestone {
                    title: "Shared runtime bridge".to_string(),
                    detail: "Bind the Moss shared library into desktop commands and move lifecycle actions out of the terminal client.".to_string(),
                    status: "next".to_string(),
                },
                Milestone {
                    title: "Feature migration".to_string(),
                    detail: "Bring rooms, peers, transfers, and diagnostics to the desktop UI without regressing runtime stability.".to_string(),
                    status: "next".to_string(),
                },
            ],
        }
    }

    pub fn toggle_runtime(&mut self) -> DesktopSnapshot {
        self.runtime_online = !self.runtime_online;
        self.snapshot()
    }
}

pub type SharedDesktopState = Mutex<DesktopShellState>;
