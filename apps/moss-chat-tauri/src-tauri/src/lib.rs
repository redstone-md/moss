use serde::Serialize;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Artifact {
    name: &'static str,
    platform: &'static str,
    notes: &'static str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Milestone {
    title: &'static str,
    detail: &'static str,
    status: &'static str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct BootstrapSnapshot {
    app_name: &'static str,
    version: &'static str,
    branch: &'static str,
    stage: &'static str,
    summary: &'static str,
    shared_strategy: &'static str,
    artifacts: Vec<Artifact>,
    milestones: Vec<Milestone>,
}

#[tauri::command]
fn bootstrap_snapshot() -> BootstrapSnapshot {
    BootstrapSnapshot {
        app_name: "Moss Chat Dev",
        version: env!("CARGO_PKG_VERSION"),
        branch: "dev",
        stage: "Scaffold ready",
        summary: "Separate Tauri desktop shell prepared for the dev branch. The next milestone is wiring the shared Moss runtime into desktop commands and replacing the legacy TUI incrementally.",
        shared_strategy: "The desktop shell stays separate from cmd/moss-chat and is intended to call the Moss shared runtime through a thin desktop bridge.",
        artifacts: vec![
            Artifact {
                name: "moss-chat-tauri-dev-linux-amd64",
                platform: "Linux x86_64",
                notes: "Uploaded by the dev-only Tauri workflow as an unsigned desktop binary.",
            },
            Artifact {
                name: "moss-chat-tauri-dev-windows-amd64",
                platform: "Windows x86_64",
                notes: "Unsigned desktop binary for internal testing on the dev branch.",
            },
            Artifact {
                name: "moss-chat-tauri-dev-macos-amd64",
                platform: "macOS Intel",
                notes: "Dedicated Intel artifact built from macOS Intel runners.",
            },
            Artifact {
                name: "moss-chat-tauri-dev-macos-arm64",
                platform: "macOS Apple Silicon",
                notes: "Dedicated Apple Silicon artifact built from arm64-capable macOS runners.",
            },
        ],
        milestones: vec![
            Milestone {
                title: "Desktop shell scaffold",
                detail: "Separate Tauri project, frontend build, Rust entrypoint, and branch-specific workflows are ready.",
                status: "ready",
            },
            Milestone {
                title: "Shared runtime bridge",
                detail: "Bind the Moss shared library into desktop commands and move core lifecycle actions out of the legacy TUI path.",
                status: "next",
            },
            Milestone {
                title: "Chat feature migration",
                detail: "Replace the terminal-only chat UX with desktop rooms, peers, transfers, and diagnostics without regressing runtime stability.",
                status: "next",
            },
        ],
    }
}

pub fn run() {
    tauri::Builder::default()
        .invoke_handler(tauri::generate_handler![bootstrap_snapshot])
        .run(tauri::generate_context!())
        .expect("failed to run moss chat dev app");
}
