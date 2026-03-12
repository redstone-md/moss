# Moss Chat Tauri

`apps/moss-chat-tauri` is the separate desktop shell for the `dev` branch migration away from the legacy terminal chat.

## Purpose

- keep desktop work isolated from `cmd/moss-chat`
- validate the future desktop architecture in CI before promoting it to `main`
- build distinct `linux`, `windows`, `macos-amd64`, and `macos-arm64` artifacts

## Local frontend build

```bash
npm install
npm run build
```

## Local desktop run

The app uses Tauri v2. You need Node.js and a configured Rust toolchain.

```bash
npm install
npm run tauri:dev
```

## Desktop contract

Current scope:

- React + Vite frontend
- TanStack Query app bootstrap state
- Zod validation for the invoke payload coming from the desktop backend
- Rust/Tauri backend stub with a typed `bootstrap_snapshot` command

Next scope:

- bind the Moss shared runtime into the desktop backend
- replace the legacy TUI lifecycle with desktop-native rooms, peers, transfers, and diagnostics
