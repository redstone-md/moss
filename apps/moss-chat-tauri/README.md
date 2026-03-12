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

The desktop backend now loads the Moss shared runtime dynamically.

- set `MOSS_SHARED_PATH` to an explicit `moss.dll`, `libmoss.so`, or `libmoss.dylib`
- or place the shared library next to the desktop executable
- the dev tag workflow (`dev-*`) publishes the desktop binary and the matching shared library together

## Desktop contract

Current scope:

- React + Vite frontend
- TanStack Query desktop shell state
- Zod validation for the invoke payload coming from the desktop backend
- Rust/Tauri backend with live `libmoss` lifecycle and diagnostics bridge

Next scope:

- bind message callbacks and event callbacks into live room updates
- replace the legacy TUI feature set with desktop-native rooms, peers, transfers, and diagnostics
