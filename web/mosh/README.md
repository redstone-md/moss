# mosh-web — Moss in the browser

A full Moss peer compiled to WebAssembly, running entirely in the browser over
WebRTC. Messages are end-to-end encrypted with Noise and routed through the
gossip pub/sub mesh — the same runtime as native Moss, no server in the data
path.

## Architecture

```
 browser tab A                         browser tab B
 ┌───────────────────────┐            ┌───────────────────────┐
 │ chat.js (UI)          │            │ chat.js (UI)          │
 │ moss-rtc.js ──────────┼─ WebRTC ───┼────────── moss-rtc.js │   <- DataChannel
 │   RTCPeerConnection   │  DataChan  │   RTCPeerConnection    │      (Noise inside)
 │ moss-node.wasm        │            │ moss-node.wasm         │
 │   Noise + gossip      │            │   Noise + gossip       │
 └──────────┬────────────┘            └───────────┬───────────┘
            └──────── ws ──── moss-signal ──── ws ─┘   <- SDP/ICE rendezvous only
```

- **JavaScript** owns RTCPeerConnection, ICE (NAT traversal), and signaling.
- **wasm** owns the encrypted transport, gossip routing, and telemetry.
- **moss-signal** only relays SDP/ICE between peers; it never sees mesh traffic,
  which is Noise-encrypted end to end. Anyone can run one; peers can use several.

## Build & run

```bash
# 1. Build the browser node wasm + stage wasm_exec.js into this folder
make mosh-web

# 2. Run a signaling server (rendezvous only)
go run ./cmd/moss-signal -addr 127.0.0.1:8788

# 3. Serve this folder over HTTP (wasm needs a real origin)
cd web/mosh && python -m http.server 8090
# open http://localhost:8090 in two tabs/devices, click "join", chat
```

## Status & limitations

- This is the runtime mosh-web builds on; live behavior must be validated in a
  real browser (WebRTC/ICE cannot be unit-tested headlessly here).
- Browser peers do **not** use trackers/DHT/NAT-hole-punching (no raw sockets);
  discovery is via the signaling room and NAT traversal via WebRTC ICE.
- To bridge the browser sub-mesh to native Moss peers, run a dual-stack bridge
  node (a native node that also speaks WebRTC) — a future addition.
- `moss-node.wasm` is a build artifact (git-ignored); run `make mosh-web`.
