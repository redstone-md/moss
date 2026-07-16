# Changelog

All notable changes to this project are documented here. Format loosely
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); the project
uses semantic versioning.

## [0.6.23] - 2026-07-16

### Fixed
- **Symmetric-NAT flap, the real fix — bootstrap no longer races UDP against
  TCP to a public seed.** v0.6.22 made a node *keep* its TCP session, but the
  bootstrap still *opened* a parallel UDP session to every public supernode,
  and the supernode (dialed side) independently latched onto that UDP session —
  whose return path a symmetric-NAT dialer strands — so its pings missed and it
  pruned the peer every ~38s regardless of what the dialer preferred. Bootstrap
  now connects **TCP-first and only falls back to UDP when TCP genuinely fails**
  (e.g. TCP blocked on the path), instead of racing both. A symmetric-NAT node
  (e.g. a Flux container) forms a single stable TCP session to each supernode —
  the fix works with only the dialer upgraded; supernodes need no change. The
  v0.6.22 transport-preference dedup stays as defense in depth.

## [0.6.22] - 2026-07-16

### Fixed
- **Symmetric-NAT peers flapped off every ~38s.** The bootstrap path races a
  TCP and a UDP connection to a public supernode, and the duplicate-session
  dedup in `registerPeer` picked the winner purely by direction and ping
  health — **ignoring transport type**. So a UDP session would silently
  replace the healthy TCP one; but a datagram session writes to a fixed remote
  address whose return path is dead once a symmetric-NAT peer's mapping
  differs, so the supernode's pings all missed and the peer was pruned after
  the 30s retain grace (~38s PeerJoined→PeerLeft cycle, endlessly). Dedup now
  prefers the reliable transport: a healthy TCP session is never displaced by
  UDP, and a UDP session is upgraded to TCP when one arrives. A node behind
  symmetric NAT (e.g. a container on Flux) now holds a stable link to public
  supernodes over TCP, which is bidirectional through any NAT.

## [0.6.21] - 2026-07-16

### Added
- **Veil dialer integration — the mesh now bootstraps through the DPI-mask
  bearer, not just listens on it.** A node can list Veil-fronted relays in
  `veil.relays` (`{addr, cover_sni, pubkey}`); at startup it holds a masked
  "Reality" session open to each, redialing with capped backoff whenever one
  drops, so a client behind active DPI (e.g. RU TSPU) keeps a mesh foothold
  when its ordinary UDP/TCP paths are throttled. `pubkey` is the relay's
  X25519 Noise static key, from which the tunnel auth secret is derived — no
  secret is shared out of band.
  - Public `Config.Veil` (`VeilConfig`/`VeilRelay`) exposes the whole bearer
    (listener + dialer) to external consumers; previously only the internal
    config carried it.
  - New `Node.NoiseStaticPublicHex()` returns the key a relay operator pins in
    dialer configs — distinct from `PublicKey()` (the Ed25519 identity key).
  - Measured cost of the masked tunnel vs plain TCP over a real path: ~2%
    throughput (within noise, 92 vs 94 Mbit/s) and a one-time +20 ms connect
    for the TLS handshake. Verified end to end with trackers/DHT/LAN disabled
    so Veil was the sole bootstrap path: peer formed and gossip flowed.

## [0.6.20] - 2026-07-16

### Added
- Public `Config.DHTEnabled` (`dht_enabled`) so consumers can turn DHT off where
  it is unsafe, and a `MOSS_FORCE_RAW_UDP` env escape hatch to force the raw
  blocking-socket UDP path on Windows. Both default to current behaviour.

## [0.6.19] - 2026-07-15

### Added
- `SetMessageCallback` on the public `Node` — the Go API could set event and
  relay callbacks but had no way to receive pub/sub messages (the FFI already
  exposed it via `Moss_SetCallback`).

## [0.6.18] - 2026-07-15

### Added
- Public Axiom config (`axiom_token`/`axiom_dataset`/`axiom_endpoint`/
  `axiom_service`), wired before Start so the first bind failure is captured;
  public `LogEvent`; a periodic `node_stats` event (peer/supernode/relay counts)
  so a dashboard can chart network health, not just errors.

## [0.6.16] - [0.6.17] - 2026-07-15

### Added
- Opt-in Axiom error/log sink behind the FFI (`Moss_EnableAxiom`,
  `Moss_LogEvent`): a node ships nothing until a host enables it. moss forwards
  its own errors (listen/tracker/handshake/relay), attaching an anonymous node
  id and os/arch. One implementation in `moss.dll` serves every consumer.

### Changed
- Telemetry (the DP-noised, k-anonymous network layer) is now **on by default**;
  opt out with `telemetry_enabled=false`.

## [0.6.14] - [0.6.15] - 2026-07-15

### Added
- **Wine/Proton UDP fallback:** when the Go netpoller cannot bind a socket
  (older Wine/Proton, where IOCP association fails), moss falls back to a raw
  blocking Winsock socket; `ListenPair` binds UDP first and treats TCP as
  best-effort, coming up UDP-only when TCP cannot bind. A failed `Start` now
  returns a dedicated `MOSS_ERR_LISTEN_FAILED` and records the real OS error,
  exposed via the new `Moss_LastError` export.
- Explicit peer targets that bypass glare/dial-ranking, a `Moss_ConnectToPeer`
  export, and neutral dial ranking (v0.6.15).

## [0.6.13] - 2026-07-15

### Fixed
- **Relay-route leak:** a supernode's forwarding table was only pruned on peer
  disconnect or explicit teardown, so relayed pairs that vanished left routes
  behind forever (hundreds against a single live session). Route liveness is now
  tied to the session — refreshed by traffic, reaped when idle past the TTL.

## [0.6.12] - 2026-07-15

### Fixed
- **Connection glare between supernodes:** two publicly-dialable nodes each
  dialed the other, producing duplicate sessions that oscillated ~1–2×/s under
  latency. Dial initiation now follows the id ordering the dedup already uses —
  only the lower-id node dials when both ends are public.

## [0.6.11] - 2026-07-15

### Added
- `peer_details` in `MeshInfoJSON` (`{id, addr, relayed}` per connected peer), so
  a caller can check whether one specific counterpart is present rather than
  trusting a bare peer count (needed on the shared substrate).

## [0.6.10] - 2026-07-15

### Fixed
- **Client flapping:** peer/connection maintenance (score decay, prune,
  reconnect) was driven by the gossip heartbeat, so a fast heartbeat (e.g. a
  chat client's 250 ms) aged peers ~4× too fast and continuously flapped healthy
  connections. Maintenance now runs at ~1 s regardless of the heartbeat.

## [0.4.0] - 2026-06-30

### BREAKING

- **Wire-format flag-day:** all UDP datagrams are now obfuscated by a keyed
  scramble codec (ChaCha20-Poly1305; key derived from mesh ID + PSK). The
  wire format is **not** backward-compatible. An obfuscated node and a
  plaintext node cannot complete a handshake. Clients and relays must upgrade
  together.

### Added

- BitTorrent mainline DHT discovery source: nodes periodically re-announce on
  a dedicated UDP socket, giving tracker-independent peer discovery.
- Persistent peer cache (`peers.json` beside the identity file) for warm
  reconnect across restarts.
- Tracker `AnnounceAll` now returns as soon as any tracker responds with
  peers (grace window), instead of blocking on dead trackers.
- New config fields: `obfs_pad_max`, `dht_enabled`, `dht_port`,
  `peer_cache_max`, `peer_cache_ttl_sec`, `peer_cache_path`.
- Privacy-preserving, decentralized network telemetry (opt-in, off by default).
  Nodes gossip a per-epoch CRDT — HyperLogLog node count, DP-noised bandwidth,
  NAT/degree histograms — under an unlinkable per-epoch ephemeral id, producing a
  self-verifying, BLAKE2s hash-chained snapshot readable via the new
  `Moss_GetNetworkStats` FFI export. No collector, no trusted signer; integrity
  comes from reproducibility. New `telemetry` config block.
- `internal/observe` + `cmd/moss-wasm`: pure, wasm-safe client-side verification
  (hash-chain continuity, multi-gateway cross-check) and a deterministic topology
  *simulation* seeded by the epoch digest.
- `cmd/moss-gateway`: read-only HTTP/SSE server exposing the aggregate snapshot
  and digest chain for browser explorers (CORS, no addresses/identities).
- `explorer/` (Mossscan): self-hostable web app that verifies telemetry in the
  browser and renders node count, bandwidth, histograms, and the simulated tree.
- Browser runtime: `cmd/moss-node-wasm` runs a full Moss peer in the browser over
  WebRTC (Noise + gossip over a DataChannel via `transport.WebRTCConn` and
  `Node.StartWebRTC`/`AttachDataChannel`); `cmd/moss-signal` is a minimal SDP/ICE
  rendezvous relay; `web/mosh/` is a chat demo. WebRTC/ICE require a real browser
  to validate end to end.

### Changed

- Default tracker list refreshed: removed unreachable / deprecated hosts
  (`open.stealth.si`, `tracker1.bt.moack.co.kr`, HTTP opentrackr mirror);
  added `open.demonii.com`, `tracker.torrent.eu.org`, `open.tracker.cl`,
  `tracker.openbittorrent.com` HTTP mirror.

### Fixed

- Direct connectivity regression from the NAT reachability hardening: a node
  behind ordinary NAT with a stable port-forward (RFC1918 local + public
  reflexive address) was guessed to be CGNAT, which made peers skip hole-punch
  and wait for a relay that may not exist — breaking previously-working direct
  connections. The classifier no longer guesses CGNAT from address shape (it is
  indistinguishable from a port-forwarded host); genuine carrier NAT is still
  caught observationally (varying mapped ports → symmetric; RFC6598 local →
  CGNAT via Detect). Reachability still requires a real inbound probe, so the
  CGNAT-supernode fix below is preserved.
- CGNAT nodes were misclassified as `public` and promoted to supernodes. The
  v0.3.1 fix closed the `applyExternalObservation` shape path but two holes
  remained: `WithBindingObservations` still upgraded an `Unknown` profile to
  `public` + `PublicReachable` from a public *reflexive* address alone, and the
  hole-punch / `freshObservedUDPAddr` paths reach it without any inbound probe.
  A node behind carrier NAT (e.g. local `10.x`, WAN `188.x`) thus self-reported
  "open" and was made a relay it could not serve. `WithBindingObservations` no
  longer infers reachability from address shape; reachability comes only from a
  successful inbound probe, and a public reflexive address with a private local
  interface and no confirmed inbound reach is now labelled `cgnat`.

### Dependencies

- Adds `github.com/anacrolix/dht/v2 v2.24.0`. Its transitive
  `github.com/anacrolix/torrent` pin is a retracted pseudo-version; only the
  leaf packages `bencode`, `iplist`, `metainfo`, and `types/infohash` are
  used (not the retracted torrent storage code), so the build is safe.
  Re-tidy when `dht/v2` publishes a release off a non-retracted torrent base.

## [0.3.1] - 2026-06-03

### Fixed
- NAT reachability detection no longer infers public reachability from a single
  reflexive address. The v0.3.0 fast path upgraded any node with a
  global-unicast external address to `public` + reachable and skipped the
  inbound probe — but behind NAT the reflexive address is only the NAT's WAN IP,
  so NATed nodes self-reported "open" and looped on futile direct dials (peer
  flapping, rapid `peer_joined`/`peer_left`). `applyExternalObservation` now
  keeps the tentative `public` type but leaves `PublicReachable` to the actual
  inbound probe, and lets multi-destination binding observations classify
  `symmetric_nat` (regression tests added).

## [0.3.0] - 2026-06-02

### Added
- Public `moss` API package wrapping `internal/mesh` for external consumers (8c7fac4).
- Relayed pub/sub transport: gossip is tunnelled over relay sessions (f62a3f1).
- Opt-in binding to a physical NIC via `IP_UNICAST_IF` / `SO_BINDTODEVICE` / `IP_BOUND_IF` (8847514).
- High-throughput transport tuning (ec912d3).

### Changed
- NAT reachability hardening on bare metal and public hosts:
  - Preserve `TypeUnknown` in `WithExternalAddress` to avoid false `port_restricted_cone` (39e6824).
  - Fast-path upgrade `TypeUnknown` → `TypePublic` for global-unicast external addresses with consistent binding ports (f10b1de, 527d1d0).
  - Skip redundant reachability confirmation when the fast path already confirmed public reachability (2767f3a, 7e0507a).
  - Route the second STUN fallback through `applyExternalObservation` for correct binding history (bda5bf6).

### Fixed
- Relay session teardown + bandwidth-bucket race that intermittently dropped the first relayed payload (096b847).
- Context timer leak in the handshake/announce paths (`go vet` lostcancel) (59f2d4a).
- Bind HTTP tracker traffic to the selected NIC (76e8e7c).
- Guard stream close against an enqueue race (f107d35).

### Tests / CI
- Relax relay bandwidth test timeouts for slow/Windows CI (7cfd5c1, 2459bde).
- Accept `TypeUnknown` in the conservative reachability test (3fa4ba6).

[0.3.0]: https://github.com/redstone-md/moss/releases/tag/v0.3.0
