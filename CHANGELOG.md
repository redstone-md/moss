# Changelog

All notable changes to this project are documented here. Format loosely
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); the project
uses semantic versioning.

## [0.7.1] - 2026-07-16

### Fixed
- **A node could never classify its own NAT, so it kept punching at peers it
  could not reach.** Fleet telemetry showed every event carrying
  `observations=1` — never two — and ~89% of connect attempts failing with
  `no_relay_peer` at ~10s each. One cause, two leaks:
  - The gossip binding reply answers *"what is my dialable address"* with the
    observed host plus **the port the asker advertised about itself**. Right for
    advertising, worthless as evidence about a NAT: every peer hands back our
    own port, so the value is a constant. It reached the classifier anyway, and
    `appendObservation` collapsed each identical echo into one entry — leaving
    nothing to compare. Genuine observations (STUN, a port mapper, a peer's UDP
    observe, which does report what it truly saw) now inform classification; the
    echo only updates the address we advertise.
  - STUN returned on the first server that answered, so only one vantage point
    was ever sampled. Symmetric NAT is *defined* by the mapped port differing per
    destination — one sample has nothing to differ from. Two distinct servers are
    now compared within the round, deliberately not through the long-lived
    history, which would fold a cone NAT's two identical mappings back into one.

  Downstream this is what matters: `shouldPreferRelayBetween` suppresses a hole
  punch only when **both** ends are known symmetric, so an unclassifiable fleet
  never used it.

  Not fixed by simply reporting the observed port: over a TCP session that is an
  ephemeral source port, unrelated to the UDP mapping a punch needs. Feeding
  those in would make ports differ every time and classify everyone symmetric —
  trading "never detects" for "always wrong".
- **Relay selection guessed instead of asking.** `OpenRelaySessionAny` walked our
  own neighbours hoping one happened to also be connected to the target, ordered
  by a guess at geographic closeness — but a relay must be adjacent to **both**
  ends, and nothing ever said which node that is. `dialKnownPeer` now consults
  the overlay, where every node publishes its attachments under its own id, and
  dials the node the peer actually reports being attached to.

### Known gap
- A UDP session closed by duplicate-session dedup takes the peer-observe path
  down with it (`ObserveContext` requires one), so the bootstrap's TCP+UDP race
  creates a UDP session and then discards it — the churn behind the
  zero-length sessions the fleet reports. Classification no longer depends on
  that path, so this is now waste rather than breakage; fixing it properly means
  a transport-level observation session, not a patch to the dedup.

## [0.7.0] - 2026-07-16

### Added
- **Overlay: a Kademlia discovery layer, so sparse topics find each other.**
  The shared substrate gave moss one network that can grow, but the layers above
  it still assumed the small isolated mesh it replaced. Two of those assumptions
  are now false: subscription state travels exactly one hop, and a starved topic
  falls back to grafting whatever is connected. On a five-node network every
  peer was a topic-mate, so both held; on a shared substrate the peers around
  you are strangers who prune the graft, and the one node that shares your
  channel is somewhere you have no link to. A two-player topic therefore never
  formed a mesh at all — `Publish` returned `NO_PEERS` and the announcement went
  nowhere, which is what made game lobbies invisible to each other.

  A node now publishes, under the hash of each opaque room topic it subscribes
  to, a record naming the core nodes it is attached to; a starved topic resolves
  its mates through the same keyspace, dials one of their attachments, relays to
  them and grafts.

  - It is **discovery, not packet routing**. Every routing node is publicly
    reachable by definition and a NAT'd node can always dial one outbound, so
    once a lookup answers "B is attached to S" the path is A → S → B: two hops,
    always. Chained forwarding buys nothing.
  - Membership is **two-tier by physics**: a lookup cannot be delivered to a node
    nobody can dial, so only reachable nodes hold buckets and answer queries.
    NAT'd nodes are full clients of the overlay, never hops.
  - **Sized for the target, not today**: with a handful of core nodes every
    contact falls out of `Closest`, which is exactly a full mesh — the same code
    serves six nodes and six million, with no second flag day.
  - Keys derive from the **opaque room topic**, never the bare channel, so a core
    node holding a record still cannot tell which room or game it belongs to.
- **Per-attempt connectivity telemetry** — `nat_attempt` (how an attempt to reach
  a peer ended, why it fell back, whether the overlay found the peer),
  `topic_rendezvous` (what a lookup resolved vs. how much became reachable) and
  `session_end` (how long a session held, and whether it died on missed pings —
  sessions dropping at a flat interval is a NAT mapping timing out, and that
  should be a query rather than a night of reading logs). Every event carries
  `nat_type`, `mapped_port`, `ports_differed`, `observations` and `family`, and
  **no address**: the diagnostic value of an observed endpoint is entirely in the
  port's behaviour, and the IP would only make the dataset a map of who plays
  what from where. `ports_differed` is withheld on a single observation — one
  vantage point cannot tell symmetric from cone.
- `Node.AxiomEnabled` and `axiom_shipping` in `MeshInfoJSON`, so "configured" and
  "actually reporting" are distinguishable.

### Fixed
- **The FFI dropped the Axiom config, so no client has ever reported.** The sink
  is enabled inside the public `moss.NewNode`, but the FFI builds a node straight
  from `mesh.NewNodeWithIdentity`, and `mesh.ParseConfig` has no Axiom fields —
  so `axiom_token` and friends were parsed into nothing and discarded silently.
  Both desktop clients set that config and assume it works (gse says so in a
  comment); neither ever shipped an event, while the Go-native spores reported
  fine. The FFI now honours the same keys and enables the sink before the caller
  can `Start`, so even a first-start bind failure — the Wine/Proton case the sink
  exists for — is reported instead of dying with the node.

## [0.6.24] - 2026-07-17

### Fixed
- **Revert v0.6.22 + v0.6.23 — they broke NAT hole-punch, so two NAT'd clients
  could no longer see each other (gse_moss lobbies invisible).** Both changes
  removed the UDP session a node keeps with its bootstrap peers: v0.6.22's dedup
  closed it in favour of TCP, and v0.6.23 stopped opening it at all (TCP-first,
  no race). That session is load-bearing, not a redundant race:
  `ObserveContext` refuses without one (`errUDPObserveRequiresSession`), so
  killing it removes the UDP binding observations that feed `bindingHistory` →
  NAT classification stays `unknown` and `attemptHolePunch` cannot predict the
  peer's ports. A node with only a TCP session also has its *TCP* source port
  observed and gossiped as its endpoint, which is useless for UDP punching. Net
  effect: no direct P2P between NAT'd peers. This matters at any topology — a
  supernode-less mesh depends on hole-punch even more, having no relay fallback.
  Both commits chased a cosmetic per-peer reconnect churn that never actually
  cost connectivity (the affected node held 4-7 direct peers throughout), and
  traded real P2P for it. Bootstrap races TCP and UDP again, and duplicate-session
  dedup is back to the direction rule.

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
