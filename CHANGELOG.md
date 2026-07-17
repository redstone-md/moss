# Changelog

All notable changes to this project are documented here. Format loosely
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); the project
uses semantic versioning.

**Retracted versions: v0.8.3, v0.8.4, v0.8.5.** They carry a session "goodbye"
that tore down live links; v0.8.4 also shipped with a failing test suite. They
are marked `retract` in go.mod, so `go get` will not select them. Use v0.8.6 or
later. Nothing is deleted: the tags stay published because builds that already
resolved them must keep resolving them.


## [0.8.11] - [0.8.14] - 2026-07-17

Release hygiene only; no code changes in any of them.

### Changed
- **v0.8.3, v0.8.4 and v0.8.5 are retracted** in go.mod, so version selection
  skips them. They carry a session goodbye that tore down live links, and v0.8.4
  also shipped with a failing test suite. The tags stay published: deleting them
  would break every build that already resolved them, and the module proxy keeps
  them regardless. A retraction only takes effect once published in a version
  above the ones it retracts, which is what v0.8.11 is for.
- The changelog now carries an entry per released version, including 0.8.0 which
  had none, and says which releases were wrong and why. Ten releases went out in
  one night and three were not fit to use; a changelog that records only the fixes
  reads as though the line went smoothly.

### Notes
- Four versions for a documentation change is three too many, and the reason is
  worth writing down: each was tagged BEFORE its own changelog entry existed, so
  every release needed another one to describe it. The entry goes in the commit
  the tag points at — that is the whole discipline, and this range is what
  skipping it costs.
- v0.8.11's tag was briefly moved to a later commit and then put back. The proxy
  serves a published version immutably, so a moved tag makes git and `go get`
  disagree about what that version is; the correction shipped as a new version
  instead.

## [0.8.10] - 2026-07-17

### Fixed
- **A peer that connects and then never speaks was redialled forever.** The dial
  succeeding is not proof of a working path. A node drowning in its own
  announcement flood accepts the connection and then drops every ping sent to it,
  so the session died at six misses ~37s later — whereupon `removePeer` cleared
  the cooldown, the redial went out at once, succeeded again, and died again, the
  backoff never engaging because as far as it knew every connect had worked.
  Players felt that loop as entering a lobby on the fourth or fifth try. A session
  that dies on missed pings is now charged as the failed path it is; a clean
  disconnect still redials immediately, and any healthy session clears the
  history. Measured: sessions opened against nodes that behave this way fell from
  74 per six minutes to 17, and kept falling.

## [0.8.9] - 2026-07-17

### Fixed
- **A node now survives a peer that floods it, rather than trusting it not to.**
  v0.8.8 stopped this node FEEDING the flood; this is the other half. Requiring
  every peer to be well-behaved — or upgraded — before anyone is safe is a hope,
  not a design, and the same hole is open to a broken client or a malicious one.
  Handling an announcement costs an Ed25519 verification and the node's central
  lock, and `readPeer` dispatches synchronously, so ~900 a second is enough to
  stop the read loop outright. Announcement traffic is now charged against a
  per-peer token budget before any work is done on it, and the surplus dropped.
  The ceiling sits orders of magnitude above what a correct peer needs — a node
  announces itself about every 10s — so it bounds damage rather than setting a
  schedule. The budget lives on the peerConn, so charging it takes no shared lock:
  it must be cheaper than what it guards.

## [0.8.8] - 2026-07-17

### Fixed
- **Nodes drowned each other in announcements, and the packets they lost were
  pings.** This is the root cause behind lobbies taking forever to appear, joining
  on the fourth attempt, and mid-game freezes and desync.

  A relay with seven peers took 21,808 supernode announcements in two minutes —
  against 29 pings — while discarding 142,125 packets in a single minute, all on
  the stream it reads itself, at 2% capacity. When a node disagreed with an
  arriving announcement it forwarded its OWN view with the signature stripped; the
  signature covers the sender's claims, not ours, so the next hop could not verify
  it, kept its own value, and corrected us straight back. Two nodes disagreed
  forever, and every correction went to every peer they had.

  `readPeer` dispatches synchronously, so the flood stalled the read and the
  256-packet stream buffer silently dropped everything behind it. That is why
  sessions died at six missed pings with the connection perfectly healthy, and why
  both ends of one such link each reported receiving two packets while both were
  writing.

  An announcement we cannot vouch for is no longer re-told; a signed one is still
  relayed verbatim. Measured: `stream_drops` went from 1,937,107 climbing at
  ~200k/min to 0. Links where both ends run this build show zero six-miss deaths
  and a median session life of 632s, against 37s and 154 deaths in 221 sessions to
  nodes without it.

## [0.8.7] - 2026-07-17

### Fixed
- Announcement re-flooding is capped at one message per advertised peer per 10s.
  Aimed at the flood and did not stop it: the storm was inbound from nodes without
  the change, and capping what we forward cannot help a node already drowning.
  v0.8.8 fixes the cause; this stays as a backstop.

## [0.8.6] - 2026-07-17

### Reverted
- **The session goodbye from v0.8.3.** It tore down live links: a test that ran
  5/5 at a steady 16.17s went 0.2s / 7.3s / hang with it in. Disabling only the
  dedup-path farewells stopped the hangs; disabling all of them restored the
  16.17s exactly. It was also aimed at the wrong target — nothing was closing
  those sessions, their packets were being thrown away. A datagram carrier still
  has no teardown signal and that remains worth fixing, but not before the flood
  was.

### Added
- `in_<envelope_type>` counters. The drop counters proved a storm; these name what
  it is made of, which is what the fix depends on.

## [0.8.5] - 2026-07-17 — RETRACTED

Carries the v0.8.3 regression. Use v0.8.6 or later.

### Added
- `stream_drops_default` / `stream_drops_other`. Drops on the default stream mean
  the reader cannot keep up; drops elsewhere would mean packets arriving for a
  stream nothing ever reads. The totals cannot tell those apart, and the two need
  opposite fixes. (Answer: every one of them was on the default stream.)

## [0.8.4] - 2026-07-17 — RETRACTED

Carries the v0.8.3 regression, and was released with a failing test suite: the
release command chained on a pipeline whose exit status came from `tail` rather
than from `go test`. Use v0.8.6 or later.

### Added
- **`stream_drops`: the packets moss throws away.** A full stream buffer discards
  the packet and says nothing — the sender's `WritePacket` returns nil, the
  carrier delivered it, the connection stays up, and the packet ceases to exist.
  An overflow hook was already in the tree to notice this and nothing had ever
  installed it, so these drops had never once been counted. This is the counter
  that found the flood.

## [0.8.3] - 2026-07-17 — RETRACTED

Tore down live links. Use v0.8.6 or later.

### Added
- `peer_capacity_pct` / `relay_capacity_pct`, with their denominators. Counts
  alone cannot say whether the network has room: 8 peers is half-idle at
  MaxPeers=16 and wedged at MaxPeers=8. A node at 90% warns — it evicts an
  existing peer for every new one, so the mesh around it churns instead of
  growing. (First reading: 2%. The bottleneck was never capacity.)

### Fixed
- Config accessors took a value receiver, copying the whole struct and so reading
  every field in it: `AnnounceInterval()` was landing on `MaxPeers` and racing
  anything that touched it.

## [0.8.2] - 2026-07-17

### Fixed
- **The overlay never asked the nodes holding the record.** Every rendezvous the
  fleet ran — 205 of 205 — returned `found=0` while the record sat on a core node
  the whole time. Three faults compounded. The batch took the alpha nearest
  contacts without regard to sessions — the overlay speaks only over established
  ones, and XOR distance has nothing to do with who we can reach — so unreachable
  contacts consumed every slot. The round then gave up as soon as it learned
  nothing new, which is not Kademlia's termination rule: a lookup ends when the
  closest have all been ASKED, and instead a node queried three contacts of six,
  learned nothing new, and reported the channel empty without asking the three
  that had it. And alpha was queried sequentially, turning a 4s timeout into
  alpha x 4s per round — the measured 12s p95 and 20s worst case.

### Added
- `overlay_publish` reports how many nodes accepted a record. A lookup cannot tell
  "nobody is in this room" from "nothing was ever stored", which is how a dead
  layer stayed invisible.
- `session_end` names the far end, so both halves of one link can be joined. From
  inside one node "zero packets arrived" means both "they never sent" and "their
  packets never landed"; across both ends it does not.

## [0.8.1] - 2026-07-17

### Fixed
- **Failing dials were never spaced out.** `peerDials` was an in-flight marker
  wearing a cooldown's name — deleted the instant an attempt returned — so the
  cooldown check could never fire for a peer that failed, and the next maintenance
  tick redialled it ~1s later, forever, at a full HandshakeTimeout per attempt.
  The fleet spent 723 attempts x ~9.8s that way against peers it never once
  reached: 81% of every connect attempt made, against 165 that succeeded. The
  interval now doubles with consecutive failures, timed from when the attempt
  ENDED, and any success — direct or relayed — resets it. Trying direct first,
  always, is unchanged: an unreachable peer is still retried, capped at 5 minutes,
  because no path now is not no path ever.
- `Session.RemoteAddr()` is nil-safe. Reading through it took the node down on an
  ordinary disconnect.

## [0.8.0] - 2026-07-17

### Fixed
- **Both ends of a duplicate session could keep different halves of it.** A
  bootstrap race leaves each side holding two sessions in the SAME direction, so
  the direction rule could not separate them and each side silently kept whichever
  handshake finished first locally. When those choices diverged, the loser's half
  became a ghost: the far side wrote into a socket nobody read and dropped the
  peer six unanswered pings later. Duplicates are now decided by transport, the
  one fact both ends agree on.

  This was a real bug and it was not the one killing players — the sessions dying
  at ~38s had no duplicate to be judged against. The cause was found in v0.8.8:
  their packets were being silently discarded.

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
