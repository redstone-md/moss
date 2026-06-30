# Changelog

All notable changes to this project are documented here. Format loosely
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); the project
uses semantic versioning.

## [Unreleased]

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

### Changed

- Default tracker list refreshed: removed unreachable / deprecated hosts
  (`open.stealth.si`, `tracker1.bt.moack.co.kr`, HTTP opentrackr mirror);
  added `open.demonii.com`, `tracker.torrent.eu.org`, `open.tracker.cl`,
  `tracker.openbittorrent.com` HTTP mirror.

### Dependencies

- Adds `github.com/anacrolix/dht/v2 v2.24.0`. Its transitive
  `github.com/anacrolix/torrent` pin is a retracted pseudo-version; only the
  leaf packages `bencode`, `iplist`, `metainfo`, and `types/infohash` are
  used (not the retracted torrent storage code), so the build is safe.
  Re-tidy when `dht/v2` publishes a release off a non-retracted torrent base.

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
