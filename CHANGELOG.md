# Changelog

All notable changes to this project are documented here. Format loosely
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); the project
uses semantic versioning.

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
