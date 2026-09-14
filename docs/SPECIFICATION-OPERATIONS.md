# Moss Technical Specification - Operations and Appendices

## 5. Security & Resource Management

### 5.1 Threat Model

| Threat | Impact | Mitigation |
|--------|--------|------------|
| **Sybil Attack** (mass fake identities) | Mesh domination, message suppression | GossipSub peer scoring (P4, P6), outbound quotas (D_out), IP colocation penalty |
| **Eclipse Attack** (isolating a target node) | Target receives no messages | Outbound connection quotas ensure ≥D_out honest outbound peers; opportunistic grafting recovers mesh in ~90 heartbeats |
| **Man-in-the-Middle** | Message interception/modification | Noise XX mutual authentication with Curve25519 static keys |
| **Relay Abuse** (flooding via SuperNode) | SuperNode bandwidth exhaustion | Token-bucket rate limiting per relayed IP, session TTL, max concurrent relays |
| **Tracker Poisoning** (injecting fake peers) | Connection to adversary nodes | Noise handshake filters non-Moss peers; PSK mode prevents discovery by adversaries |
| **Mesh ID Enumeration** | Adversary discovers active meshes | PSK-mixed InfoHash generation via HKDF makes InfoHash unpredictable without PSK |
| **DDoS on SuperNodes** | Relay infrastructure collapse | Automatic SuperNode demotion on overload, connection pruning, graceful degradation to direct-only mode |

### 5.2 Cryptography Summary

| Layer | Algorithm | Purpose |
|-------|-----------|---------|
| Key Exchange | X25519 (Curve25519 DH) | 128-bit security, fast on all platforms |
| Symmetric Cipher | ChaCha20-Poly1305 | AEAD encryption, fast without AES-NI hardware |
| Hash | BLAKE2s (256-bit) | Message IDs, peer scoring, faster than SHA-256 |
| InfoHash | SHA-1 (20-byte, BEP 15 compat) | Tracker announce compatibility |
| PSK Derivation | HKDF-SHA256 | PSK-to-InfoHash and PSK-to-session-key derivation |
| Identity | Ed25519 | Node identity keypair (signing), derived Curve25519 for DH |

### 5.3 Rate Limiting & Resource Bounds

**SuperNode Relay Rate Limiting (Token Bucket):**
- **Bucket capacity:** 256 KB (burst allowance).
- **Refill rate:** 64 KB/s sustained per relayed peer.
- **Implementation:** In-memory per-IP token buckets, cleaned up on session expiry.
- Exceeding the rate limit triggers a 429-equivalent response and temporary relay suspension.

**Connection Management:**
- **Maximum peers:** Configurable (default: 200).
- **Connection pruning:** Every 30 seconds, peers with latency >2s or score <0 are candidates for pruning.
- **Inbound connection throttling:** Maximum 10 new inbound connections per second to prevent connection flood attacks.

---

## 6. Project Structure

```
moss/
├── cmd/
│   └── moss-ffi/
│       └── main.go              # CGO exports, empty main(), //export directives
├── internal/
│   ├── bootstrap/
│   │   ├── tracker_udp.go       # BEP 15 UDP tracker client
│   │   ├── tracker_http.go      # BEP 3 HTTP tracker client
│   │   ├── infohash.go          # SHA-1 InfoHash generation + HKDF-PSK mixing
│   │   └── tracker_manager.go   # Concurrent tracker rotation, retry logic
│   ├── transport/
│   │   ├── noise.go             # Noise XX/IK handshake (flynn/noise)
│   │   ├── conn.go              # Encrypted connection wrapper (net.Conn)
│   │   ├── multiplexer.go       # Stream multiplexing over single connection
│   │   ├── listener.go          # TCP listener with accept logic
│   │   ├── udp.go               # UDP listener public surface and shared state
│   │   ├── udp_handshake.go     # UDP Noise handshake packet flow
│   │   ├── udp_observe.go       # UDP/STUN endpoint observation helpers
│   │   └── udp_session.go       # UDP carrier/session lifecycle
│   ├── nat/
│   │   ├── profiler.go          # NAT type classification
│   │   ├── upnp.go              # UPnP IGD port mapping
│   │   ├── pmp.go               # NAT-PMP / PCP port mapping
│   │   ├── holepunch.go         # UDP/TCP hole-punching coordinator
│   │   ├── relay.go             # SuperNode relay session manager
│   │   └── supernode.go         # SuperNode promotion/demotion logic
│   ├── gossip/
│   │   ├── pubsub.go            # GossipSub v1.1 mesh manager
│   │   ├── scoring.go           # Peer scoring engine
│   │   ├── messages.go          # GRAFT, PRUNE, IHAVE, IWANT, IDONTWANT
│   │   └── cache.go             # Message ID cache (deduplication)
│   ├── mesh/
│   │   ├── node_types.go        # Core Node state and private helper structs
│   │   ├── node_lifecycle.go    # construction, start/stop, public Node API
│   │   ├── node_accept.go       # inbound/outbound peer connection lifecycle
│   │   ├── node_advertise.go    # local address and announce-port selection
│   │   ├── node_envelope.go     # gossip envelope delivery and flood publish
│   │   ├── node_gossip_control.go # IHAVE/IWANT/IDONTWANT control flow
│   │   ├── node_peer_*.go       # peer announcement, discovery, topic mesh upkeep
│   │   ├── node_relay_*.go      # relay API, selection, and relay control flow
│   │   ├── node_nat_control.go  # binding, reachability, and hole-punch messages
│   │   ├── node_reachability.go # external address and reachability probes
│   │   ├── node_maintenance.go  # latency probing, pruning, and housekeeping
│   │   ├── config.go            # JSON config parsing + defaults
│   │   └── events.go            # Event bus (peer join/leave, supernode, etc.)
│   └── crypto/
│       ├── keys.go              # Ed25519 identity, Curve25519 derivation
│       └── hkdf.go              # HKDF-SHA256 key derivation
├── examples/
│   ├── c_example/
│   │   ├── main.c               # C integration example
│   │   └── Makefile
│   ├── python_example/
│   │   └── moss_demo.py         # Minimal Python ctypes integration
│   ├── cpp_example/
│   │   └── main.cpp             # C++ integration example
│   ├── csharp_example/
│   │   ├── Program.cs           # C# integration example
│   │   └── MossDemo.csproj
│   ├── python_chat/             # Interactive Python chat demo
│   │   ├── moss_chat.py         # Compatibility entrypoint
│   │   └── README.md
│   └── rust_example/
│       ├── src/main.rs           # Rust FFI integration
│       └── build.rs
├── Makefile                      # Cross-platform build targets
├── go.mod
├── go.sum
└── README.md
```

### 6.1 Package Slicing Rules

The detailed architecture and import-boundary rules live in [ARCHITECTURE.md](./ARCHITECTURE.md). The short version:

Go package boundaries should follow encapsulation boundaries, not individual files. `internal/mesh` intentionally remains a single package because the `Node` coordinator owns tightly coupled peer, relay, NAT, scoring, and pubsub state. Splitting these files into child packages would force private state to become exported or introduce broad interfaces that only exist to cross directory boundaries.

Preferred slicing inside `internal/mesh`:

- keep `node_types.go` as the private state map for `Node` and closely related structs
- keep public lifecycle and API methods in `node_lifecycle.go` and `node_relay_api.go`
- group private behavior by capability using `node_<capability>.go`
- keep integration tests grouped by scenario, not by implementation file
- create a new package only when the code can expose a small stable API and stop depending on `Node` internals

---

## 7. Development Phases & Milestones

| Phase | Milestone | Deliverables | Key Dependencies | Acceptance Criteria |
|-------|-----------|-------------|------------------|-------------------|
| **Phase 1** | **Foundation & Build Pipeline** | Go project scaffolding, CGO FFI skeleton, cross-platform build scripts (`.dll`, `.so`, `.dylib`), memory management tests, CI pipeline. | Go 1.22+, GCC/MinGW toolchains | `Moss_Init` / `Moss_Stop` / `Moss_Free` work from C, Python, and C++. No memory leaks on 10K init/stop cycles. Build produces valid headers for all 3 platforms. |
| **Phase 2** | **BitTorrent Bootstrap Layer** | BEP 15 UDP tracker client, BEP 3 HTTP tracker fallback, SHA-1 InfoHash generation, HKDF-PSK mixing, concurrent multi-tracker querying, retry/backoff logic. | Phase 1 | Successfully retrieve ≥1 peer IP from `tracker.opentrackr.org` within 3 seconds. PSK-mixed InfoHash produces different hash than plain SHA-1. |
| **Phase 3** | **Cryptographic Transport** | Noise XX/IK handshakes (using `flynn/noise`), encrypted `net.Conn` wrapper, Mesh ID verification protocol, Ed25519 identity generation, key caching for IK reconnection. | Phase 1 | Two Moss nodes on same LAN establish encrypted connection in <100ms. Non-Moss peers are rejected within 2 seconds. Handshake produces unique session keys. |
| **Phase 4** | **GossipSub Channels** | Topic mesh manager (GRAFT/PRUNE), IHAVE/IWANT gossip, flood publishing, message deduplication cache, peer scoring engine (P1-P6), C-API for subscribe/publish/callback. | Phase 3 | 10-node mesh on LAN: message published to a channel reaches all subscribers within 500ms. Peer with score <0 is pruned within 2 heartbeats. |
| **Phase 5** | **Autonomous NAT Engine** | NAT type classifier, UPnP/NAT-PMP/PCP port mapping, SuperNode auto-promotion, distributed STUN (binding via peers), UDP hole-punching coordinator, relay fallback with rate limiting, port prediction for Symmetric NAT. | Phases 3, 4 | Two nodes behind separate Port-Restricted Cone NATs connect via hole-punch. Two nodes behind Symmetric NATs connect via SuperNode relay within 5 seconds. SuperNode correctly rate-limits to configured bandwidth. |
| **Phase 6** | **Integration, Optimization & Documentation** | Connection pruning, bandwidth governance, comprehensive API documentation, integration examples in C, C++, C#, Python, and Rust, benchmarks (throughput, latency, memory), integration test suite with simulated NAT topologies. | Phases 1-5 | Published API docs with all functions documented. All examples compile and run. Throughput ≥10 MB/s direct, ≥256 KB/s relayed. Memory usage <50MB per node with 200 peers. |

---

## 8. Recommended Go Dependencies

| Package | Purpose | License |
|---------|---------|---------|
| `github.com/flynn/noise` | Noise Protocol Framework (XX/IK handshakes) | BSD-3 |
| `github.com/huin/goupnp` | UPnP IGD port mapping | BSD-2 |
| `github.com/jackpal/go-nat-pmp` | NAT-PMP port mapping | Apache-2.0 |
| `github.com/jech/portmap` | Conflict-safe port mapping | MIT |
| `golang.org/x/crypto` | HKDF, Ed25519, Curve25519 | BSD-3 |
| `golang.org/x/net` | UDP/TCP networking utilities | BSD-3 |
| `github.com/nictuku/dht` | Mainline DHT (BEP 5) — optional | Apache-2.0 |

**Zero external dependencies for the core** is ideal. Consider vendoring `flynn/noise` and implementing HKDF/Ed25519 via Go stdlib (`crypto/ed25519`, `golang.org/x/crypto/hkdf`).

---

## 9. Performance Targets

| Metric | Target | Measurement |
|--------|--------|-------------|
| Peer discovery latency | <3s from `Moss_Start()` | Time to first peer connection via tracker |
| Direct connection throughput | ≥10 MB/s | 1MB message, same datacenter |
| Relayed connection throughput | ≥256 KB/s | Through SuperNode relay |
| Message propagation (10 peers) | <500ms | Flood publish, LAN topology |
| Message propagation (100 peers) | <2s | GossipSub mesh, WAN topology |
| Memory usage (200 peers) | <50 MB | Heap profiling at steady state |
| CGO call overhead | <50ns | `Moss_Publish` round-trip, Go 1.22+ |
| Library binary size | <5 MB | Stripped `.so` / `.dll` |
| Concurrent relay sessions | 50 per SuperNode | Under load test |

---

## 10. Testing Strategy

| Test Type | Scope | Tools | Status |
|-----------|-------|-------|--------|
| **Unit Tests** | Individual components (InfoHash generation, Noise handshake, peer scoring, token bucket) | `go test`, table-driven tests | Implemented and in CI on every push |
| **Integration Tests** | Multi-node scenarios on localhost (mesh formation, pubsub propagation, NAT simulation) | Plain `go test` multi-node topologies on loopback | Implemented and in CI on every push |
| **NAT Simulation** | Full Cone, Restricted, Port-Restricted, Symmetric NAT, CGNAT | Simulated profiles exercised through the NAT profiler's own classification path | Implemented at the classification level; container-based `iptables` simulation is planned, not present |
| **FFI Tests** | C, C++, C#, Python, Rust integration | Compile-and-run test binaries in `cmd/moss-ffi/main_test.go` | Implemented and in CI; memory leak detection via Valgrind/ASan is planned, not wired |
| **Load Tests** | 25-node sustained soak, 201-node steady-state memory | `go test` loopback topologies, `-bench` heap gate (`make soak`, `make memory-gate`) | Implemented in CI; the 100-node Kubernetes fleet target is planned, not present (no k8s manifests or containers in the repository) |
| **Race Tests** | Concurrency correctness | `go test -race` on fast packages per push; nightly `-race` sweep of `internal/mesh` | Implemented (`.github/workflows/ci-dev.yml`) |
| **Fuzz Tests** | Malformed tracker responses, invalid Noise handshakes, oversized messages | `go test -fuzz` seeds shipped in `internal/gossip`/`internal/bootstrap` | Seed corpus shipped and run in CI; continuous fuzzing with AFL is planned, not present |

Infrastructure-backed tooling that this document previously described as
current — Docker Compose with `tc` latency/loss simulation, `mininet`
topologies, Kubernetes load clusters, and AFL — is **planned**. No Docker,
`tc`, `mininet`, or Kubernetes configuration exists in the repository today,
and the largest automated topologies are loopback-only (25 nodes for
behavior, 201 nodes for memory). Until that tooling lands, load claims in
§9 beyond those scales should be read as targets, not as measured results.

## Appendix A: Error Codes

| Code | Name | Description |
|------|------|-------------|
| 0 | `MOSS_OK` | Success |
| -1 | `MOSS_ERR_INVALID_HANDLE` | Invalid or expired MossHandle |
| -2 | `MOSS_ERR_ALREADY_STARTED` | `Moss_Start` called on already-running instance |
| -3 | `MOSS_ERR_NOT_STARTED` | Operation requires started instance |
| -4 | `MOSS_ERR_INVALID_CHANNEL` | Channel name is empty or exceeds 256 bytes |
| -5 | `MOSS_ERR_MESSAGE_TOO_LARGE` | Message exceeds `max_message_size_bytes` |
| -6 | `MOSS_ERR_NO_PEERS` | No peers available for the channel |
| -7 | `MOSS_ERR_TRACKER_FAIL` | All trackers failed to respond |
| -8 | `MOSS_ERR_CONFIG_INVALID` | JSON config parsing error |
| -9 | `MOSS_ERR_OUT_OF_MEMORY` | Memory allocation failed |
| -10 | `MOSS_ERR_CONNECT_FAILED` | Connect failed (see `Moss_LastError` for the underlying OS error) |
| -11 | `MOSS_ERR_RELAY_FAILED` | Relay send failed (no route, session open failed, or send error) |
| -12 | `MOSS_ERR_INTERNAL` | Unexpected internal failure (e.g. room-seal crypto error) |
| -13 | `MOSS_ERR_LISTEN_FAILED` | Could not bind the TCP/UDP listener (port in use, or Go's netpoller can't bind under Wine/Proton). `Moss_LastError` has the underlying OS error |
| -14 | `MOSS_ERR_NOT_IN_ROOM` | The room was never joined, so this node cannot address or seal for it |

---

## Appendix B: Wire Protocol Message Types

**This table previously listed numeric wire IDs (0x01…0x34) that do not
match the implementation.** The shipped protocol does not use numeric
message IDs: every mesh message is a JSON gossip envelope whose `type`
field is a string constant (see `internal/gossip/messages.go`). Mesh
membership verification is not a post-handshake message pair either — it
is the Noise handshake itself (signed identity payload + `moss|<mesh_id>`
prologue; PSK mode binds the PSK into the handshake, and UDP carriers add
the scramble codec).

Envelope types on the wire today:

| Type string | Direction | Description |
|------------|-----------|-------------|
| `graft` | Bidirectional | Request to join a topic mesh |
| `prune` | Bidirectional | Leave topic mesh (sets the meshBlocked cooldown; short TTL when answering our own recent GRAFT) |
| `ihave` | Outbound | Gossip: advertise known message IDs |
| `iwant` | Response | Request messages by ID (serve side capped: 64 per request, one per ID per peer per 10s) |
| `idontwant` | Outbound | Suppress duplicate sends for large messages |
| `publish` | Bidirectional | Publish message to topic (room-sealed AEAD payload; Ed25519-signed sender, verify-on-present) |
| `direct` | Bidirectional | Directed payload between two peers (DMs, TUN packets, game snapshots) |
| `peer_announce` | Outbound | Known-peer advertisement (Ed25519-signed, v2 verifies Noise static key) |
| `supernode_announce` | Outbound | Announce SuperNode status |
| `supernode_revoke` | Outbound | Revoke SuperNode status |
| `binding_request` | Bidirectional | STUN-like external address query |
| `binding_response` | Response | External IP:Port result |
| `reachability_request` | Bidirectional | Ask a peer to probe our advertised address |
| `reachability_response` | Response | Reachability probe result |
| `hole_punch_coord` | Bidirectional | Hole-punch coordination (endpoint exchange) |
| `relay_request` | Bidirectional | Request relay session |
| `relay_accept` | Response | Relay session accepted |
| `relay_data` | Bidirectional | Relayed encrypted payload |
| `relay_close` | Bidirectional | Close relay session |
| `ping` | Bidirectional | Keepalive / latency probe |
| `pong` | Response | Keepalive response |
| `stat_delta` | Bidirectional | Telemetry CRDT delta gossip |
| `room_invite` | Bidirectional | Room-membership invitation (sealed to the invitee; peer-to-peer, never flooded) |
| `ov_find_node` | Bidirectional | Overlay (Kademlia) node lookup |
| `ov_find_value` | Bidirectional | Overlay record lookup |
| `ov_nodes` | Response | Overlay lookup contacts |
| `ov_values` | Response | Overlay record providers |
| `ov_store` | Outbound | Overlay record store |

Frame-level constants that DO exist as numbers: the transport stream frame
cap (256 KiB max data frame, `internal/transport/stream.go`), the 64 KiB
handshake frame cap, and the 12-byte TFRG fragment header
(`internal/tun/frag.go`). The 4-byte magic `TFRG` (0x54 0x46 0x52 0x47) is
the only wire-level "magic number" in the envelope payload layer.

