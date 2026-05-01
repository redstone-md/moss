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
| `github.com/ethereum/go-ethereum/p2p/nat` | UPnP + NAT-PMP unified interface | LGPL-3.0 |
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

| Test Type | Scope | Tools |
|-----------|-------|-------|
| **Unit Tests** | Individual components (InfoHash generation, Noise handshake, peer scoring, token bucket) | `go test`, table-driven tests |
| **Integration Tests** | Multi-node scenarios on localhost (mesh formation, pubsub propagation, NAT simulation) | Docker Compose with `tc` (traffic control) for latency/packet loss simulation |
| **NAT Simulation** | Full Cone, Restricted, Port-Restricted, Symmetric NAT, CGNAT | `iptables` rules in Docker containers, `mininet` for complex topologies |
| **FFI Tests** | C, C++, C#, Python, Rust integration | Compile-and-run test binaries, memory leak detection via Valgrind/ASan |
| **Load Tests** | 100+ node mesh, throughput saturation, relay capacity | Kubernetes cluster with Moss containers |
| **Fuzz Tests** | Malformed tracker responses, invalid Noise handshakes, oversized messages | `go test -fuzz`, AFL |

---

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

---

## Appendix B: Wire Protocol Message Types

| ID | Type | Direction | Description |
|----|------|-----------|-------------|
| 0x01 | `MESH_ID_PROOF` | Bidirectional | Post-handshake mesh membership verification |
| 0x02 | `MESH_ID_ACK` | Response | Mesh membership confirmed |
| 0x03 | `MESH_ID_REJECT` | Response | Mesh membership rejected (wrong mesh/PSK) |
| 0x10 | `GRAFT` | Bidirectional | Request to join topic mesh |
| 0x11 | `PRUNE` | Bidirectional | Leave topic mesh, with backoff timer |
| 0x12 | `IHAVE` | Outbound | Gossip: advertise known message IDs |
| 0x13 | `IWANT` | Response | Request messages by ID |
| 0x14 | `IDONTWANT` | Outbound | Suppress duplicate sends for large messages |
| 0x15 | `PUBLISH` | Bidirectional | Publish message to topic |
| 0x20 | `SUPERNODE_ANNOUNCE` | Outbound | Announce SuperNode status |
| 0x21 | `SUPERNODE_REVOKE` | Outbound | Revoke SuperNode status |
| 0x22 | `RELAY_REQUEST` | Bidirectional | Request relay session |
| 0x23 | `RELAY_DATA` | Bidirectional | Relayed encrypted payload |
| 0x24 | `RELAY_CLOSE` | Bidirectional | Close relay session |
| 0x30 | `BINDING_REQUEST` | Bidirectional | STUN-like external address query |
| 0x31 | `BINDING_RESPONSE` | Response | External IP:Port result |
| 0x32 | `HOLE_PUNCH_COORD` | Bidirectional | Hole-punch coordination (endpoint exchange) |
| 0x33 | `PING` | Bidirectional | Keepalive |
| 0x34 | `PONG` | Response | Keepalive response |
