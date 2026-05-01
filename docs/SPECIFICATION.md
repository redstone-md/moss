
# Moss Technical Specification

**Document Version:** 2.0  
**Last Updated:** March 2026  
**Project:** Moss  
**Core Technologies:** Go 1.22+, CGO (`-buildmode=c-shared`), BitTorrent UDP/HTTP Trackers (BEP 15), GossipSub v1.1+, Noise Protocol Framework, Autonomous NAT Traversal (STUN/TURN/ICE-lite).

---

## 1. Executive Summary

**Moss** is a zero-infrastructure, embeddable Peer-to-Peer (P2P) networking core written in Go. Compiled via CGO into C-shared libraries (`.dll`, `.so`, `.dylib`), Moss allows any application — written in C, C++, C#, Python, Rust, or any language with C FFI support — to join a decentralized data-exchange mesh with a small host integration layer.

The defining characteristic of Moss is its **minimization of centralized infrastructure requirements**:

1. **Bootstrapping** — Public BitTorrent trackers (BEP 15 UDP, BEP 3 HTTP) serve as zero-cost, high-availability rendezvous points for initial peer discovery.
2. **NAT Traversal** — An autonomous SuperNode promotion system turns publicly reachable peers into distributed STUN/TURN relays, guaranteeing connectivity across all NAT types including Symmetric NAT and Carrier-Grade NAT (CGNAT).
3. **Data Routing** — A GossipSub v1.1-inspired Publish-Subscribe mesh provides topic-based, Sybil-resistant message propagation with peer scoring and adaptive routing.

**Target Use Cases:** Game networking (lobby discovery, in-game messaging), IoT device meshes, collaborative tools, censorship-resistant communication, real-time data synchronization, and any scenario requiring decentralized peer connectivity without server infrastructure.

---

## 2. Goals & Non-Goals

### Goals

| # | Goal | Success Criteria |
|---|------|-----------------|
| G1 | **True Serverless Operation** | Zero dedicated bootstrap, STUN, or TURN servers. Node discovery and connectivity rely exclusively on public BitTorrent tracker infrastructure and peer-promoted SuperNodes. |
| G2 | **Universal FFI Integration** | Thread-safe C-API exported via `go build -buildmode=c-shared`. Verified integration examples in C++, C#, Python (ctypes), and Rust. CGO call overhead ≤50ns per invocation (Go 1.22+ benchmark). |
| G3 | **Autonomous NAT Penetration** | ≥95% connectivity across Full Cone, Restricted Cone, and Port-Restricted Cone NATs via UDP hole-punching. 100% connectivity (including Symmetric NAT) via SuperNode relay fallback. |
| G4 | **Channel-Based Routing** | GossipSub v1.1-inspired PubSub with peer scoring, outbound connection quotas (D_out), and flood publishing. Topic mesh degree D=6, D_lo=4, D_high=12. |
| G5 | **Zero-Config Bootstrapping** | Peer discovery via ≥5 concurrent tracker announces within 3 seconds of `Moss_Start()`. Graceful fallback across tracker failures with rotating tracker list. |
| G6 | **Sub-100ms Mesh Formation** | First peer connection established within 100ms of successful tracker response on LAN, within 500ms on WAN with favorable NAT conditions. |

### Non-Goals

- Building a graphical user interface (GUI) or standalone executable.
- Implementing blockchain ledgers, consensus algorithms, or cryptocurrency tokens.
- Providing long-term persistent data storage (Moss is a transport and routing layer, not a database).
- Implementing application-layer framing protocols (e.g., HTTP, gRPC). Moss provides raw byte-stream and datagram channels.
- Supporting mobile platforms (iOS/Android) in v1.0 — future consideration.

---

## 3. Core Architecture & Mechanics

### 3.1 Bootstrapping Engine (Tracker-Based Rendezvous)

Moss nodes start with zero knowledge of the network. Instead of hardcoding master nodes, Moss utilizes the existing, massive, freely available infrastructure of public BitTorrent trackers as anonymous rendezvous points.

#### Protocol Flow

```
Host App                    Moss Core                    Public Tracker
   │                           │                              │
   │── Moss_Init(mesh_id) ────>│                              │
   │                           │── SHA1(mesh_id) ────────────>│ (InfoHash)
   │── Moss_Start() ──────────>│                              │
   │                           │── UDP Connect Request ──────>│ (BEP 15)
   │                           │<── connection_id ────────────│
   │                           │── Announce(infohash) ───────>│
   │                           │<── Peer List (IP:Port) ──────│
   │                           │                              │
   │                           │── Noise XX Handshake ───────>│ Peer
   │                           │<── Mesh ID Verification ─────│ Peer
   │                           │<══ Encrypted Channel ════════│ Peer
```

#### InfoHash Generation

- The Mesh ID string (e.g., `"MyGame_Lobby_v2"`) is hashed using SHA-1 to produce a standard 20-byte InfoHash (`[20]byte`).
- Optionally, a Pre-Shared Key (PSK) can be mixed into the hash via HKDF-SHA256: `InfoHash = SHA1(HKDF-Expand(PSK, mesh_id, 20))`, preventing adversaries who know the Mesh ID from discovering peers without the PSK.

#### Tracker Communication (BEP 15 — UDP Tracker Protocol)

The announce request follows the 98-byte binary format defined in BEP 15:

| Offset | Size | Field | Moss Value |
|--------|------|-------|------------|
| 0 | 8B | `connection_id` | From connect response |
| 8 | 4B | `action` | `1` (announce) |
| 12 | 4B | `transaction_id` | Random `uint32` |
| 16 | 20B | `info_hash` | `SHA1(mesh_id)` |
| 36 | 20B | `peer_id` | `-MS0100-` + 12 random bytes |
| 56 | 8B | `downloaded` | `0` |
| 64 | 8B | `left` | `1` (pretend incomplete to receive peers) |
| 72 | 8B | `uploaded` | `0` |
| 80 | 4B | `event` | `2` (started) → `0` (periodic) |
| 84 | 4B | `IP address` | `0` (use source IP) |
| 88 | 4B | `key` | Random per-session |
| 92 | 4B | `num_want` | `50` (request up to 50 peers) |
| 96 | 2B | `port` | Moss listening port |

**Retransmission:** Retry after `15 × 2^n` seconds (n = 0..8, max 3840s) per BEP 15.

#### Default Tracker List (Verified Working March 2026)

```
udp://tracker.opentrackr.org:1337/announce     (primary, highest uptime)
udp://open.stealth.si:80/announce
udp://tracker.openbittorrent.com:6969/announce
udp://tracker1.bt.moack.co.kr:80/announce
udp://exodus.desync.com:6969/announce
udp://tracker.torrent.eu.org:451/announce
udp://explodie.org:6969/announce
udp://open.demonii.com:1337/announce
http://tracker.opentrackr.org:1337/announce     (HTTP fallback)
http://tracker.openbittorrent.com:80/announce   (HTTP fallback)
```

The host application MAY provide additional custom trackers via the API. Moss rotates through the list, querying ≥3 trackers concurrently per announce cycle, with a configurable announce interval (default: 120 seconds).

#### Peer Filtering (Custom Handshake)

Upon receiving peer IP:Port pairs from trackers, Moss performs a **Noise Protocol XX handshake** (see Section 3.3) to:
1. Verify that the remote peer is a Moss node (not a standard BitTorrent client).
2. Authenticate membership in the target Mesh ID via PSK or public key verification.
3. Establish an encrypted transport channel.

Non-Moss peers are silently dropped after a configurable timeout (default: 2 seconds).

#### Secondary Discovery: Mainline DHT (BEP 5) — Optional Enhancement

As a complementary discovery mechanism, Moss MAY optionally participate in the BitTorrent Mainline DHT (Kademlia-based, BEP 5) using the same InfoHash:
- `get_peers(infohash)` to discover peers without tracker dependency.
- `announce_peer(infohash, port)` to register presence.
- This provides tracker-independent peer discovery at the cost of higher implementation complexity and startup latency (~15-30 seconds for DHT bootstrap vs ~1-3 seconds for tracker).

**Reference implementations:** `nictuku/dht` (Go, BEP 5 compliant, ~5K packets/sec).

---

### 3.2 Autonomous NAT Traversal & Relay System

To achieve 100% connectivity without centralized STUN/TURN servers, Moss implements a self-sustaining relay mesh through autonomous node promotion.

#### NAT Type Classification

Moss classifies each node's network environment upon joining the mesh:

| NAT Type | Detection Method | Direct Connection | Hole Punch | Relay Required |
|----------|-----------------|-------------------|------------|----------------|
| **Open / Public IP** | Binding response matches local address | Yes | N/A | No |
| **Full Cone** | Same external port for all destinations | Yes (after one outbound) | UDP: ~95% | No |
| **Restricted Cone** | Same port, but IP-restricted | Via hole-punch | UDP: ~95% | Rarely |
| **Port-Restricted Cone** | Different port per destination IP | Via hole-punch | UDP: ~90% | Sometimes |
| **Symmetric NAT** | Different port per destination IP:Port pair | No | Fails | Yes |
| **CGNAT** | Multiple NAT layers detected | Via hole-punch if outer is cone | Varies | Often |

#### Network Profiling Protocol

1. **Port Mapping Attempt:** On startup, Moss attempts to map a UDP port using:
   - **UPnP IGD** (Internet Gateway Device Protocol) — most common in consumer routers.
   - **NAT-PMP** (RFC 6886) — Apple ecosystem routers.
   - **PCP** (Port Control Protocol, RFC 6887) — modern successor to NAT-PMP, supports IPv6 and CGNAT.
   **Implementation notes:**
   - UPnP IGD is implemented directly with `github.com/huin/goupnp`.
   - NAT-PMP is implemented directly with `github.com/jackpal/go-nat-pmp`.
   - STUN binding uses a minimal local RFC 8489 codec for Binding/XOR-MAPPED-ADDRESS only.
2. **External Address Discovery:** If port mapping succeeds, the mapped external address is used. Otherwise, Moss performs a STUN-like binding request against ≥2 already-connected peers (or SuperNodes) from different IP ranges to detect the NAT type and discover the external IP:Port.

3. **Reachability Test:** Moss asks a connected peer to initiate a new connection to the discovered external address. Success confirms public reachability.

#### SuperNode Promotion

A node is promoted to **SuperNode** status when ALL of the following conditions are met:

- The node has a confirmed publicly reachable address (via port mapping or direct public IP).
- The node's measured uptime exceeds a configurable threshold (default: 5 minutes).
- The node has sufficient bandwidth capacity (self-reported, validated via throughput probes from peers).
- The node's GossipSub peer score is above the promotion threshold (prevents malicious nodes from becoming relays).

SuperNode status is announced to the mesh via a signed `SUPERNODE_ANNOUNCE` message. Status is periodically re-verified (default: every 60 seconds) and revoked if conditions degrade.

#### Distributed STUN (Hole-Punching Coordination)

When two nodes behind cone-type NATs wish to connect directly:

1. **Endpoint Exchange:** Both nodes send their external IP:Port (discovered via SuperNode-mediated binding) to each other via an already-connected intermediary peer.
2. **Simultaneous Open:** Both nodes send UDP packets to each other's external endpoints simultaneously, creating NAT state entries ("punching holes").
3. **Verification:** Upon receiving a packet, the Noise XX handshake proceeds over the punched hole.

**Success rates (empirical):** ~92% for UDP across cone NATs, ~70% for TCP (TCP requires `SO_REUSEADDR` and precise RTT-based synchronization).

#### Mesh TURN (Relay Fallback)

When both peers are behind Symmetric NATs (or hole-punching fails after 3 attempts):

1. Both nodes query their routing tables for a mutually connected SuperNode.
2. The SuperNode allocates a relay session with the following constraints:
   - **Token-bucket rate limiting:** Per-relayed-IP, configurable (default: 256 KB/s burst, 64 KB/s sustained).
   - **Session TTL:** Maximum relay duration (default: 30 minutes, renewable).
   - **Maximum concurrent relays per SuperNode:** Configurable (default: 50).
3. All relayed data is **end-to-end encrypted** via the Noise Protocol session — the SuperNode sees only opaque ciphertext.
4. The relay session is transparently migrated to a direct connection if NAT conditions change (e.g., if a node gains a public IP).

#### Port Prediction (Symmetric NAT Enhancement)

For Symmetric NATs with sequential port allocation behavior, Moss MAY attempt port prediction:
- Observe the port delta across multiple binding requests to different SuperNodes.
- If sequential (delta = 1 or constant), predict the next allocated port.
- Attempt simultaneous open to `external_ip:predicted_port ± N` (N = 5 by default).
- **Expected success rate:** ~80% for sequential allocators, ~0% for random allocators.

---

### 3.3 Cryptographic Transport Layer

#### Noise Protocol Framework

All Moss connections (direct and relayed) are secured using the **Noise Protocol Framework** (noiseprotocol.org). Moss uses two handshake patterns:

| Pattern | Use Case | RTT | Properties |
|---------|----------|-----|------------|
| **Noise_XX_25519_ChaChaPoly_BLAKE2s** | First connection between two peers | 1.5 RTT | Mutual authentication, forward secrecy, identity hiding |
| **Noise_IK_25519_ChaChaPoly_BLAKE2s** | Reconnection (responder's static key cached) | 1 RTT | 0-RTT encrypted payload, forward secrecy |

**Cryptographic primitives:**
- **DH function:** Curve25519 (X25519) — 128-bit security level.
- **Cipher:** ChaCha20-Poly1305 — fast on non-AES-NI hardware (ARM, older CPUs).
- **Hash:** BLAKE2s — faster than SHA-256, 256-bit security.

**Go implementation:** `github.com/flynn/noise` — production-ready, used in WireGuard ecosystem. For `net.Conn` wrapping: `github.com/go-i2p/go-noise`.

#### Handshake Extension: Mesh ID Binding

After the Noise XX handshake establishes a shared secret, Moss performs an additional Mesh ID verification step:

```
Initiator                              Responder
    │                                      │
    │── Noise XX msg1 (e) ────────────────>│
    │<── Noise XX msg2 (e, ee, s, es) ────│
    │── Noise XX msg3 (s, se) ────────────>│
    │                                      │
    │  [Encrypted channel established]     │
    │                                      │
    │── MESH_ID_PROOF(HMAC(session_key,    │
    │   mesh_id || nonce)) ───────────────>│
    │<── MESH_ID_ACK / MESH_ID_REJECT ────│
```

- If a PSK is configured, both parties must prove knowledge of the PSK by including `HMAC(PSK, session_key || mesh_id)` in the proof.
- If no PSK, any peer with the correct Mesh ID string is accepted (open mesh mode).

#### Key Management

- **Static keypairs** are generated per Moss instance on first `Moss_Init()` and stored in memory (not persisted to disk by default).
- The host application MAY provide a persistent key store via `Moss_SetKeyStore(load_fn, save_fn)` callback for identity persistence across restarts.
- **Ephemeral keypairs** are generated per handshake for forward secrecy.

---

### 3.4 Data Exchange & Routing (GossipSub-Inspired Channels)

Moss uses a topic-based Publish-Subscribe architecture inspired by **libp2p GossipSub v1.1** to prevent network flooding, resist Sybil attacks, and allow granular data flow.

#### Mesh Parameters

| Parameter | Value | Description |
|-----------|-------|-------------|
| **D** (mesh degree) | 6 | Target number of mesh peers per topic |
| **D_lo** | 4 | Minimum peers before grafting |
| **D_high** | 12 | Maximum peers before pruning |
| **D_out** | 2 | Minimum outbound connections (Sybil resistance) |
| **D_lazy** | 6 | Gossip propagation degree |
| **Heartbeat** | 1 second | Mesh maintenance interval |

#### Message Propagation

1. **Flood Publishing:** New messages from the local node are sent to ALL mesh peers for the topic (bypasses potential Sybil-dominated mesh connections).
2. **Mesh Forwarding:** Received messages are forwarded only to mesh peers for the topic (not to non-subscribed peers).
3. **Gossip (IHAVE/IWANT):** Every heartbeat, nodes emit IHAVE messages to `D_lazy` random non-mesh peers subscribed to the topic. Peers respond with IWANT to request messages they haven't seen.
4. **IDONTWANT (GossipSub v1.2):** For large messages (>1KB), nodes immediately broadcast IDONTWANT with the message ID to all mesh peers, suppressing duplicate transmissions.

#### Peer Scoring (Sybil Resistance)

Each node locally computes scores for all known peers based on observed behavior. Scores are **never shared** across the network.

**Scoring Parameters:**

| Parameter | Weight | Description |
|-----------|--------|-------------|
| **P1: Time in Mesh** | +0.03 | Rewards long-lived mesh participation |
| **P2: First Message Deliveries** | +0.66 | Rewards first delivery of valid messages |
| **P3: Mesh Message Delivery Deficit** | -0.50 | Penalizes mesh peers who don't forward messages |
| **P4: Invalid Messages** | -10.0 | Penalizes delivery of invalid/malformed messages |
| **P5: Application-Specific** | Configurable | Host app can inject custom scoring logic via `Moss_SetScoringCallback` |
| **P6: IP Colocation** | -5.0 | Penalizes excessive peers from the same IP (Sybil indicator) |

**Score Thresholds:**

| Threshold | Value | Action |
|-----------|-------|--------|
| **0** | Baseline | Peers below 0 are pruned from mesh, excluded from PX |
| **GossipThreshold** | -10 | No gossip emitted to/from peers below this |
| **PublishThreshold** | -100 | Messages from peers below this are not propagated |
| **GraylistThreshold** | -10000 | Peer is completely ignored, all messages dropped |
| **OpportunisticGraftThreshold** | 1.0 | If median mesh score drops below, graft ≥2 high-scoring peers |

#### Channel Operations

- `Moss_Subscribe(channel)` — Subscribes the node to a topic. Triggers GRAFT to D peers.
- `Moss_Unsubscribe(channel)` — Sends PRUNE to all mesh peers for the topic.
- `Moss_Publish(channel, data, len)` — Flood-publishes a byte array to all mesh peers in the channel.
- Messages are identified by `BLAKE2s(sender_pubkey || sequence_number || payload)` for deduplication.
- Message cache retains IDs for 120 heartbeats (2 minutes) for gossip and deduplication.

---

## 4. Foreign Function Interface (FFI) & API Surface

### 4.1 Build Pipeline

```bash
# Linux shared library
CGO_ENABLED=1 GOOS=linux GOARCH=amd64 \
  go build -buildmode=c-shared -o libmoss.so ./cmd/moss-ffi

# Windows DLL
CGO_ENABLED=1 GOOS=windows GOARCH=amd64 CC=x86_64-w64-mingw32-gcc \
  go build -buildmode=c-shared -o moss.dll ./cmd/moss-ffi

# macOS dylib
CGO_ENABLED=1 GOOS=darwin GOARCH=arm64 \
  go build -buildmode=c-shared -o libmoss.dylib ./cmd/moss-ffi
```

**Build outputs:** The shared library (~3-5MB, includes Go runtime) + auto-generated `moss.h` C header file.

**Critical CGO constraints:**
- Package MUST be `main` with an empty `main()` function.
- Exported functions use `//export FunctionName` directive (no space after `//`).
- Go shared libraries **cannot be safely unloaded** (`dlclose` not supported). Use `-ldflags="-extldflags=-Wl,-z,nodelete"` to prevent unload attempts.
- Each loaded Go shared library embeds its own Go runtime. Do NOT load multiple Moss instances in a single process.
- CGO call overhead: ~40ns per call on Go 1.22+ (17x faster than pre-1.21 versions).

### 4.2 Core C-API

```c
// moss.h (auto-generated, key signatures shown)

// ═══════════════════════════════════════════════════════════
//  LIFECYCLE
// ═══════════════════════════════════════════════════════════

// Initialize the Moss core. Returns an opaque handle.
// mesh_id: UTF-8 string identifying the mesh network.
// psk: Optional Pre-Shared Key (32 bytes). Pass NULL for open mesh.
// config: Optional JSON config string. Pass NULL for defaults.
// Returns: handle > 0 on success, negative error code on failure.
typedef int64_t MossHandle;
MossHandle Moss_Init(const char* mesh_id, const uint8_t* psk, const char* config);

// Start mesh operations (tracker announces, network profiling).
// Returns: 0 on success, negative error code on failure.
int32_t Moss_Start(MossHandle handle);

// Gracefully stop all operations, disconnect from peers, free resources.
int32_t Moss_Stop(MossHandle handle);

// ═══════════════════════════════════════════════════════════
//  PUB/SUB
// ═══════════════════════════════════════════════════════════

// Subscribe to a topic/channel.
int32_t Moss_Subscribe(MossHandle handle, const char* channel);

// Unsubscribe from a topic/channel.
int32_t Moss_Unsubscribe(MossHandle handle, const char* channel);

// Publish a message to a channel.
// data: pointer to byte array. len: length in bytes.
// Returns: 0 on success, negative error code on failure.
int32_t Moss_Publish(MossHandle handle, const char* channel,
                     const uint8_t* data, uint32_t len);

// ═══════════════════════════════════════════════════════════
//  CALLBACKS
// ═══════════════════════════════════════════════════════════

// Callback function type for incoming messages.
// channel: topic name (null-terminated).
// sender_id: 32-byte public key of sender.
// data/len: message payload.
typedef void (*MossMessageCallback)(const char* channel,
                                     const uint8_t* sender_id,
                                     const uint8_t* data, uint32_t len);

// Register the message callback. Thread-safe; callback is invoked
// from a dedicated Go goroutine (NOT from C threads).
int32_t Moss_SetCallback(MossHandle handle, MossMessageCallback cb);

// Callback for mesh events (peer joined/left, supernode promotion, etc.)
typedef void (*MossEventCallback)(int32_t event_type,
                                   const char* detail_json);
int32_t Moss_SetEventCallback(MossHandle handle, MossEventCallback cb);

// ═══════════════════════════════════════════════════════════
//  MESH INFO
// ═══════════════════════════════════════════════════════════

// Returns JSON string with mesh state (peer count, channels, NAT type, etc.)
// Caller must free the returned pointer with Moss_Free().
const char* Moss_GetMeshInfo(MossHandle handle);

// Returns the node's 32-byte public identity key.
const uint8_t* Moss_GetPublicKey(MossHandle handle);

// Returns the detected NAT type as a string.
const char* Moss_GetNATType(MossHandle handle);

// ═══════════════════════════════════════════════════════════
//  MEMORY
// ═══════════════════════════════════════════════════════════

// Free memory allocated by the Go runtime (for strings returned by Moss_GetMeshInfo, etc.)
void Moss_Free(void* ptr);
```

### 4.3 Thread Safety & Callback Model

**Critical CGO callback constraints (Go 1.22+):**

1. **Go → C → Go works** — When a Go goroutine calls C, and C calls back into Go, it runs on the same OS thread.
2. **Pure C threads CANNOT call Go** — Threads created by C code (`pthread_create`, etc.) will crash when calling Go functions. This is a known Go limitation (issue #3068).
3. **Moss solution:** All callbacks (`MossMessageCallback`, `MossEventCallback`) are invoked from a **dedicated Go goroutine via a channel-based dispatch queue**. The host application's callback function is called on a Go-managed OS thread, ensuring safety.

```
┌──────────────────────┐     ┌────────────────────────┐
│  Host Application    │     │     Moss Core (Go)      │
│                      │     │                         │
│  callback_fn() <─────│─────│── dispatch goroutine    │
│                      │     │      ↑                  │
│                      │     │   channel buffer (1024) │
│                      │     │      ↑                  │
│                      │     │   GossipSub handler     │
│                      │     │   NAT event handler     │
└──────────────────────┘     └────────────────────────┘
```

**Concurrency control:** Moss internally limits concurrent CGO boundary crossings using a semaphore (default: 500) to prevent OS thread exhaustion.

### 4.4 Configuration (JSON)

```json
{
  "trackers": [
    "udp://tracker.opentrackr.org:1337/announce",
    "udp://open.stealth.si:80/announce"
  ],
  "announce_interval_sec": 120,
  "listen_port": 0,
  "max_peers": 200,
  "gossipsub": {
    "D": 6,
    "D_lo": 4,
    "D_high": 12,
    "D_out": 2,
    "heartbeat_ms": 1000
  },
  "nat": {
    "upnp_enabled": false,
    "natpmp_enabled": false,
    "pcp_enabled": false,
    "supernode_min_uptime_sec": 300,
    "relay_max_bandwidth_kbps": 256,
    "relay_max_sessions": 50,
    "relay_session_ttl_sec": 1800,
    "hole_punch_attempts": 3,
    "port_prediction_enabled": true
  },
  "security": {
    "handshake_timeout_sec": 5,
    "max_message_size_bytes": 65536,
    "rate_limit_burst": 256000,
    "rate_limit_sustained": 64000
  }
}
```

---


Operational, resource, testing, and appendix details continue in [SPECIFICATION-OPERATIONS.md](SPECIFICATION-OPERATIONS.md).
