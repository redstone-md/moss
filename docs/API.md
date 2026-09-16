# Moss API Reference

This document describes the current public FFI surface exported by `cmd/moss-ffi`.

For packaging, lifecycle, callback/threading guidance, and JNI integration patterns, see [docs/SHARED_INTEGRATION.md](./SHARED_INTEGRATION.md).

## Build Outputs

Build Moss as a C-shared library:

```bash
# Linux
go build -buildmode=c-shared -o libmoss.so ./cmd/moss-ffi

# Windows
go build -buildmode=c-shared -o moss.dll ./cmd/moss-ffi

# macOS
go build -buildmode=c-shared -o libmoss.dylib ./cmd/moss-ffi
```

The generated C header is emitted next to the library (`moss.h` or `libmoss.h`).

## Lifecycle

### `Moss_Init`

```c
MossHandle Moss_Init(const char* mesh_id, const uint8_t* psk, const char* config);
```

Creates a node instance and returns an opaque handle.

- `mesh_id`: required UTF-8 mesh identifier. Use `global` — the standard public
  mesh maintained by the Moss developers — to join the shared network; choose a
  unique id only for a private, isolated mesh.
- `psk`: optional 32-byte pre-shared key; pass `NULL` for an open mesh.
- `config`: optional JSON config; pass `NULL` for defaults.

Returns a positive handle on success or a negative error code on failure.

### `Moss_Start`

```c
int32_t Moss_Start(MossHandle handle);
```

Starts listeners, bootstrap, maintenance loops, NAT profiling, and mesh operations.

### `Moss_Stop`

```c
int32_t Moss_Stop(MossHandle handle);
```

Stops the node, closes sessions, releases runtime resources, and invalidates the handle.

## Connectivity

### `Moss_Connect`

```c
int32_t Moss_Connect(MossHandle handle, const char* addr);
```

Attempts an explicit direct connection to `host:port`.

This is optional. The runtime can still bootstrap and discover peers autonomously.

## Pub/Sub

### `Moss_Subscribe`

```c
int32_t Moss_Subscribe(MossHandle handle, const char* channel);
```

Subscribes the local node to a channel.

### `Moss_Unsubscribe`

```c
int32_t Moss_Unsubscribe(MossHandle handle, const char* channel);
```

Unsubscribes from a channel and sends `PRUNE` to current mesh peers.

### `Moss_Publish`

```c
int32_t Moss_Publish(MossHandle handle, const char* channel,
                     const uint8_t* data, uint32_t len);
```

Publishes a binary payload to a channel.

The current runtime uses:

- local flood publish to eligible direct peers
- GossipSub-style mesh forwarding
- `IHAVE` / `IWANT` replay for recent messages
- `IDONTWANT` suppression for larger payloads


## Rooms

Rooms let ONE node serve several conversations. A host that gave each
conversation its own room had to start a node per conversation, and node
identity is per process, so every one of those nodes presented the same peer
id from a different port — remote peers keep one session per identity and
closed the rest on arrival.

The room-less calls (`Moss_Subscribe`, `Moss_Publish`, ...) are unchanged and
still mean "this node's own room", so a host that does not care never sees
any of this. Callers older than this build simply lack the symbols; treat a
missing one as "this moss cannot share a node".

### `Moss_JoinRoom`

```c
int32_t Moss_JoinRoom(MossHandle handle, const char* mesh_id,
                      const uint8_t* psk, uint32_t psk_len);
```

Adds a room this node can subscribe and publish in, alongside the one it was
constructed with. Idempotent. `psk` may be `NULL` for an open room.

### `Moss_LeaveRoom`

```c
int32_t Moss_LeaveRoom(MossHandle handle, const char* mesh_id);
```

Drops a joined room's key. Subscriptions made in it stop resolving, so
anything still arriving for it is dropped rather than delivered. Callers
should `Moss_UnsubscribeRoom` first if they want the mesh told; this only
forgets the key. The node's own room cannot be left this way.

### `Moss_SubscribeRoom`

```c
int32_t Moss_SubscribeRoom(MossHandle handle, const char* mesh_id,
                           const char* channel);
```

Subscribes the node to a channel inside a joined room.

### `Moss_UnsubscribeRoom`

```c
int32_t Moss_UnsubscribeRoom(MossHandle handle, const char* mesh_id,
                             const char* channel);
```

Leaves a channel inside a joined room.

### `Moss_PublishRoom`

```c
int32_t Moss_PublishRoom(MossHandle handle, const char* mesh_id,
                         const char* channel,
                         const uint8_t* data, uint32_t len);
```

Publishes a binary payload to a channel inside a joined room. The message
callback fires with the room's channel name; rooms carried in the envelope
are matched on the receiving side, so a message published to a channel the
node is subscribed to in that room arrives regardless of which mesh carried
it.

## Directed Payloads (DMs)

### `Moss_ConnectToPeer`

```c
int32_t Moss_ConnectToPeer(MossHandle handle, const char* peer_id);
```

Attempts an explicit direct connection to a peer by its hex-encoded public
key (64 chars), using known addresses for it — worth calling when reaching
one specific peer matters (e.g. a DM counterpart on the room-blind
substrate, which organic discovery would only ever reach by chance). The
registration survives disconnects and is dropped on `Moss_Stop`.

### `Moss_SendToPeer`

```c
int32_t Moss_SendToPeer(MossHandle handle, const char* peer_id,
                        const uint8_t* data, int32_t len);
```

Delivers a directed payload to one peer: over the direct session when one
exists, else via the relay path with the same 5-second budget as
`Moss_RelaySendTo`. The receiver sees it through the packet callback
(`Moss_SetPacketCallback`), which also catches relayed payloads. The size
gate matches `Moss_Publish`'s (`security.max_message_size_bytes`); callers
wanting larger directed transfers must chunk.

Returns `MOSS_ERR_RELAY_FAILED` (-11) when neither path could deliver.

### `Moss_SendToPeerAsync`

```c
uint64_t Moss_SendToPeerAsync(MossHandle handle, const char* peer_id,
                              const uint8_t* data, int32_t len,
                              MossAsyncCompletionCallback cb);
```

The non-blocking form of `Moss_SendToPeer`: the same routing (direct
session first, relay fallback) and the same 5-second relay budget, but the
send runs on a detached goroutine and the outcome is reported through the
completion callback instead of the return value.

- The payload is copied before the call returns, so the caller may free the
  buffer immediately.
- Returns a job ID — never 0, never reused — or `0` when the call is
  refused up front (unknown handle, `NULL` peer, negative length, oversize
  payload, or `NULL` callback). A refusal never fires the callback.
- The completion fires exactly once, from a Go runtime thread, possibly
  concurrent with other callbacks. `result` is `MOSS_OK` (0) or
  `MOSS_ERR_RELAY_FAILED` (-11).
- Do not call `Moss_Stop` from inside the callback. Completion is NOT
  guaranteed after `Moss_Stop`: if the handle is gone when the send
  resolves, the callback is dropped rather than invoked on a torn-down host.

Callback signature:

```c
typedef void (*MossAsyncCompletionCallback)(uint64_t job_id,
                                             int32_t result);
```

### `Moss_RelaySendToAsync`

```c
uint64_t Moss_RelaySendToAsync(MossHandle handle, const char* peer_id,
                              const uint8_t* data, int32_t len,
                              MossAsyncCompletionCallback cb);
```

The non-blocking form of `Moss_RelaySendTo`: the explicit relay path with
the same 5-second budget, run on a detached goroutine. The same contract as
`Moss_SendToPeerAsync` applies: payload copied up front, job IDs never 0
or reused, completion exactly once from a Go runtime thread, dropped (not
fired) when the handle was stopped in between.

### `Moss_PeerRTT`

```c
int64_t Moss_PeerRTT(MossHandle handle, const char* peer_id);
```

Returns the last measured round-trip time to a peer in **nanoseconds** —
the same value the maintenance loop's ping/pong probes refresh and that peer
selection sorts by. Returns `0` when the peer is unknown or has not yet
been probed (zero RTT is also a legitimate sub-microsecond measurement on
loopback, but in practice treat 0 as "no sample yet"). Blocked (never
trampolines through a callback), so it is safe to call from a scoring
callback.

## Streams

Streams are ordered per-stream channels. On a peer with a **direct**
session they are multiplexed over the transport mux; on a **relayed** peer
they ride the relay path under a small additive header (see below). The
transport reserves stream 0 (raw) and stream 1 (gossip) and rejects them
with `MOSS_ERR_CONFIG_INVALID`.

Stream ID convention across moss-based applications: 0–1 transport,
100–101 game-profile defaults, 200+ TUN, 300+ messenger app-data (e.g. 300
as the messenger default). All defaults are overridable per-app; only 0–1
are truly off-limits.

Direct is the fast path — no discovery, no dialing, no wrapping. Relay is
the fallback: `Moss_SendStream` on a relayed peer wraps the payload as

```
magic (4 bytes: 'M','S','s','1') || stream_id (4 bytes, big-endian) || data
```

and delivers it via the relay path (`Moss_RelaySendTo` semantics, same
5-second relay-session budget); the receiving side unwraps the header and
dispatches to the `Moss_OnStream` handler for `stream_id`, with the same
callback shape as the direct path. Streams therefore work on every peer,
relayed or direct.

Consequences of the wire format:

- The header reservation is 8 bytes per relayed stream payload. The size
  gate matches `Moss_Publish`'s (`security.max_message_size_bytes`) on the
  raw payload, as before.
- An application payload whose first four bytes happen to equal the magic
  is indistinguishable from a wrapped stream payload and gets misdispatched
  (dropped, or delivered to a stream handler). Applications speaking binary
  protocols over plain relayed DMs should not start their payloads with
  these bytes while stream fallback is in play.
- Registering any stream handler installs the FFI dispatch chain as the
  node's packet callback, so the legacy relay callback
  (`Moss_SetRelayCallback`) stops firing on that handle: mixing
  `Moss_SetRelayCallback` with relayed streams on one handle is
  unsupported. Non-wrapped relayed payloads forward to the packet callback
  when one is registered.

### `Moss_OpenStream`

```c
int32_t Moss_OpenStream(MossHandle handle, const char* peer_id,
                        uint32_t stream_id);
```

Makes sure a reader goroutine drains `stream_id` on the direct session with
`peer_id`, dialing the peer first if unknown. Returns
`MOSS_ERR_NO_PEERS` (-6) when the peer cannot be resolved. On a relayed
peer it returns `MOSS_OK`: nothing needs pre-opening there — the relay
session opens lazily on the first `Moss_SendStream` fallback, and the
handler registered with `Moss_OnStream` catches payloads from either path.

### `Moss_SendStream`

```c
int32_t Moss_SendStream(MossHandle handle, const char* peer_id,
                        uint32_t stream_id,
                        const uint8_t* data, uint32_t len);
```

Writes data to `stream_id` on the direct session with `peer_id`, spawning
the inbound reader for that stream if needed. Fast path: no overlay lookup,
no dialing — a hot loop must not stall on discovery. Use `Moss_OpenStream`
first for peers you have not connected to yet. Size gate matches
`Moss_Publish`'s. On a relayed peer this falls back to the wrapped relay
delivery described above; `MOSS_ERR_RELAY_FAILED` (-11) is returned only
when neither path could deliver.

### `Moss_OnStream`

```c
int32_t Moss_OnStream(MossHandle handle, uint32_t stream_id,
                      MossStreamCallback cb);
```

Registers the handler for `stream_id`; packets arrive on it from the moment
of registration (per-peer readers spawn as peers connect or send). Register
before sending traffic: the handler is snapshotted when a reader spawns, so
re-registering replaces the entry for future readers but does not
retro-fit already-running ones. Passing `NULL` returns
`MOSS_ERR_CONFIG_INVALID`; the runtime has no unregister — re-register with
a no-op handler instead of expecting to clear it.

The handler serves BOTH delivery paths: the direct-session mux and the
relayed fallback map. A relayed sender's payloads are unwrapped by the FFI
dispatch chain and routed to the same C callback with the same shape.

Callback signature:

```c
typedef void (*MossStreamCallback)(const char* peer_id,
                                   const uint8_t* data,
                                   uint32_t len);
```

## Callbacks

### `Moss_SetCallback`

```c
int32_t Moss_SetCallback(MossHandle handle, MossMessageCallback cb);
```

Registers the per-message callback.

Callback signature:

```c
typedef void (*MossMessageCallback)(const char* channel,
                                    const uint8_t* sender_id,
                                    const uint8_t* data,
                                    uint32_t len);
```

### `Moss_SetEventCallback`

```c
int32_t Moss_SetEventCallback(MossHandle handle, MossEventCallback cb);
```

Registers the event callback.

Callback signature:

```c
typedef void (*MossEventCallback)(int32_t event_type,
                                  const char* detail_json);
```

Current event IDs:

- `1` `EventPeerJoined`
- `2` `EventPeerLeft`
- `3` `EventSupernodePromoted`
- `4` `EventSupernodeRevoked`
- `5` `EventTrackerAnnounce`
- `6` `EventTrackerFailure`
- `7` `EventRelayMigrated`
- `8` `EventMessageDelivered` — *reserved, not dispatched by this runtime*
- `9` `EventMessageRead` — *reserved, not dispatched by this runtime*
- `10` `EventTyping` — *reserved, not dispatched by this runtime*
- `11` `EventPresence` — *reserved, not dispatched by this runtime*

Events 8–11 are pinned for the messenger layer but the mesh runtime never
dispatches them yet. Connection-level presence is already covered by
`EventPeerJoined`/`EventPeerLeft`; read receipts and typing indicators are
application-level concepts that live on top of directed payloads — hosts
that want them carry their own protocol inside the payload and emit their
own events. The values are pinned so every host agrees on the numbering
when that layer exists. Treat an unknown positive ID as a future event.

### `Moss_SetRelayCallback`

```c
int32_t Moss_SetRelayCallback(MossHandle handle, MossRelayCallback cb);
```

Registers the legacy callback for relayed data packets. Pass `NULL` to
clear.

Callback signature:

```c
typedef void (*MossRelayCallback)(const uint8_t* sender_id,
                                  const uint8_t* data,
                                  uint32_t length);
```

### `Moss_SetPacketCallback`

```c
int32_t Moss_SetPacketCallback(MossHandle handle, MossPacketCallback cb);
```

Registers the unified sink for directed payloads: it receives BOTH direct
packets (`Moss_SendToPeer` over a direct session) and raw relayed payloads.
The legacy relay callback still fires for relayed payloads while no packet
callback is registered. Pass `NULL` to clear.

On a handle that has ever registered a stream handler (`Moss_OnStream`),
the FFI dispatch chain owns this slot and forwards non-wrapped payloads
here; the ordering between the two calls does not matter. See Streams for
the chain's contract.

Callback signature:

```c
typedef void (*MossPacketCallback)(const uint8_t* sender_id,
                                   const uint8_t* data,
                                   uint32_t length);
```

### `Moss_SetScoringCallback`

```c
int32_t Moss_SetScoringCallback(MossHandle handle, MossScoringCallback cb);
```

Allows the host application to override per-peer score decisions used by:

- mesh candidate selection
- pruning
- opportunistic grafting
- relay candidate ranking

Callback signature:

```c
typedef double (*MossScoringCallback)(const uint8_t* peer_id,
                                      double base_score);
```

`peer_id` is the 32-byte public identity key.

### `Moss_SetKeyStore`

```c
int32_t Moss_SetKeyStore(MossKeyStoreLoadCallback load,
                         MossKeyStoreSaveCallback save);
```

Registers global identity persistence callbacks used by subsequent `Moss_Init` calls.

Callback signatures:

```c
typedef uint32_t (*MossKeyStoreLoadCallback)(uint8_t* buffer,
                                             uint32_t capacity);

typedef void (*MossKeyStoreSaveCallback)(const uint8_t* data,
                                         uint32_t len);
```

Behavior:

- if `load` returns a valid encoded identity, Moss reuses it
- otherwise Moss generates a new identity and calls `save`

## Diagnostics

### `Moss_GetMeshInfo`

```c
const char* Moss_GetMeshInfo(MossHandle handle);
```

Returns a JSON document describing the current node state. Current fields:

```json
{
  "mesh_id": "example",
  "listen_port": 41030,
  "peer_count": 3,
  "peers": ["10.0.0.10:41031"],
  "channels": ["alpha"],
  "nat_type": "unknown",
  "public_key": "hex-encoded-32-byte-key",
  "supernode_ready": false
}
```

### `Moss_GetPublicKey`

```c
const uint8_t* Moss_GetPublicKey(MossHandle handle);
```

Returns a newly allocated 32-byte public key buffer.

### `Moss_GetNATType`

```c
const char* Moss_GetNATType(MossHandle handle);
```

Returns the current NAT type string, for example:

- `unknown`
- `public`
- `full_cone`
- `restricted_cone`
- `port_restricted_cone`
- `symmetric_nat`
- `cgnat`

### `Moss_GetNetworkStats`

```c
const char* Moss_GetNetworkStats(MossHandle handle);
```

Returns a JSON document with the current **privacy-preserving, decentralized**
network telemetry snapshot, or `{}` when telemetry is disabled (`telemetry.enabled`
is `false`, the default).

The snapshot is computed from a gossiped CRDT, so every honest node converges to
the same values; the `epoch_digest` is reproducible and hash-chained to
`prev_digest`, letting any observer verify history without trusting a collector.
No field exposes a peer's address or stable identity. Detailed metrics are
suppressed until at least `k_anon` nodes contribute (`k_anon_ok`).

```json
{
  "epoch": 5829142,
  "node_count_estimate": 1284,
  "contributors": 47,
  "k_anon_ok": true,
  "bandwidth_in_total": 90431122,
  "bandwidth_out_total": 88210044,
  "nat_histogram": {"public": 12, "symmetric_nat": 20, "cgnat": 15},
  "degree_histogram": {"1-2": 9, "3-5": 22, "6-10": 16},
  "epoch_digest": "hex-blake2s-256",
  "prev_digest": "hex-blake2s-256",
  "chain_head": 5829141
}
```

- `node_count_estimate`: HyperLogLog cardinality (cannot enumerate members).
- `bandwidth_*_total`: DP-noised, per-epoch byte sums (omitted when `k_anon_ok` is false).
- `nat_histogram` / `degree_histogram`: aggregate distributions for topology
  *simulation* — no real edges or addresses are ever published.

### `Moss_Version`

```c
const char* Moss_Version(void);
```

Returns the version this library was built at, as a newly allocated string
(free with `Moss_Free`). Release builds carry their tag; anything else
reports `"dev"`.

A host loads moss by path at runtime, so nothing stops an old library from
sitting next to a new host — and the symptoms of that are transport bugs
the host cannot diagnose. This lets a host say which library it got instead
of guessing. Callers must treat a missing symbol as "older than v0.8.17".

### `Moss_LastError`

```c
const char* Moss_LastError(MossHandle handle);
```

Returns the human-readable reason for the most recent operation on this
handle that failed with a coarse error code — chiefly the underlying OS
bind error behind `MOSS_ERR_LISTEN_FAILED` (-13), which is what surfaces
when Go's netpoller cannot bind sockets under an older Wine/Proton.
Returns an allocated C string (free with `Moss_Free`), or `NULL` if the
handle is unknown. Call it before `Moss_Stop`, which removes the handle
from the registry.

### `Moss_EnableAxiom`

```c
int32_t Moss_EnableAxiom(MossHandle handle, const char* token,
                         const char* dataset, const char* endpoint,
                         const char* service);
```

Turns on the opt-in Axiom error/log sink. `token` is an ingest-only Axiom
token, `dataset` the target dataset, `endpoint` the Axiom base URL (`""` →
cloud default `https://api.axiom.co`), and `service` a host identifier
(e.g. `"gse-4576510"`, `"mosh-0.6.5"`). A node ships nothing until this is
called.

### `Moss_LogEvent`

```c
int32_t Moss_LogEvent(MossHandle handle, const char* level,
                      const char* kind, const char* message,
                      const char* fields_json);
```

Ships a structured event through the Axiom sink (no-op when disabled).
`level` is `"error"`|`"warn"`|`"info"`, `kind` a short slug, `message` free
text, and `fields_json` an optional JSON object of extra context (`""` for
none).

### `Moss_Free`

```c
void Moss_Free(void* ptr);
```

Frees memory returned by:

- `Moss_GetMeshInfo`
- `Moss_GetPublicKey`
- `Moss_GetNATType`
- `Moss_GetNetworkStats`
- `Moss_Version`
- `Moss_LastError`

## Error Codes

Current error codes:

- `0` `MOSS_OK`
- `-1` `MOSS_ERR_INVALID_HANDLE`
- `-2` `MOSS_ERR_ALREADY_STARTED`
- `-3` `MOSS_ERR_NOT_STARTED`
- `-4` `MOSS_ERR_INVALID_CHANNEL`
- `-5` `MOSS_ERR_MESSAGE_TOO_LARGE`
- `-6` `MOSS_ERR_NO_PEERS`
- `-7` `MOSS_ERR_TRACKER_FAIL`
- `-8` `MOSS_ERR_CONFIG_INVALID`
- `-9` `MOSS_ERR_OUT_OF_MEMORY`
- `-10` `MOSS_ERR_CONNECT_FAILED`
- `-11` `MOSS_ERR_RELAY_FAILED` — a directed send (or a relayed stream's
  fallback delivery) could not deliver over either path, direct or relay
- `-12` `MOSS_ERR_INTERNAL` — an internal precondition failed
- `-13` `MOSS_ERR_LISTEN_FAILED` — the OS refused the bind; call
  `Moss_LastError` for the underlying reason
- `-14` `MOSS_ERR_NOT_IN_ROOM` — the room was never joined on this handle

## Config JSON

Top-level config schema:

```json
{
  "trackers": ["udp://tracker.opentrackr.org:1337/announce"],
  "announce_interval_sec": 120,
  "listen_port": 0,
  "max_peers": 200,
  "static_peers": ["10.0.0.10:41030"],
  "bootstrap_timeout_sec": 3,
  "gossipsub": {
    "D": 6,
    "D_lo": 4,
    "D_high": 12,
    "D_out": 2,
    "D_lazy": 6,
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
  },
  "transport": {
    "high_throughput": false,
    "stream_buffer_size": 0,
    "udp_buffer_size": 0
  },
  "telemetry": {
    "enabled": false,
    "epoch_sec": 300,
    "dp_epsilon": 1.0,
    "bandwidth_cap_bytes": 1073741824,
    "degree_cap": 256,
  },
  "masq": {
    "enabled": true,
    "cover_sni": "en.wikipedia.org"
  }
}
```

Notes:

- omitting `trackers` uses the built-in default tracker set
- explicitly passing `"trackers": []` disables tracker bootstrap
- partial nested config objects are supported; unspecified fields fall back to defaults
- omitting the `masq` block keeps the masquerade ON (it is the default);
  pass `"masq": {"enabled": false}` to run the bare Noise transport

### Transport Tuning

The `transport` block controls per-session inbound queue sizes and
gossip overhead. Defaults (256-packet queues, full GossipSub control
traffic) suit chat- and discovery-style workloads where each peer
publishes at most a handful of messages per second.

For high-rate point-to-point streams (file transfer, game traffic,
media tunnels), set `high_throughput: true`. This applies the following
preset to the node, taking effect on the next `Moss_Start`:

- per-stream and per-UDP-session inbound queues grow from 256 to 65536
  packets, eliminating silent drops during bursts
- `IHAVE` and `IDONTWANT` gossip control broadcasts are skipped on
  publish, since they exist to amortize duplicate delivery in large
  meshes and add pure overhead in dense point-to-point topologies

`stream_buffer_size` and `udp_buffer_size` allow per-axis overrides when
the preset is too coarse — set them explicitly to choose the queue
capacity without enabling the full preset, or alongside
`high_throughput` to keep the gossip-overhead reduction while picking
custom queue sizes. Values <= 0 fall back to defaults / the preset.

Default is `high_throughput: false` so existing integrations keep
their original memory footprint.

### Telemetry (privacy-preserving network observability)

The `telemetry` block is **off by default**. When `enabled` is `true`, the node
joins a decentralized, gossiped CRDT that yields a self-verifying, hash-chained
snapshot of the network, readable via `Moss_GetNetworkStats`. There is no
collector and no trusted signer: integrity comes from reproducibility.

- `epoch_sec`: snapshot period. Each epoch is hashed and chained to the previous.
- `dp_epsilon`: differential-privacy budget for numeric metrics; smaller = more
  noise / stronger privacy. `<= 0` disables noise.
- `bandwidth_cap_bytes`: per-epoch per-node clamp that bounds DP sensitivity.
- `degree_cap`: per-node connection-count clamp.
- `k_anon`: detailed metrics (bandwidth sums, histograms) are suppressed until at
  least this many nodes contribute in the epoch.

Privacy properties: a node contributes under a per-epoch **unlinkable** id
(`BLAKE2s(epoch ‖ pubkey)`), never its address or public key; node count uses
HyperLogLog (cannot enumerate members); topology is exposed only as aggregate
NAT/degree histograms for client-side *simulation*, never as real edges.

### Masq (peer-to-peer Chrome TLS masquerade)

The `masq` block is **on by default** — Masq is opt-OUT. A node built from
the default config (or from JSON that omits the `masq` block) carries its
direct peer-to-peer TCP legs inside a **Chrome uTLS fingerprint** TLS
stream: outbound dials present a Chrome-shaped ClientHello aimed at
`cover_sni` (default `en.wikipedia.org`), and the listener answers with a
locally generated certificate, so the Noise session inside is
indistinguishable from ordinary HTTPS on the wire. A plain TCP ear is a
beacon an on-path DPI can fingerprint and reset; masking is the sane
default for every fleet that did not explicitly ask for less.

Unlike the Veil "Reality" bearer, Masq is purely peer-to-peer: no relays,
no splice target, no third party to run. Both peers must agree on the
`cover_sni` they shape their camouflage against — the default
`en.wikipedia.org` is chosen so two stock nodes interoperate without
coordinating anything.

To disable the masquerade (the bare Noise-over-TCP path), set:

```json
{ "masq": { "enabled": false } }
```

or, in the Go API, leave `moss.Config.Masq` unset and rely on the default,
or set an explicit `&moss.MasqConfig{Enabled: false}`. A node that opts
out can still talk to a masked peer only if that peer also opts out —
the masquerade replaces the plain TCP ear, so both ends of a direct link
must be in the same mode.

The air-gapped preset (`DefaultOfflineConfig`) keeps masq **off**: an
isolated site has no DPI to hide from, and TLS wrapping would only add
handshake latency and certificate overhead to loopback/LAN traffic that
was never going to leave the host.

## Current Examples

Example integrations live in:

- `examples/c_example`
- `examples/cpp_example`
- `examples/csharp_example`
- `examples/python_example`
- `examples/python_chat`
- `examples/rust_example`

The CI-style smoke coverage in `cmd/moss-ffi/main_test.go` currently compile-and-run tests:

- C
- C++
- Python
- C#

Rust is run when a valid Rust toolchain is configured in the environment.
