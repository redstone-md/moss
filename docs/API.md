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

- `mesh_id`: required UTF-8 mesh identifier.
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

### `Moss_Free`

```c
void Moss_Free(void* ptr);
```

Frees memory returned by:

- `Moss_GetMeshInfo`
- `Moss_GetPublicKey`
- `Moss_GetNATType`

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
  }
}
```

Notes:

- omitting `trackers` uses the built-in default tracker set
- explicitly passing `"trackers": []` disables tracker bootstrap
- partial nested config objects are supported; unspecified fields fall back to defaults

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
