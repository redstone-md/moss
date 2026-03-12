# Moss

[![CI](https://github.com/redstone-md/moss/actions/workflows/ci.yml/badge.svg)](https://github.com/redstone-md/moss/actions/workflows/ci.yml)

Moss is an embeddable P2P mesh core written in Go and exported through CGO as a C-shared library. The project in this repository follows the `PRD.md` scope with a pragmatic v1 implementation:

- tracker-based bootstrapping via BEP 15 UDP and BEP 3 HTTP announces
- encrypted peer transport with Noise XX (`25519_ChaChaPoly_BLAKE2s`) plus identity binding
- topic-based pub/sub routing with GRAFT/PRUNE control messages, BLAKE2s message IDs, and peer scoring
- NAT profiling, relay rate limiting primitives, and supernode eligibility checks
- C FFI surface with examples for C, C++, Python (`ctypes`), and Rust
- native single-binary terminal chat in `cmd/moss-chat`
- unit, integration, and shared-library smoke tests

API reference: [docs/API.md](docs/API.md)

## Layout

```text
cmd/moss-ffi/              CGO shared library entrypoint
cmd/moss-chat/             Native single-binary TUI chat
internal/bootstrap/        tracker clients and infohash generation
internal/transport/        encrypted sessions and handshake
internal/gossip/           pubsub cache, scoring, envelopes
internal/nat/              NAT profiling and relay primitives
internal/mesh/             runtime node orchestration
examples/                  C, C++, Python, Rust FFI examples
```

## Build

```bash
# Linux
CGO_ENABLED=1 GOOS=linux GOARCH=amd64 \
  go build -buildmode=c-shared -o libmoss.so ./cmd/moss-ffi

# Windows
CGO_ENABLED=1 GOOS=windows GOARCH=amd64 \
  go build -buildmode=c-shared -o moss.dll ./cmd/moss-ffi

# macOS
CGO_ENABLED=1 GOOS=darwin GOARCH=arm64 \
  go build -buildmode=c-shared -o libmoss.dylib ./cmd/moss-ffi
```

The generated header is emitted next to the shared library as `moss.h` or `libmoss.h`, depending on the output name.

## Test

```bash
go test ./...
```

If your environment blocks the default Go cache location, set workspace-local cache paths:

```bash
GOCACHE=$PWD/.gocache GOTMPDIR=$PWD/.gotmp go test ./...
```

## FFI API

Current exported functions:

- `Moss_Init`
- `Moss_Start`
- `Moss_Stop`
- `Moss_Subscribe`
- `Moss_Connect`
- `Moss_Unsubscribe`
- `Moss_Publish`
- `Moss_SetCallback`
- `Moss_SetEventCallback`
- `Moss_SetScoringCallback`
- `Moss_SetKeyStore`
- `Moss_GetMeshInfo`
- `Moss_GetPublicKey`
- `Moss_GetNATType`
- `Moss_Free`

See [docs/API.md](docs/API.md) for signatures, config fields, event IDs, and error codes.

## Local integration example

Two nodes on localhost can be started with one node configured as a static peer of the other:

```json
{
  "trackers": [],
  "static_peers": ["127.0.0.1:41001"],
  "listen_port": 41002
}
```

The second node connects automatically, exchanges subscriptions, and forwards published messages over the encrypted session.
