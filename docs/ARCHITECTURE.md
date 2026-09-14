# Moss Architecture Guide

Moss is an embeddable peer-to-peer mesh runtime. The core is written in Go and exported through a C-compatible shared-library boundary so host applications can use the same networking layer from C, C++, C#, Python, Rust, or any runtime with C FFI support.

This guide is the project map. Use it to understand where behavior lives, how data moves through the runtime, and where a new change should be made.

## System At A Glance

Moss has five runtime responsibilities:

- discover peers for a mesh ID through tracker rendezvous and static peers
- authenticate and encrypt peer sessions with Noise and stable Moss identities
- route application messages through topic-based pub/sub
- improve connectivity through NAT profiling, hole punching, and relay fallback
- expose a small C ABI that host applications can call safely

The Go core is not a UI application. Examples demonstrate integration, but protocol behavior belongs in `internal/`.

## Runtime Flow

A typical host application follows this path:

```text
Host app
  -> Moss_Init(mesh_id, psk, config_json)
      -> mesh.NewNode(...)
          -> bootstrap manager setup
          -> transport listener setup
          -> gossip manager setup
          -> NAT profiler and relay state setup
  -> Moss_SetCallback / Moss_SetEventCallback
  -> Moss_Start
      -> TCP and UDP accept loops
      -> tracker announce loop
      -> static peer dials
      -> NAT and reachability probing
      -> pubsub dispatch loop
      -> maintenance loop
  -> Moss_Subscribe(channel)
  -> Moss_Publish(channel, payload)
      -> local envelope creation
      -> gossip cache and scoring checks
      -> mesh broadcast / flood publish
      -> host callback delivery on receiving peers
  -> Moss_Stop
      -> listener shutdown
      -> peer/session cleanup
      -> goroutine wait
```

The public C ABI is intentionally narrow. Most behavior should be reachable through `mesh.Node` methods before it is exposed through `cmd/moss-ffi`.

## Repository Layout

```text
cmd/moss-ffi/              C shared-library adapter and exported ABI
cmd/moss-gateway/          read-only telemetry gateway binary (deprecated by moss-scope serve)
cmd/moss-scope/            MossScope: single-binary site bundle + telemetry relay + server
cmd/moss-signal/           WebRTC signaling relay binary
cmd/moss-wasm/             wasm verifier build for the site explorer
cmd/moss-node-wasm/        node built for GOOS=js (browser/runtime targets)
internal/bootstrap/        tracker rendezvous, DHT source, and infohash derivation
internal/crypto/           Moss identity and key derivation
internal/geo/              embedded GeoLite2 country lookup for relay-distance ranking
internal/gossip/           pubsub envelopes, cache, and scoring
internal/inspect/          debug plane: event bus, WebSocket server, ring recorder
internal/mesh/             Node orchestration across transport, NAT, relay, gossip
internal/nat/              NAT profiling, mapping, relay primitives
internal/observe/          client-side telemetry verification (wasm-safe, pure)
internal/overlay/          Kademlia-style discovery layer (routing table, records)
internal/stat/             privacy-preserving telemetry aggregation (HLL, DP, hash chain)
internal/telemetry/         opt-in Axiom event sink (inert without a token)
internal/transport/        encrypted TCP/UDP sessions and handshakes
internal/tun/              virtual intranet: packet router and TFRG fragmentation
internal/webui/            go:embed bundle so moss-scope serves with no files on disk
examples/                  host-language FFI examples
docs/                      API, integration, architecture, and specification docs
```

## Package Responsibilities

### `cmd/moss-ffi`

The FFI package is the shared-library adapter. It owns handle registries, C memory ownership, callback registration, return-code translation, and exported `//export` functions.

It should stay thin. Do not implement peer selection, pubsub routing, NAT probing, or relay policy here.

### `internal/mesh`

`mesh` is the runtime coordinator. `Node` composes trackers, transport sessions, gossip state, NAT probing, relay sessions, callbacks, and lifecycle management.

Important files:
```text
node_types.go                 private Node state and closely related structs
node_lifecycle.go             construction, start/stop, public Node API
node_accept.go                inbound/outbound peer connection lifecycle
node_advertise.go             local address and announce-port selection
node_direct_connect.go        direct peer dial policy and known-peer address ranking
node_dispatch_bootstrap.go    host callback dispatch and bootstrap coordination
node_envelope.go              local delivery, broadcast, and flood publish
node_gossip_control.go        IHAVE/IWANT/IDONTWANT control messages and per-peer outbound queues
node_peer_announce.go         known-peer and supernode announcement handling
node_peer_discovery.go        discovered peer targets, dial backoff, and topic mesh upkeep
node_relay_api.go             public relay methods, directed payloads (SendToPeer), packet callbacks
node_relay_control.go         relay request, accept, data, close, migration
node_relay_selection.go        relay candidate ranking and mesh eligibility
node_relay_transport.go       relay session transport integration
node_nat_control.go           binding, reachability, and hole-punch control messages
node_holepunch.go             UDP hole-punch attempts and direct peer promotion
node_reachability.go          external address and reachability probes
node_network_probe.go         address utility and probe helpers used by Node
node_maintenance.go           latency probing, pruning, housekeeping
node_overlay.go               Kademlia overlay orchestration (lookups, STORE, republish)
node_room.go                  room keys, topic sealing, room invites
node_veil.go                  Veil "Reality" DPI-masked bearer (non-js builds)
node_game.go                  game preset: binary snapshot codec, latest-sequence-wins filter
node_stat.go                  per-epoch telemetry contribution to the stat CRDT
node_telemetry.go             telemetry wiring and event forwarding
node_telemetry_nat.go         NAT telemetry snapshots for the explorer
node_tun_bind.go              intranet binding: virtual-IP registry, packet pump over SendToPeer
node_explicit_targets.go      ConnectToPeer registration and explicit dial scheduling
config.go                     JSON config parsing and defaults
events.go                     host-visible event IDs and event payload helpers
errors.go                     public error code constants
peer_cache.go                 persisted known-good peers for fast cold start
lan_discovery.go              multicast LAN beacon discovery
dht.go                        BitTorrent mainline DHT peer source
reachability.go               probe orchestration for external reachability
supernode_signing.go          signed supernode announcements
relay_signing.go              relay envelope signatures
```

### `internal/transport`

`transport` owns encrypted sessions and packet flow. It exposes listener/session primitives to `mesh` and hides TCP/UDP handshake mechanics.

Important files:

```text
listener.go        TCP listener and authenticated accept path
conn.go            encrypted connection wrapper
noise.go           Noise XX/IK handshake implementation, identity payloads
multiplexer.go     stream multiplexing over an encrypted session
stream.go          stream frame path (256 KiB max data frame)
datagram.go        datagram support for session payloads
udp.go             UDP listener state and public UDP listener methods
udp_handshake.go   UDP Noise handshake packet flow
udp_observe.go     endpoint observation and STUN helpers
udp_session.go     UDP carrier and session lifecycle
obfs.go            DPI-scramble codec for UDP carriers
bind.go            listener address/binding selection and platform variants
webrtc_js.go       WebRTC transport surface (js builds)
```

`transport` must not know about pubsub topics, peer scoring, relay policy, or host callbacks.

Inbound queue sizing is a runtime tuning knob. Per-stream
(`multiplexer.go`) and per-UDP-session (`udp_session.go`) channels
default to 256 packets and silently drop on overflow to keep memory
predictable. `mesh.transportBufferConfig` maps application-driven
configuration (e.g. the `transport.high_throughput` JSON option) into
`transport.BufferConfig`, which is attached to listeners and sessions so
tuned nodes do not mutate process-wide transport defaults.

### `internal/bootstrap`

`bootstrap` owns tracker rendezvous and infohash generation. It knows how to announce to UDP/HTTP BitTorrent trackers and return candidate peer addresses.

It should not know about `Node`, relay sessions, pubsub channels, or NAT strategy.

### `internal/gossip`

`gossip` owns pubsub data structures: envelopes, cache, mesh membership, scoring, and control message types. It is protocol logic, not socket logic.

It should not import transport, NAT, bootstrap, or FFI code.

### `internal/nat`

`nat` owns NAT classification, mapping helpers, relay primitives, token buckets, and relay session bookkeeping. It provides reusable building blocks; `mesh.Node` decides how to orchestrate them.

### `internal/crypto`

`crypto` owns Moss identities and key derivation. Keep key generation, serialization, signing, and HKDF logic here instead of spreading crypto details across callers.

### `internal/overlay`

`overlay` implements the Kademlia-style discovery layer: the routing table (XOR-distance buckets), the record store (channel/peer rendezvous), and closest-contact queries. It is a lookup layer, not a packet-routing layer — data always flows A → core → B in two hops, so the overlay only answers "who holds/can reach what". Only publicly reachable nodes hold buckets and answer queries; NAT'd nodes are full clients but never hops. See `mesh/node_overlay.go` for the orchestration side.

### `internal/tun`

`tun` implements the virtual-intranet data plane: a packet router over virtual IPs, IPv4 classification, and the v2 TFRG fragmentation of packets between the MTU and the 64KB directed-payload cap. It knows nothing of `mesh` — `Node` supplies the send function (`SendToPeer`) and the packet callback; the binding glue lives in `mesh/node_tun_bind.go`.

### `internal/inspect`

`inspect` is the debug plane: an event bus whose emit cost is one atomic load when nobody listens, a ring recorder that keeps recent history even with no subscriber attached, and the loopback HTTP/WebSocket server the MossScope UI reads. Mesh emits lifecycle, dial, punch, drop, and session events into it via `mesh/node_debug*.go`; it is enabled only with `debug.enabled`.

### `internal/observe`

`observe` provides the pure, client-side primitives a network explorer needs to TRUST telemetry it did not produce: hash-chain continuity verification, cross-gateway agreement, and deterministic topology simulation from aggregate statistics. It is deliberately free of sockets, time, and randomness so it compiles unchanged to GOOS=js/wasm and verifies what it renders in the browser.

### `internal/stat`

`stat` is the privacy-preserving telemetry aggregation layer: per-epoch unlinkable node IDs (BLAKE2s over epoch and pubkey), HyperLogLog node counting, differential-privacy noise, per-node bandwidth/degree clamps, and the hash-chained epoch snapshots gossiped between nodes. `mesh/node_stat.go` feeds it; the layer is inert until `telemetry.enabled`.

### `internal/telemetry`

`telemetry` is the opt-in, best-effort Axiom event sink: structured errors and host logs shipped asynchronously, drop-on-full by design. Entirely inert unless a host enables it with a token — a moss node never phones home on its own.

### `internal/geo`

`geo` maps peer IPs to coarse country/continent via the embedded GeoLite2-Country database, so relay selection can prefer a relay close to the peer it must reach. Offline, no runtime downloads.

### `internal/webui`

`webui` embeds the built MossScope bundle via `go:embed` so `moss-scope` serves the whole interface from one binary with no files on disk. `dist` is filled by `make scope`; when empty the handler serves the committed placeholder, keeping `go build ./...` working without Node installed.

## Import Direction

Prefer one-way dependencies. The orchestration layer may depend on lower-level services, but lower-level packages must not import the orchestrator.

Allowed direction:

```text
cmd/moss-ffi
  -> internal/mesh
      -> internal/bootstrap
      -> internal/crypto
      -> internal/geo
      -> internal/gossip
      -> internal/nat
      -> internal/overlay
      -> internal/stat
      -> internal/telemetry
      -> internal/transport
      -> internal/tun (send function injected, never imported)
```

Periphery (no mesh dependency, wired from cmd/* or the browser):

```text
cmd/moss-scope -> internal/inspect, internal/observe, internal/webui
internal/inspect <- mesh (mesh imports inspect: event bus emit)
internal/observe, internal/webui, internal/telemetry: leaf packages, no internal deps
```

Rules:

- `internal/mesh` may compose all runtime services.
- `internal/transport` must not import `internal/mesh`, `internal/gossip`, or `internal/bootstrap`.
- `internal/nat` must stay independent of `internal/mesh` and `internal/gossip`.
- `internal/gossip` must not know about sockets, NAT, trackers, or FFI.
- `internal/bootstrap` must not know about `Node`, pubsub channels, relay sessions, or callbacks.
- `cmd/moss-ffi` should translate C ABI calls into `mesh.Node` methods and avoid owning protocol behavior.

- `internal/overlay` must stay a pure discovery structure; `mesh` orchestrates lookups and never lets it dial or own sockets.
- `internal/tun` must not import `internal/mesh`; the binding injects the send function and packet callback instead.
- `internal/stat` and `internal/observe` must not know about sockets or `Node`; the former is fed by `mesh`, the latter is pure/wasm-safe.
- `internal/inspect` may be imported by `mesh` (event emit) but must never import it back.

If a new dependency points upward, pass data or a narrow callback down instead.

## State And Concurrency Model

`mesh.Node` owns runtime state behind mutexes and lifecycle goroutines. The common pattern is:

- public methods validate inputs and enqueue work or call private helpers
- network loops read from transport sessions and hand envelopes to mesh handlers
- dispatch callbacks isolate host calls from protocol loops
- maintenance loops handle periodic probing, pruning, and mesh upkeep
- shutdown cancels the root context, closes listeners/sessions, and waits for goroutines

When adding state, prefer keeping ownership local to the package that maintains the invariant. If the state coordinates multiple subsystems, it likely belongs on `Node`; if it is reusable protocol state, it likely belongs in `gossip`, `nat`, `transport`, or `bootstrap`.

## Message And Event Flow

Application payloads enter through `Moss_Publish` or `Node.Publish`. `mesh` wraps the payload in a gossip envelope, stores it in the cache, delivers it locally where appropriate, then sends it to eligible peers.

Incoming peer messages follow the reverse path:

```text
transport.Session
  -> peer read loop
  -> node envelope handler
  -> gossip validation/cache/scoring
  -> local callback dispatch or relay/control handling
```

Host-visible events are emitted from mesh lifecycle and network transitions. Keep event payloads stable because they cross the FFI boundary and are consumed by host applications.

## FFI Memory Ownership

The host owns inputs passed into Moss. Moss owns strings and buffers returned by functions such as `Moss_GetMeshInfo`, `Moss_GetPublicKey`, and `Moss_GetNATType`; hosts must release those with `Moss_Free`.

Keystore callbacks are global host-provided persistence hooks used during node initialization. Keep callback objects alive on the host side for as long as Moss may call them.

See [API.md](./API.md) and [SHARED_INTEGRATION.md](./SHARED_INTEGRATION.md) for exact signatures and integration rules.

## Examples

Examples are host integration references, not alternate runtimes. They should show realistic FFI usage while keeping protocol behavior in Go.

Python chat module responsibilities:

```text
moss_chat.py             compatibility entrypoint and re-export surface
moss_chat_native.py      ctypes bindings, constants, shared-library loading
moss_chat_identity.py    private identity file persistence
moss_chat_client.py      Python wrapper around the Moss FFI handle
moss_chat_format.py      parsing, formatting, room names, payload rendering
moss_chat_cli.py         command-line argument parsing and app bootstrapping
moss_chat_app.py         prompt-toolkit chat UI and command handling
```

If the Python chat example grows substantially, it can become a package with `domain/`, `infra/`, and `ui/` folders. Do that only when it improves navigation or testability.

## Tests

Test files should be grouped by behavior, not by implementation file.

Useful test areas:

```text
cmd/moss-ffi/*_test.go                 shared-library and exported API behavior
internal/bootstrap/*_test.go           tracker and infohash behavior
internal/crypto/*_test.go              identity and key derivation
internal/gossip/*_test.go              cache, pubsub, scoring, fuzzing
internal/mesh/*_test.go                node behavior, integration scenarios, benchmarks
internal/nat/*_test.go                 NAT profiling, relay primitives, mapping helpers
internal/transport/*_test.go           Noise, TCP/UDP sessions, multiplexing
examples/python_chat/test_moss_chat.py Python integration wrapper behavior
internal/overlay/*_test.go            routing table, record store, top-k queries
internal/stat/*_test.go               EID rotation, HLL merge, DP noise, chain windows
internal/observe/*_test.go            chain verification and simulation (wasm-safe)
internal/inspect/*_test.go            bus emit/drop, WebSocket session, recorder
internal/tun/*_test.go                router, IPv4 classification, TFRG fragmentation
internal/geo/*_test.go                embedded database lookups
```

For broad changes, run:

```bash
go test ./...
python -m unittest discover -s examples/python_chat -p 'test_*.py'
```

For fast compile checks on mesh or transport changes:

```bash
go test ./internal/mesh -run '^$'
go test ./internal/transport -run '^$'
```

### CI gates (see .github/workflows/ci-dev.yml)

Per push (dev):

```bash
go test ./... -count=1                        # full suite, both OSes
go test -race -count=1 ./internal/gossip ./internal/transport ./internal/crypto ./internal/bootstrap ./internal/nat
go test ./internal/mesh -count=1             # strict: a flake stays red
go test ./internal/mesh -count=3             # soak: repetition, not retries
go test ./internal/mesh -count=3 -run 'Test(TwentyFiveNode|RelayBurst|MixedTopology|StarTopology|TwelveNodeStar)'  # sustained load soak
go test ./internal/mesh -run '^$' -bench BenchmarkTwoHundredPeerSteadyStateMemory -benchtime=1x  # heap gate: fail > baseline+20%
```

Nightly (schedule + manual dispatch):

```bash
go test -race -count=1 -timeout 3600s ./internal/mesh   # mesh race sweep
```

Local equivalents: `make test-fast`, `make test-race`, `make test-race-mesh`, `make soak` (SOAK_WINDOW_SEC adjusts the window), `make memory-gate`.

## Adding Or Changing Behavior

Use these entry points:

- new C ABI function: start in `mesh.Node`, then add the adapter in `cmd/moss-ffi`, then update `docs/API.md`
- new node lifecycle behavior: `internal/mesh/node_lifecycle.go` or a focused `node_<capability>.go`
- new gossip behavior: `internal/gossip`, then orchestration in `internal/mesh`
- new transport packet flow: `internal/transport`, then integration in `internal/mesh`
- new NAT or relay primitive: `internal/nat`, then policy/orchestration in `internal/mesh`
- new host integration example: `examples/<language>_example/` or `examples/python_chat/`
- new config field: `internal/mesh/config.go`, API docs, and at least one config test

Keep changes tracer-bullet shaped: prove the end-to-end path with a narrow test before expanding the surface.

## Package Extraction Rules

Create a new Go package only when it has a stable responsibility and can hide its own state behind a small API.

Good candidates:

- pure helpers used by multiple packages
- reusable policy engines with simple inputs and outputs
- protocol codecs that do not need `Node` internals
- host-facing adapters that translate one boundary into another

Bad candidates:

- folders created only to reduce visible file count
- packages that require exported access to `Node.mu`, peer maps, relay maps, or callback fields
- packages with interfaces that have only one implementation and exist only to work around a directory move
- circular dependency workarounds

Potential future extracts, if pressure appears:

```text
internal/netaddr/          advertise host selection and address classification
internal/relaypolicy/     relay candidate scoring if it becomes pure policy
internal/peerid/          peer ID parsing/formatting if reused outside mesh
```

Do not extract these until they remove real duplication or let a package become independently testable.

## Documentation Map

- [SPECIFICATION.md](./SPECIFICATION.md): product and protocol specification
- [SPECIFICATION-OPERATIONS.md](./SPECIFICATION-OPERATIONS.md): operational details and milestones
- [API.md](./API.md): exported C ABI reference
- [SHARED_INTEGRATION.md](./SHARED_INTEGRATION.md): host integration guide
- [SHARED_INTEGRATION-ADVANCED.md](./SHARED_INTEGRATION-ADVANCED.md): advanced integration notes
- [KNOWN_LIMITATIONS.md](./KNOWN_LIMITATIONS.md): current runtime limitations
