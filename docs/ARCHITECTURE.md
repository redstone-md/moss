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
internal/bootstrap/        tracker rendezvous and infohash derivation
internal/crypto/           Moss identity and key derivation
internal/gossip/           pubsub envelopes, cache, and scoring
internal/mesh/             Node orchestration across transport, NAT, relay, gossip
internal/nat/              NAT profiling, mapping, relay primitives
internal/transport/        encrypted TCP/UDP sessions and handshakes
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
node_gossip_control.go        IHAVE/IWANT/IDONTWANT control messages
node_peer_announce.go         known-peer and supernode announcement handling
node_peer_discovery.go        discovered peer targets and topic mesh upkeep
node_relay_api.go             public relay methods
node_relay_control.go         relay request, accept, data, close, migration
node_relay_selection.go       relay candidate ranking and mesh eligibility
node_nat_control.go           binding, reachability, and hole-punch control messages
node_holepunch.go             UDP hole-punch attempts and direct peer promotion
node_reachability.go          external address and reachability probes
node_network_probe.go         address utility and probe helpers used by Node
node_maintenance.go           latency probing, pruning, housekeeping
config.go                     JSON config parsing and defaults
events.go                     host-visible event IDs and event payload helpers
errors.go                     public error code constants
```

`internal/mesh` intentionally remains one Go package because `Node` owns tightly coupled private state. Split behavior by capability files first. Create child packages only when extracted code no longer needs `Node` internals.

### `internal/transport`

`transport` owns encrypted sessions and packet flow. It exposes listener/session primitives to `mesh` and hides TCP/UDP handshake mechanics.

Important files:

```text
listener.go        TCP listener and authenticated accept path
conn.go            encrypted connection wrapper
noise.go           Noise XX/IK handshake implementation
multiplexer.go     stream multiplexing over an encrypted session
datagram.go        datagram support for session payloads
udp.go             UDP listener state and public UDP listener methods
udp_handshake.go   UDP Noise handshake packet flow
udp_observe.go     endpoint observation and STUN helpers
udp_session.go     UDP carrier and session lifecycle
```

`transport` must not know about pubsub topics, peer scoring, relay policy, or host callbacks.

Inbound queue sizing is a runtime tuning knob. Per-stream
(`multiplexer.go`) and per-UDP-session (`udp_session.go`) channels
default to 256 packets and silently drop on overflow to keep memory
predictable. `mesh.applyTransportTuning` reaches in via the package
setters `SetStreamBufferSize` and `SetUDPCarrierBufferSize` before any
session is established, so application-driven configuration (e.g. the
`transport.high_throughput` JSON option) propagates without leaking
mesh types into transport.

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

## Import Direction

Prefer one-way dependencies. The orchestration layer may depend on lower-level services, but lower-level packages must not import the orchestrator.

Allowed direction:

```text
cmd/moss-ffi
  -> internal/mesh
      -> internal/bootstrap
      -> internal/crypto
      -> internal/gossip
      -> internal/nat
      -> internal/transport
```

Rules:

- `internal/mesh` may compose all runtime services.
- `internal/transport` must not import `internal/mesh`, `internal/gossip`, or `internal/bootstrap`.
- `internal/nat` must stay independent of `internal/mesh` and `internal/gossip`.
- `internal/gossip` must not know about sockets, NAT, trackers, or FFI.
- `internal/bootstrap` must not know about `Node`, pubsub channels, relay sessions, or callbacks.
- `cmd/moss-ffi` should translate C ABI calls into `mesh.Node` methods and avoid owning protocol behavior.

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
