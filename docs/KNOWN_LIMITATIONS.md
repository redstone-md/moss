# Known Limitations

This document exists to keep repository claims aligned with current runtime behavior.

## Runtime and networking

- NAT traversal is implemented pragmatically, but not every real-world NAT topology has the same success rate.
- Direct UDP paths are more sensitive to packet loss and jitter than relay fallback paths.
- Relay and NAT behavior are covered by tests, but large-scale public-network validation is still more limited than pure unit coverage.

### Reconnect and redial timing

- After a failed dial, a peer is retried with exponential backoff (base
  `security.handshake_timeout_sec`, default 5s, doubling per consecutive
  failure, capped at 5 minutes) spaced across maintenance passes every ~3s.
  The practical floor for noticing a vanished path and re-dialing it is
  therefore ~3 seconds even on loopback-tast networks; a peer whose
  handshake itself times out costs the full `handshake_timeout_sec` per
  attempt before the retry is even scheduled.
- A session that dies on missed pings (six misses at the probe interval)
  is NOT redialled immediately — the failure counts against the backoff so
  the flap loop cannot repeat. Players see this as a lobby rejoin taking
  noticeably longer than the first join.
- The mesh-block cooldown from an inbound PRUNE has two tiers: ~2s when the
  PRUNE answers our own recent GRAFT (join choreography), 30s otherwise
  (`meshGraftRetryInterval`). A peer that refuses grafts stays locked out
  of topic mesh formation for that window, and GRAFT retries to ANY peer
  are throttled to once per 30s per channel.

### Publish and delivery semantics

- `Moss_Publish` returning `MOSS_OK` means the envelope was accepted and
  enqueued/broadcast to at least one peer — NOT that any remote peer
  received or will receive it. Enqueues are non-blocking by design: a peer
  whose outbound queue is full silently costs one redundant copy (gossip
  re-announce covers it later), and `MOSS_OK` is still returned.
- With zero mesh peers and zero subscribers on the topic, Publish returns
  `MOSS_ERR_NO_PEERS`; with at least one eligible target it returns OK even
  if that target's queue then drops the copy.
- The serve side of IWANT (answering a peer's request for cached payloads)
  is capped: at most 64 payload serves per request and one serve per
  message ID per peer per 10s. A large IHAVE fan-in that legitimately asks
  for more than 64 fresh messages in one window will be partially served;
  the rest must wait for the next cooldown window.

### Allowlist and transit behavior

- `Moss_AllowPeer` switches the node to strict admission on the FIRST call:
  configure the full set before/alongside Start. `Moss_DisallowPeer`
  removing the LAST entry does NOT re-enable open-substrate mode — an
  empty-but-present allowlist rejects everyone.
- The allowlist gates NEW registrations only, on every bearer (direct and
  relayed). An already-registered peer stays connected until it drops or
  `DisallowPeer` tears the session down; enabling strict mode does not kick
  existing peers.
- Relayed transit: a relayed session counts toward the relayed fan-out cap,
  and a refusal at the target (capacity or allowlist) tears the whole relay
  session down — the opener gets an honest failure, not a half-open session.
  Transit through a relay is available only when a relay-capable
  (public-reachable) peer is connected; there is no infrastructure relay.

### Directed payloads (DMs) and rooms

- `Moss_SendToPeer` to a peer with no live session falls back to the relay
  path with a 5-second budget; a DM therefore requires either a direct
  session or a reachable relay-capable peer. There is no store-and-forward:
  an offline peer misses the DM entirely.
- Receiving DMs in the unified sink requires `Moss_SetPacketCallback`;
  without it, relayed payloads fall back to the legacy relay callback and
  direct TypeDirect payloads are not delivered to any application callback
  at all.
- Room-invite flows require both sides on a DM-capable path (above); the
  invite envelope itself is sent like a DM.

### TUN fragmentation edge cases

- Outbound IP packets between the MTU and the 64KB hard cap are fragmented
  (v2 "TFRG" frames) over the directed path; packets over the hard cap are
  dropped and counted, never sent. The hard cap matches
  `security.max_message_size_bytes` — lowering that config below 64KB
  makes some fragmentable packets undeliverable.
- Reassembly state is per (sender, fragID): two peers colliding on one
  32-bit fragID inside the 5s TTL window can corrupt each other's pending
  assembly (bounded by the 64-entry pending cap and per-assembly size cap).
- A coincidental application payload starting with the "TFRG" magic is
  indistinguishable from a fragment frame — an application whose payloads
  may start with those bytes must not share the packet callback with an
  attached intranet. Same residual-ambiguity caveat as IPv4's 0x4X
  classification.

## Specification parity

- The repository tracks [docs/SPECIFICATION.md](SPECIFICATION.md) with a practical v1 implementation.
- Some production-grade goals from the specification are only partially proven by automated tests and not by broad public telemetry.
- The 100-node load target in the spec's performance table is NOT exercised
  by any current CI stage: no Kubernetes cluster or container fleet is
  provisioned by the repository (see "Testing scale" below). Largest
  automated topologies are 25 nodes (loopback) and 201 nodes (memory
  footprint gate).

## Testing scale

- CI validates correctness first; it is not a substitute for sustained long-running load tests.
- The nightly mesh `-race` sweep and the sustained load soak
  (`TestTwentyFiveNodeLoadSoakSustainsPublishing`, minutes-long window)
  exist precisely because per-push CI stays fast; a failure there is a real
  finding, not a flake to re-roll.
- Infrastructure-backed scenarios (Docker + `tc` latency/loss simulation,
  `mininet` topologies, Kubernetes fleets, AFL) described in
  [SPECIFICATION-OPERATIONS.md](SPECIFICATION-OPERATIONS.md) §10 are
  planned tooling, not present repository capabilities. No 100-node test
  stand exists today — this is a known gap, tracked as planned work.

## Performance and scale

- Benchmarks exist, but public support guarantees for every network environment are intentionally conservative.
- The steady-state heap gate (201 nodes, ~80MB baseline, fail at +20%) is
  CI-runner-specific: allocator behavior varies by platform, so the
  baseline may need re-anchoring (MOSS_HEAP_BASELINE_MB) when the runner
  image changes — never widen the growth margin to make a regression pass.
- Default per-session inbound buffers (256 packets) target low-rate
  chat/discovery workloads. Sustained bursts above ~1k packets/sec per
  peer can hit the silent-drop threshold of the receive queues; opt
  into `transport.high_throughput` (see [API.md](API.md)) to raise
  buffers and skip gossip control overhead for streaming use cases.

## Client applications

- `MOSS` is the runtime and shared-library repository.
- Desktop chat UX now lives in [MOSH](https://github.com/redstone-md/mosh), which evolves separately.
