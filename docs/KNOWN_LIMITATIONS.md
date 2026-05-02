# Known Limitations

This document exists to keep repository claims aligned with current runtime behavior.

## Runtime and networking

- NAT traversal is implemented pragmatically, but not every real-world NAT topology has the same success rate.
- Direct UDP paths are more sensitive to packet loss and jitter than relay fallback paths.
- Relay and NAT behavior are covered by tests, but large-scale public-network validation is still more limited than pure unit coverage.

## Specification parity

- The repository tracks [docs/SPECIFICATION.md](SPECIFICATION.md) with a practical v1 implementation.
- Some production-grade goals from the specification are only partially proven by automated tests and not by broad public telemetry.

## Performance and scale

- CI validates correctness first; it is not a substitute for sustained long-running load tests.
- Benchmarks exist, but public support guarantees for every network environment are intentionally conservative.
- Default per-session inbound buffers (256 packets) target low-rate
  chat/discovery workloads. Sustained bursts above ~1k packets/sec per
  peer can hit the silent-drop threshold of the receive queues; opt
  into `transport.high_throughput` (see [API.md](API.md)) to raise
  buffers and skip gossip control overhead for streaming use cases.

## Client applications

- `MOSS` is the runtime and shared-library repository.
- Desktop chat UX now lives in [MOSH](https://github.com/redstone-md/mosh), which evolves separately.
