# Announcement signature verification

## Scope

The Mosh issue 94 follow-up found 1,825 repeated complete signed inputs among
2,084 successful peer/status announcement checks in a ninety-second native
fixture window (87.57%). Owner Windows instruction samples place 8.61% of
selected weights in Go Edwards25519 leaves; those leaves do not identify the
exact caller or predict a whole-application saving. The user authorized this
additional Moss change. Network traffic remains a separate investigation.

Memoize only successful cryptographic verification of peer announcements and
supernode status announcements/revocations. Keep the cache private and owned
by one Node, with a usable zero value. No public Go/C API, dependency, storage,
configuration, wire-format, discovery cadence or routing policy changes.

## Implementation boundary

- Hash the complete decoded public key, signature and canonical signed payload;
  public keys and signatures must retain their required fixed lengths.
- Cache only successful checks. Changed bytes and invalid signatures execute
  the normal verifier. Do not retain attacker-controlled payload buffers.
- Bound the memo to 1,024 SHA-256 keys, enough for the observed 262 unique
  inputs without giving arbitrary announcements unbounded memory. Allocate
  storage only after a successful check; use FIFO eviction, not a time cache.
- Synchronize lookup/insertion, leaving expensive verification outside the lock.
  Concurrent misses may verify the same input; no packet waits on an in-flight
  crypto result owned by another peer.
- A hit proves the same immutable cryptographic fact. Every envelope still
  executes sender trust, address/NAT/capability updates, admission, backoff and
  forwarding decisions. Do not cache an entire handler result.
- Keep hole-punch, relay, publish and handshake verification outside this memo.

## Proof and checks

1. Introduce a behavior-preserving verification boundary; prove a repeated
   check still invokes real crypto twice with a failing regression assertion.
2. Cover complete key/payload/signature identity, negative results, bounded
   eviction, zero-value operation and concurrent access. Exercise real handler
   state changes on warm entries, including third-party versus direct trust,
   malformed Noise keys and changed signed capability claims.
3. Compare stable handler-state/message goldens between the uncached and cached
   boundaries. Existing legacy v1/v2, poisoning and relay-identity tests remain.
4. Run full Go tests, focused race/vet, formatting and changed-line coverage;
   run Linux and Windows CI and rebuild the native shared library.
5. Run at least twenty alternating same-host pairs of optimized Go executables,
   built before measurement, with identical deterministic signed fixtures.
   Include repeated, unique and invalid controls; check outputs on both sides.
6. Review the immutable patch against this spec and repository standards, then
   inspect CodeAnt findings before pinning the runtime in Mosh and packaging.

Ordering and forwarding remain in the existing handler. Cache eviction changes
only the number of pure signature checks. No time, RNG or floating-point
decision is cached. A cache hit does not grant peer trust or freshness and does
not remove network packets. A change in the measured component is not a claim
about whole-Mosh CPU or bandwidth. Opportunity score: impact 2 × confidence 4
/ effort 3 = 2.67. Roll back the implementation commit to restore uncached
verification through the same private boundary.

Existing size exceptions: `node_types.go` and `node_peer_announce.go` already
exceed the host's file/type/function limits. This change adds one private Node
field and replaces two verifier calls; an unrelated Node/handler split is not
part of this optimization. New modules and tests remain within the limits.
