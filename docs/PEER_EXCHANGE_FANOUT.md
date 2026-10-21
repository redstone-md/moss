# Bound peer announcement forwarding

An idle node used `broadcastToAll` for each newly learned peer announcement.
A burst of catalog entries therefore multiplied by every transport connection,
despite having no application publishes or transit relay routes. A standalone
Mosh-shaped node reproduced this at `509a3f86`: 35 private subscriptions,
99,901 direct peer announcements / 33,895,669 plaintext bytes in 210 seconds.
Its largest sampled interval reached 65.42 Mbit/s in combined envelope bytes.
These measurements are not complete OS wire-byte accounting.

Forwarded peer/supernode claims now use the existing bounded control-gossip
selection. The configured `GossipSub.D` bounds recipients, eligible peers are
filtered before selection, and hashing the claim with the forwarding node's
identity avoids sending every hop into the same global subset. Verification,
the meaningful-change gate and the per-identity
forwarding cooldown remain in place. Immediate join snapshots, introduction
to a new connection and this node's own announcements retain their behavior.

The target-selection implementation is shared with telemetry gossip. Its
hash inputs, sorting, tie-break and fanout are unchanged for telemetry.
No wire fields, FFI symbols, dependencies or configured timers change.

## Relay identity does not depend on global gossip

Bounded discovery is insufficient if relay endpoints depend on receiving every
other peer's key through catalog gossip. A high-degree star exposed that hidden
dependency: setup reported success without creating an authenticated relay peer.

Relay request/accept now each send the endpoint's existing self-signed v2 peer
announcement first. The intermediary forwards that exact claim before control,
using a connection-scoped immutable copy whose signature and Noise key match
the live authenticated endpoint. It never reconstructs a claim from a directory
entry, signs on an endpoint's behalf or introduces an unsigned key.

Synchronous writes preserve local control ordering. UDP may still lose/reorder
packets; a missing key refuses setup rather than reporting readiness, and a late
authenticated claim permits retry. A failed control write releases its session
or route. Close removes the matching relayed peer, preserving a replacement or
upgraded direct session. Legacy paths with already-known keys keep working.

## Controlled receive regression

The same real signed-announcement receive path and encrypted recorded-session
fixture were run before and after the change:

| Connected recipients | Before copies / ciphertext bytes | After |
| --- | ---: | ---: |
| 1 | 1 / 358 | 1 / 358 |
| 10 | 10 / 3,580 | 6 / 2,148 |
| 64 | 64 / 22,912 | 6 / 2,148 |
| 200 | 200 / 71,600 | 6 / 2,148 |

This is a 97% reduction for one forwarded claim at 200 peers, not a forecast
of whole-application traffic. The regression failed on the old receive path
and passed on the corrected path, including 20 repeated runs. Separate checks
cover signed supernode promotion/revocation, source exclusion, filtering before
the cap and independent forwarders' target selection.

A real TCP star with more than `D` existing one-direct-slot leaves adds a late
leaf and selects an earlier leaf excluded from its announcement recipients.
The original bounded-only patch reported success without a relay peer; the
corrected setup delivers encrypted private-room publications and directed
payloads in both directions. No catalog/key is injected to make it pass.
Other regressions cover signed claim ownership, status-signature separation,
retired connections, mismatched keys, reordered acceptance and failed writes.
Backfill honors the existing peer-exchange score policy, and a full relay
refuses a new session before sending endpoint introductions. Existing direct
connections retain their admission precedence. Signature-verification tests
use authenticated key claims and working encrypted relay transports.

## Acceptance and limits

Run the full real-network suite, including private rooms, late joins, churn,
overlay lookup and delivery without a direct path. Native Mosh messaging and
call integration must also pass after updating its dependency pin.

Different public-mesh runs observe different peers and claim bursts; their
rates are indicative rather than controlled causal A/B measurements. Older
peers can still send excessive announcements to an updated node. The change
bounds its own forwarding amplification and does not claim zero idle traffic,
physical Android battery acceptance or completed background incoming calls.
