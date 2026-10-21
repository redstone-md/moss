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
filtered before selection, and hashing the claim rotates neighbors when the
claim changes. Verification, the meaningful-change gate and the per-identity
forwarding cooldown remain in place. Immediate join snapshots, introduction
to a new connection and this node's own announcements retain their behavior.

The target-selection implementation is shared with telemetry gossip. Its
hash inputs, sorting, tie-break and fanout are unchanged for telemetry.
No wire fields, FFI symbols, dependencies or configured timers change.

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
the cap and stable/rotating target selection.

## Acceptance and limits

Run the full real-network suite, including private rooms, late joins, churn,
overlay lookup and delivery without a direct path. Native Mosh messaging and
call integration must also pass after updating its dependency pin.

Different public-mesh runs observe different peers and claim bursts; their
rates are indicative rather than controlled causal A/B measurements. Older
peers can still send excessive announcements to an updated node. The change
bounds its own forwarding amplification and does not claim zero idle traffic,
physical Android battery acceptance or completed background incoming calls.
