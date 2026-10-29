# Direct-peer readiness notifications

## Scope and evidence

Mosh's Windows Go probe on shipping Moss `979a634` recovered
`waitForDirectPeerContext` below offline explicit-target dials. Three such
waiters were present in the final goroutine snapshot. The implementation polls
the peer table with a fresh timer every 25 ms. Sampler totals disagree with OS
CPU counters, so this identifies a caller without predicting whole-Mosh gains.
Evidence: [run 40145525630](https://github.com/redstone-md/mosh-flutter/actions/runs/40145525630).

This experiment replaces only that readiness polling. It is stacked on PR #29,
Mosh's current `979a634`, and leaves that PR's discovery/cancellation changes
intact. The owner explicitly authorized the Moss-source experiment after the
Mosh scope rule blocked it. No public API, wire/storage format, dependency,
discovery policy, retry interval, authorization or handshake behavior changes.

A separate twenty-pair control on older main `8eb416d` does not establish median
CPU savings and increases allocations: its wait uses reusable `time.Sleep`,
whereas #29's context-aware wait creates a fresh `time.After` per poll. That
control is not an acceptable allocation optimization for the older path. Stacking
this PR on the actual context-aware baseline avoids that intermediate regression.
Results for the two baselines must not be combined into one claimed gain.

## Plan

1. Reproduce delayed readiness deterministically through real direct-peer
   registration. Use Go 1.25's existing `testing/synctest` virtual time: a new
   peer must release blocked waiters without advancing to another polling tick.
2. Keep a private, reference-counted notification per actively awaited peer,
   guarded by the same node mutex as the predicate. Direct session registration
   closes that notification under the lock. Recheck the predicate after a wake;
   a connection removed before observation must not report success.
3. Retain a single deadline timer per wait and current cancellation/final-state
   semantics. Remove notification entries after their last waiter leaves.
   Ordinary registrations without waiters allocate no notification state.
4. Test multiple waiters, unrelated/relayed peers, direct replacement, lost-wake
   races, cancellation, deadlines and lifecycle cleanup. Run focused race tests,
   normal mesh tests and the existing platform CI.
5. Compare optimized before/after test executables on one Windows runner with
   twenty alternating pairs of eight offline waits. Record OS CPU counters,
   allocations, timing and exact outcomes, with compilation outside measurements.
   Preserve immediate/direct and absent/relayed outcomes. Reprofile the shipping
   integration separately; a microbenchmark cannot prove whole-Mosh improvement.

## Opportunity and behavior proof

| Candidate | Impact | Confidence | Effort | Score |
| --- | ---: | ---: | ---: | ---: |
| Replace periodic readiness polling | 2 | 3 | 2 | 3.0 |

Confidence concerns the observed polling mechanism; deployment-level CPU benefit
is unmeasured. Accept only if the paired measurements improve beyond noise and
connection/race checks pass. Roll back with an ordinary revert if needed.

The success predicate remains a present, non-relayed peer. Notifications carry
no success result or trust: the waiter rereads the predicate. No ordering,
tie-breaking, random seeds or floating-point decisions change. An already
direct peer still succeeds, including the final check at cancellation/deadline;
an absent/relayed peer still cannot succeed. Deadline overshoot caused by 25 ms
polling is removed, without extending the caller's budget. Registration and
wait subscription share one mutex to prevent a lost wakeup.

Only active waits retain entries; identities are removed after the final wait
or direct-registration notification. Stop-bound contexts still cancel their
waits. Unbound callers retain their own deadline rather than gaining a new
node-lifecycle policy. No Mosh submodule pin, merge or release is included.
