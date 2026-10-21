# Topic refusal and retry ownership

The pub/sub manager owns PRUNE backoff per peer and topic. Both received
refusals and local excess-mesh pruning use the existing backoff: 30 seconds,
or a longer configured heartbeat.
A recent speculative GRAFT does not shorten it. A refusal on one topic does
not exclude the peer from other topics, or clear their refusals.

A first positive subscription claim admits a late subscriber immediately.
Repeating a claim does not override a newer refusal. Peer-wide health/score
exclusion remains separate and cannot be cleared by subscription traffic.
Moss uses GRAFT as its subscription claim; no wire fields or ABI change here.
This follows the topic-scoped refusal pattern in
[Gossipsub](https://github.com/libp2p/specs/blob/master/pubsub/gossipsub/gossipsub-v1.1.md).

Only locally subscribed topics retain refusal state. Unsubscribe clears the
topic's refusal and send markers; disconnect clears the peer's state.
Queued control messages and delayed send bookkeeping check the connection's
identity, synchronized with peer removal, before mutating state. A retired
connection cannot recreate state or throttle its replacement.

Checks cover refusal spacing, independent topics, first/repeated claims,
candidate selection, health exclusion, expiry, unsubscribe/resubscribe and
retired/replaced connections. Existing real-network tests cover late peers,
simultaneous subscriptions, cached delivery, churn and mesh repair.

The Mosh owner trace contained 85,272 refusals across 35 topics in 192 seconds.
In an isolated encrypted-session fixture with 35 topics and one refusing peer,
8.5 seconds produced 175 outbound GRAFTs before the fix and 35 afterward:
9,975 versus 1,995 ciphertext bytes. This demonstrates reduced control churn,
not an equivalent percentage reduction in full application network use.

The fix is based on the `main` revision Mosh ships. `CONTRIBUTING.md` defaults
to `dev` for active development; this focused stable maintenance branch avoids
importing its unrelated divergent changes. Existing large mesh source files
retain their established boundaries; new helper/test files stay below 400 lines.
