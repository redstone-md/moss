# Opt-in waiting lifecycle

Status: unmerged implementation; upstream review and host/device acceptance
are pending. The existing single-hop relay protocol cannot deliver arbitrary traffic between
waiting leaves on different carriers. A live carrier alone is not evidence of
a route to an application peer. Mosh must select active for outgoing user work;
the waiting recipient can reply through the resulting relay session.

Waiting prefers an already authenticated stream carrier, then attempts a stream
upgrade of an existing UDP attachment. TCP/Veil carries application envelopes
without relying on IP fragmentation; small UDP keepalives alone do not prove
that a path carries application-sized frames. UDP-only relays keep their
existing best-effort datagram behavior when no stream is reachable. Large relay
frames on either endpoint also try that existing stream transport, including an
ordinary desktop sender. The upgrade is serialized per physical carrier;
failures retain UDP and suppress another probe on that carrier for one minute.
This changes relay data transport only, not ordinary discovery or mesh policy.

A first accepted topic subscription receives one reciprocal claim. This lets a
new room participant exchange traffic immediately when known-recipient hints
had suppressed a speculative introduction; repeated claims do not echo, and
topic refusals and peer health exclusions still control admission.

Topic interest and mesh admission are separate. A healthy connected peer with a
mildly negative IP-colocation score can receive our bounded subscription claim
without becoming a mesh member. Score-only mesh backoff is kept separately
from health backoff; maintenance must not turn P6 into a health exclusion that
suppresses the claim again. Both backoffs still prevent mesh admission.
This prevents a small household cohort from
losing all subscriber routes before its first group-admission packet. The
non-negative mesh baseline, existing gossip floor, room-discovery constraints,
PRUNE backoff and health exclusions remain unchanged. This correction also
applies to ordinary active consumers; it does not whitelist public IPs or
weaken P6.

The [GossipSub specification](https://github.com/libp2p/specs/blob/master/pubsub/gossipsub/gossipsub-v1.1.md)
separates mesh, gossip and own-publish score thresholds;
[base PubSub](https://github.com/libp2p/specs/blob/master/pubsub/README.md) also
separates subscription controls from content. Moss uses its existing bounded
GRAFT as both an interest claim and a mesh request. It still uses the gossip
floor (-10) for interest and own flood publishing, rather than the separately
declared publish floor (-100). This patch does not change that restriction or
guarantee arbitrary numbers of installations behind one public IP.

A waiting node refuses unrelated direct UDP peers before sending a mesh
confirmation probe. A rejected upgrade must not make its initiator retire
a working relayed path. Final peer admission revalidates the same policy.

An outbound TCP replacement of an existing virtual peer or datagram carrier
also waits for an ordinary authenticated mesh packet before retiring the old
path. A carrier refusing the TCP session must leave its working datagram path
and virtual routes intact. Noise
alone can complete before the remote refuses admission. Existing ping probes
and handshake deadlines supply this confirmation; the first packet is replayed
to its normal handler. A permitted cold Waiting candidate also requires this
fresh mesh admission; cached signed capability and Noise alone can otherwise
report ready immediately before a remote refusal closes the connection.
Ordinary cold direct startup is unchanged. Cancellation, Stop and current
admission policy are rechecked before replacement.

Waiting is a policy for the embedding application to select automatically when
it has no interactive work, accepted call or user-started transfer. It is not a
new end-user setting. Every node starts active; Bastion, games and other hosts
that never call the setter retain their existing discovery, forwarding and
maintenance policy.

The Go API is `Node.SetActivityMode(ActivityActive|ActivityWaiting)`; the optional
C symbol is `Moss_SetActivityMode(handle, 0|1)`. Success accepts a desired mode,
not network availability. Duplicate requests do no additional work. Invalid
values return error `-15`, an unstarted node `-3`, an invalid C handle `-1`.
Mode changes serialize with Stop. Stop still invalidates the C handle and is
never the implementation of a policy change.

A valid setter call opts that node into the lifecycle, including an initial
active request. Opted-in nodes prefer foreground identity resolution when an
explicit target has no direct address or its direct path was refused. A healthy
known direct path retains its existing startup behavior.

An ordinary host also refreshes one explicit target's identity after losing
that target's current established relay. The recipient may now be waiting on a
different carrier and refuse direct physical sessions after Noise succeeds.
Those speculative admissions and restoration of old local relay state do not
consume recovery; fresh authenticated relay acceptance or a direct session
with actual inbound traffic does. Existing per-target retry
coalescing, deadlines and backoff still apply. Unregistered peers, pending or
replaced relay sessions and ordinary direct-only targets retain their original
routing order. This does not opt the host into lifecycle or change discovery.

Active virtual peers also retire at the existing unanswered end-to-end probe
limit. They have no physical socket to close: retirement removes only the
matching failed relay-session incarnation and lets an ordinary explicit target
refresh its identity route, while retaining its healthy carrier. A stale probe
cannot prune or retire a newer peer incarnation. Waiting virtual peers still
rely on carrier liveness and do not start per-conversation probes.

Closing a current relay incarnation restores another already-established
session to that peer if its authenticated physical carrier remains admitted.
The closing session is excluded; current allowlist policy is checked again,
and application interest, subscriptions and scoring survive replacement.
Stale closes cannot replace a newer incarnation. This also corrects ordinary
active callers, without opting them into Waiting. It does not retransmit data
already sent into a concurrently closing route or make queue acceptance a
delivery acknowledgement.

One Waiting repair can try at most three already-known candidates sequentially
inside its existing handshake deadline. Cold candidates share the remaining
budget; a retained authenticated carrier keeps its full migration budget.
No parallel fanout, new discovery or shorter global retry floor is introduced.
Every candidate owns its admission generation. A partially completed migration
retains that ownership across later refusal, even before the first completed
Waiting transition. It preserves useful paths, retires an unused former
attachment before another dial, and refuses a third physical admission when
both retained paths are still needed. Active and node restart clear this
private lifecycle ownership; initial ordinary mesh draining remains distinct.

This recovery does not change the probe interval, miss limit, retention grace
or host authentication timeout. A carrier that silently loses a route can
therefore cause a transient application-offline interval before the existing
probe budget expires and fresh routing succeeds. No fixed recovery latency
or uninterrupted-presence guarantee is inferred from carrier readiness.

## State and carrier

Read these additive fields from MeshInfo, or subscribe to event 12:

| Field | Meaning |
| --- | --- |
| `activity_mode` | Actual `active` / `waiting` policy |
| `requested_activity_mode` | Latest accepted desired policy |
| `waiting_reachability` | `attaching`, `ready`, or `unavailable` |
| `waiting_attachment_count` | Physical connections selected for waiting, excluding virtual end-to-end peers |

`ready` currently describes the selected carrier only. It does not assert a
route to every conversation peer. In particular, two waiting leaves attached
to different relays cannot exchange room or directed traffic through the
existing protocol, even if those relay nodes are connected to each other.

An actual waiting node retains one authenticated relay carrier. A live handover
may temporarily retain two; the attempt uses the existing handshake deadline.
This explicit exception also permits handover when MaxPeers is one. It does not
change ordinary consumers' admission ceiling. A successful transition closes
unrelated physical peers and retires third-party relay routes before reporting
ready. The node does not forward other peers' relay traffic or pub/sub messages.

Existing room subscribers, explicit targets, directed application paths and
stream registrations are retained. Application-relevant direct paths migrate
to authenticated end-to-end relay sessions before their old transport closes.
The public packet callback and stable identity continue to represent the same
peer. A prepared fallback prevents a direct carrier closing from erasing a new
relay path or reporting a false peer departure.
An attachment loss also retains the exact previously established application,
explicit-target and subscribed paths removed with that carrier. The next
attempt must migrate those paths before reporting ready, even though their
virtual peers have already left the live directory. Unrelated saved offline
targets do not become migration barriers. Verified replacement consumes the
captured loss; a newer carrier loss survives an older completion. Returning
active or stopping clears this Waiting-only intent.
Restored local application interest remains significant before a replacement
peer's fresh subscription claim arrives; the removed remote claim itself is
never fabricated. Loss of a useful partially migrated carrier uses the same
preservation rule, even before that candidate becomes the committed attachment.
Failed handovers discard an unused candidate under the same policy-generation
guard. A carrier already carrying a partially migrated application path is
retained; failure never discards that working path just to reduce the socket
count before reporting the incomplete transition.
Directory retirement, outbound-queue closure and subscription/score cleanup
share the admission lock. A late direct teardown cannot erase a newly admitted
relay's GRAFT; the orphan-queue sweep uses the same lock order and live check.

If an application path cannot migrate, the node reports actual `active`, desired
`waiting`, reachability `unavailable`. It retains the required paths and retries;
it does not advertise a full ordinary mesh as economical waiting. The LastError
text distinguishes a failed path migration. Hosts must inspect actual state,
not interpret setter success as a completed transition.

Moss does not own a host's MLS state or application persistence. Its callbacks,
room keys/subscriptions, node identity and listeners survive the transition.
Replacing a physical Noise connection does not replace the application peer.
Hosts must preserve their own authenticated conversation state on transport
migration and still verify the real application-level integration.

## Work that pauses

Tracker announcements, DHT/LAN discovery, overlay work, ordinary Veil bootstrap
dialers, statistical gossip, peer warming, direct upgrades and unrelated
maintenance run in cancellable active scopes. New ordinary dials, overlay query
work, third-party forwarding and hole-punch requests are gated while waiting.
Returning active starts one new discovery scope after the old scope drains.
In-flight discovery is cancelled/bounded; if it cannot drain within an attempt,
the transition remains visibly incomplete and retries.

The waiting node refreshes its own identity and necessary automatic room
provider records only through its selected carrier, using the existing 30s
record cadence and bounded outbound queue. This performs no lookup or fanout
and creates no additional attachment. This retains the record beyond its
existing 90s TTL at the selected carrier; a foreground origin still needs a
working discovery or application route to that carrier.

Identity and room lookups keep the existing no-dial rule and query budget.
When the globally nearest K contacts are exhausted or unconnected, they ask
farther contacts already connected locally. A fresh record on a live carrier
must not become invisible merely because unrelated unconnected contacts are
closer in the keyspace. An ordinary explicit target recovering a lost relay
also reuses a live carrier's authenticated listen-address hint; this does not
change healthy ordinary direct startup or admit unsigned address aliases.
Identity-first recovery also collects alternate records: a previous carrier's
valid 90s record must not hide the new one. Three RPC slots refill independently
within the existing 24-query budget; returning cancels outstanding RPCs.
Collection reserves half the caller's remaining deadline for route attempts,
whose unique attachment hints share the remaining budget. The existing 5s
explicit worker limit is not extended, and no extra sockets are retained.

Waiting maintenance uses the existing carrier probe interval, ping timeout and
miss limit. It stops the ordinary gossip heartbeat. A small resource sweep still
bounds the peer directory; it does not dial or refresh the whole mesh. No wire
keepalive interval or relay-server defaults were changed.

## Recovery and cold processes

One attachment attempt runs at a time. Failed attempts back off from five
seconds to five minutes with bounded positive jitter; success resets the
backoff. An attachment disconnect or a signed capability revocation triggers
one coalesced immediate recovery. Repeated identical mode calls cannot bypass
the retry timer.

Candidates come from already known signed relay identities and existing
configuration/cache addresses. A cold process can probe one configured/static,
Veil or cached seed without restarting broad discovery. An address is not proof
of capability: Noise authentication and the peer's signed reachable-relay
announcement must arrive within the attempt deadline before it becomes ready.
Anonymous candidates never bypass a configured identity allowlist. The cache
format and configuration schema remain unchanged.

Existing provider records on a previous live carrier are not explicitly
invalidated. Their normal 90s TTL still applies, so a fresh lookup can encounter
an old attachment hint after handover. Hosts retain route-failure/retry policy;
an immediate route for every new origin is not inferred from carrier readiness.

Hosts can observe their default network and call optional
`Moss_NotifyNetworkChanged(handle)` / `Node.NotifyNetworkChanged()`. This never
opts an ordinary consumer into waiting and is a no-op in active mode. Hints
coalesce in the existing attachment worker, with a five-second floor and one
pending latest check. An eligible hint cancels an obsolete candidate; healthy
carriers receive the existing authenticated liveness probe and remain in place.
Unavailable nodes can retry once without resetting ordinary outage backoff.
There is no OS observer inside Moss; older libraries still require failure or
timer recovery. A host hint alone never establishes delivery or reachability.

Waiting transitions revalidate captured trim work against the current policy
generation. Suspended virtual-peer probes cannot evict room membership, and
application use survives both sending and receiving over relay. A direct path
being replaced by relay drains for the existing one-second migration grace;
settled waiting is reported after that physical path has closed.

## Host responsibilities and limitations

* Detect the optional symbol when loading older libraries. Missing support is
  unsupported; it is not permission to label an ordinary mesh as waiting.
* Request active before interactive discovery, calls or stream traffic. Existing
  raw `SendStream` does not gain relay support. Registering a stream handler
  survives a transition; a stream still needs a direct path to carry data.
* Request active for user-generated outgoing work. Normal foreground discovery
  and identity/room lookup can reach a waiting recipient's carrier. The waiting
  recipient can respond over that existing end-to-end session. Arbitrary
  waiting-to-waiting sends across distinct fixed carriers are not supported.
* Keep existing application authentication and resend/recovery policies. A
  carrier being ready does not promise that every remote peer is online.
* Do not assume private-room subscriptions turn carriers into room routers.
  Relay setup requires the carrier to know the target directly; an existing
  virtual relay cannot be used as another relay hop. Overlay discovery names
  the target's carrier and normally dials it. Keeping one carrier per leaf
  therefore requires additional rendezvous/routing work for distinct carriers.
  Already received authenticated work can trigger active transport, but this
  cannot recover a control packet that never reaches the waiting installation.
* Waiting does not persist messages at relays or wake a suspended Android OS.
  The Android host still needs its permitted service/execution lifecycle and
  key-access policy. No battery, radio or delivery-latency budget is inferred
  from Go integration tests.

## Verification

Focused tests use independent real nodes and a real relay, with an artificial public-NAT
fixture only for hermetic loopback topology. They check bidirectional private
room and directed delivery with both leaves waiting; identity, callbacks and
subscriptions across active/waiting/active; cold configured attachment; relay
outage and live handover; MaxPeers-one bounds; duplicate requests/backoff;
third-party relay refusal; invalid inputs and concurrent Stop; tracker
suspension and unchanged default consumers. Public Go and a compiled C harness
exercise the host API. A deterministic direct-loss-during-relay-admission test
reproduced a migration race: direct teardown deleted an in-flight relay accept.
Waiting now preserves that bounded pending admission through its existing
waiter. Fresh foreground callers are also tested after the real 90s provider TTL,
using only recipient identity or room knowledge, without a test-supplied target
carrier address. Three- and four-node TCP tests also verify subscription
exchange under the real P6 engine output after score-pruning maintenance,
without granting mesh membership. Their loopback topology pins that negative
score through the existing host hook; separate policy tests use actual public
address metadata and default scoring. These are not physical Android acceptance
tests.
