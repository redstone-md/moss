# Automatic discovery and admission capacity

An idle client previously dialed tracker/directory candidates until it reached
`MaxPeers` (200 by default). An optimized standalone Mosh-shaped node reproduced
50–53 connections, mostly outgoing, even with no topic subscriptions. General
discovery announcements continued to consume bandwidth and signature work.

Automatic known-peer discovery, immediate tracker kicks and seed retries now
share a working goal derived from `max(DHigh, D, DOut, 1)` (12 by default).
Existing direct and relayed neighbors count toward it. Discovery replenishes
lost neighbors and reserves demand for `DOut` direct outbound connections, subject
to available eligible peers and the existing hard capacity. Known capable relays can fill a relay deficit
after the working goal is reached; unrelated plain candidates cannot use those
repair slots. Capable-relay repair may initiate despite public-peer ID ordering:
the remote relay can already have met its own goal and never dial us. Ordinary
peers keep glare avoidance. Candidate ranking, host backoff and dial batch size
remain intact.

The working goal is not a hard connection cap. Manual/retained contacts,
topic-provider lookup, configured static peers, LAN discovery and inbound
admission retain their existing paths and hard limits. No existing connection
is disconnected to meet the goal. Inbound traffic, concurrent attempts and
explicit application demand can therefore leave more than 12 peers connected.
MaxPeers, directory capacity, wire/FFI contracts and timers are unchanged.

Selector regressions cover all three automatic paths, replenishment, inbound
and relay deficits, relayed neighbors, static seeds and hard capacity. Real
isolated nodes verify a retained absent contact becomes reachable beyond the
working goal and exchanges encrypted private-room and directed payloads both
ways. The same delivery checks cover new inbound/manual contacts. The existing
late-leaf relay regression exercises delivery without catalog-wide key gossip.

The architecture follows the separation of operational connection management
and hard safety limits described by [libp2p](https://github.com/libp2p/go-libp2p/blob/master/p2p/host/resource-manager/README.md),
without adding libp2p or a new public configuration field. Reusing Moss's degree
settings for the working goal is internal policy; GossipSub defines those
settings for per-topic meshes, not a global transport cap.
