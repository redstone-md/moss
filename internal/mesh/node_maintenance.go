package mesh

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/transport"
)

// supernodeReannounceEveryTicks throttles how often an active SuperNode
// re-broadcasts its signed SupernodeAnnounce (every N maintenance heartbeats), so
// peers that joined after promotion converge on its relay role without flooding
// the mesh. At the default 1s heartbeat this is ~10s; tests using a faster
// heartbeat converge proportionally sooner.
const supernodeReannounceEveryTicks uint64 = 10

func (n *Node) removePeer(peerID string, session *transport.Session) {
	n.mu.Lock()
	peer := n.peers[peerID]
	if peer == nil || peer.session != session {
		n.mu.Unlock()
		return
	}
	// Capture before the entry goes: how long the session held, and whether it
	// died on missed pings, is what tells a NAT mapping timing out apart from an
	// ordinary disconnect. Sessions dropping at a flat interval is a signature,
	// and it should be a query rather than something to reconstruct from logs.
	endedRelayed, endedAt, endedMisses, endedOrigin := peer.relayed, peer.connectedAt, peer.pingMisses, peer.origin
	endedInbound := peer.inboundPackets.Load()
	// A non-nil session can still have no remote: a relayed peer carries none at
	// all, and a session whose carrier is already gone returns nil here. Reading
	// through that panicked a test outright — in production it would have taken
	// the node down on an ordinary disconnect, which is a poor trade for a
	// telemetry field.
	endedTransport := ""
	if peer.session != nil {
		if remote := peer.session.RemoteAddr(); remote != nil {
			endedTransport = remote.Network()
		}
	}
	n.emitSessionClose(peerID, time.Since(endedAt), endedMisses, endedOrigin, endedInbound, endedRelayed)
	delete(n.peers, peerID)
	delete(n.suppress, peerID)
	delete(n.relayBuckets, peerID)
	delete(n.directProbes, peerID)
	// A session that died on missed pings is a FAILED path, whatever the dial
	// thought.
	//
	// Clearing the cooldown here means an instant redial, which is right for a
	// peer that simply went away. It is wrong for one that answers the dial and
	// then never speaks: the connect succeeds, so the backoff records success and
	// resets, the session dies at six misses ~37s later, and it redials at once —
	// forever, with no backoff ever engaging. That loop is what players feel as
	// entering a lobby on the fourth or fifth try.
	//
	// A peer drowning in its own flood cannot be fixed from here, and there is no
	// need to keep proving it every 37s: charge it as a failure so the interval
	// grows, and let a peer that works be preferred instead. Any healthy session
	// clears it again.
	if endedMisses >= peerDisconnectMissLimit {
		n.peerDialFailures[peerID]++
		n.peerDials[peerID] = time.Now()
	} else {
		delete(n.peerDials, peerID)
		delete(n.peerDialFailures, peerID)
	}
	removedRelayed := make([]string, 0)
	for sessionID, relaySession := range n.relayLocals {
		if relaySession.viaPeerID == peerID || relaySession.remotePeerID == peerID {
			if n.removeRelayedPeerLocked(relaySession) {
				removedRelayed = append(removedRelayed, relaySession.remotePeerID)
			}
			delete(n.relayLocals, sessionID)
			delete(n.directProbes, relaySession.remotePeerID)
		}
	}
	for sessionID, route := range n.relayRoutes {
		if route.initiator != peerID && route.target != peerID {
			continue
		}
		delete(n.relayRoutes, sessionID)
		n.relaySessions.Release(sessionID)
	}
	if info, ok := n.knownPeers[peerID]; ok {
		info.direct = false
		info.lastSeen = time.Now()
		n.knownPeers[peerID] = info
	}
	n.mu.Unlock()
	// The peer is gone: its outbound queue (~110KB at depth) must go with
	// it, or every peer this node EVER connected leaks until Stop. Under
	// NO n.mu here: the single lock order is n.mu → outboundMu
	// (sendOrEnqueueWire enqueues while holding n.mu.RLock), so
	// teardownOutboundQueue's outboundMu must never be taken under n.mu.
	n.teardownOutboundQueue(peerID)
	n.pubsub.RemovePeer(peerID)
	for _, relayedPeerID := range removedRelayed {
		n.pubsub.RemovePeer(relayedPeerID)
		n.scoring.Remove(relayedPeerID)
	}
	// The scoring engine outlives the peer: without eviction every peer the
	// node EVER connected kept its entry (and Tick walked them all, every
	// second, forever) — an unbounded map on a long-running node. Score()
	// recreates an evicted peer at zero on first use, so this costs nothing.
	n.scoring.Remove(peerID)
	n.recalculateIPColocationPenalties()
	if peer != nil {
		n.enqueueEvent(EventPeerLeft, map[string]string{"peer": peerID, "addr": peer.addr})
		n.reportSessionLifetime(endedRelayed, endedAt, endedMisses, endedOrigin, endedTransport, endedInbound, peerID)
	}
}

func (n *Node) observeMeshDelivery(channel, messageID, peerID string) {
	if channel == "" || messageID == "" || peerID == "" {
		return
	}
	if !n.pubsub.InMesh(channel, peerID) {
		return
	}
	if n.isPeerBelowBaseline(peerID) {
		return
	}
	expected := make(map[string]struct{})
	for _, meshPeerID := range n.pubsub.MeshPeers(channel) {
		if n.isPeerBelowBaseline(meshPeerID) {
			continue
		}
		expected[meshPeerID] = struct{}{}
	}
	due := time.Now().Add(n.config.Heartbeat())
	if n.config.Heartbeat() <= 0 {
		due = time.Now().Add(time.Second)
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	obs := n.meshDeliveries[messageID]
	if obs == nil {
		obs = &meshDeliveryObservation{
			due:       due,
			expected:  expected,
			delivered: make(map[string]struct{}),
		}
		n.meshDeliveries[messageID] = obs
	}
	if _, ok := obs.expected[peerID]; ok {
		obs.delivered[peerID] = struct{}{}
	}
}

func (n *Node) evaluateMeshDeliveryDeficits(now time.Time) {
	n.mu.Lock()
	expired := make([]*meshDeliveryObservation, 0, len(n.meshDeliveries))
	for messageID, obs := range n.meshDeliveries {
		if now.Before(obs.due) {
			continue
		}
		expired = append(expired, obs)
		delete(n.meshDeliveries, messageID)
	}
	n.mu.Unlock()

	for _, obs := range expired {
		for peerID := range obs.expected {
			if _, delivered := obs.delivered[peerID]; delivered {
				continue
			}
			n.scoring.PenalizeMeshDelivery(peerID)
		}
	}
}

// handlePong records the round-trip of the answered probe and re-bases the
// probe floor at the pong's arrival: pingSentAt is set to now and, unlike
// pingPending, deliberately NOT zeroed. A non-zero timestamp is the base the
// probe floor (peerProbeInterval) counts from, so a healthy peer is re-pinged
// a full interval after its last pong, not on the very next conn-tick.
// Zeroing it here used to wipe that base on every pong, making
// peerProbeIntervalFloor dead code and pinging every connected peer once per
// ~1s maintenance pass — almost all idle-session traffic on a healthy mesh.
// The prune scan gates on pingPending, not on the zero value, so the retained
// timestamp cannot manufacture an expired-ping miss.
func (n *Node) handlePong(peer *peerConn, env gossip.Envelope) {
	if peer == nil || env.RequestID == "" {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	current := n.peers[peer.id]
	if current == nil || current.pingPending != env.RequestID || current.pingSentAt.IsZero() {
		return
	}
	now := time.Now()
	rtt := now.Sub(current.pingSentAt)
	if rtt <= 0 {
		rtt = time.Nanosecond
	}
	current.lastRTT = rtt
	current.pingPending = ""
	current.pingSentAt = now
	current.pingMisses = 0
}

// pingTarget pairs a peer selected for a latency probe with the request ID
// already written to its pingPending, so the unlocked send phase can clear the
// field if the write fails and the next pass re-probes instead of timing out.
type pingTarget struct {
	peer      *peerConn
	requestID string
}

// collectPingTargetsLocked marks probe-eligible peers with a fresh ping and
// returns the send list. Caller must hold n.mu. A peer holding an expired ping
// is deliberately left alone — consuming it belongs to the prune scan
// (collectPruneLocked), so a standalone probe never eats the timeout
// accounting the prune pass acts on.
func (n *Node) collectPingTargetsLocked(now time.Time) []pingTarget {
	interval := n.peerProbeInterval()
	targets := make([]pingTarget, 0, len(n.peers))
	for _, peer := range n.peers {
		if peer.pingPending != "" {
			continue
		}
		if !peer.pingSentAt.IsZero() && now.Sub(peer.pingSentAt) < interval {
			continue
		}
		requestID, err := newRelaySessionID()
		if err != nil {
			continue
		}
		peer.pingPending = requestID
		peer.pingSentAt = now
		targets = append(targets, pingTarget{peer: peer, requestID: requestID})
	}
	return targets
}

// sendPingTargets writes the queued pings outside n.mu and clears pingPending
// on peers whose write failed, so a ping that never left the node is retried
// by the next pass instead of aging into a phantom miss.
func (n *Node) sendPingTargets(targets []pingTarget) {
	failed := make([]pingTarget, 0, len(targets))
	for _, target := range targets {
		if n.sendEnvelope(target.peer, gossip.Envelope{Type: gossip.TypePing, RequestID: target.requestID}) {
			continue
		}
		failed = append(failed, target)
	}
	if len(failed) == 0 {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, target := range failed {
		current := n.peers[target.peer.id]
		if current == target.peer && current.pingPending == target.requestID {
			current.pingPending = ""
			current.pingSentAt = time.Time{}
		}
	}
}

// pruneLists is one prune scan's outcome: peers to drop from topic meshes,
// and the subset whose session must also be closed.
type pruneLists struct {
	meshOnly   []string
	disconnect []string
}

// collectPruneLocked consumes expired pings and partitions the affected peers
// into mesh-prune and disconnect lists. Caller must hold n.mu.
func (n *Node) collectPruneLocked(now time.Time) pruneLists {
	lists := pruneLists{
		meshOnly:   make([]string, 0, len(n.peers)),
		disconnect: make([]string, 0),
	}
	for id, peer := range n.peers {
		if peer.lastRTT > peerLatencyPruneThreshold {
			lists.meshOnly = append(lists.meshOnly, id)
			continue
		}
		if peer.pingPending != "" && now.Sub(peer.pingSentAt) > peerPingTimeout {
			peer.pingPending = ""
			peer.pingSentAt = time.Time{}
			peer.pingMisses++
			lists.meshOnly = append(lists.meshOnly, id)
			if !n.shouldRetainPeerLocked(peer) && peer.pingMisses >= peerDisconnectMissLimit {
				lists.disconnect = append(lists.disconnect, id)
			}
		}
	}
	return lists
}

// applyPruneLists drops mesh-only peers from every topic mesh and closes the
// sessions of disconnect peers.
func (n *Node) applyPruneLists(lists pruneLists) {
	for _, id := range lists.meshOnly {
		n.prunePeerFromAllMeshes(id)
	}
	for _, id := range lists.disconnect {
		n.mu.RLock()
		peer := n.peers[id]
		n.mu.RUnlock()
		peer.closeSession()
	}
}

func (n *Node) probePeerLatency(now time.Time) {
	n.mu.Lock()
	targets := n.collectPingTargetsLocked(now)
	n.mu.Unlock()
	n.sendPingTargets(targets)
}

func (n *Node) pruneHighLatencyPeers() {
	n.mu.Lock()
	lists := n.collectPruneLocked(time.Now())
	n.mu.Unlock()
	n.applyPruneLists(lists)
}

// connTickProbeAndPrune is the maintenance loop's per-second pass: the probe
// and prune scans share one n.mu acquisition instead of two back-to-back
// lock cycles, with the envelope sends and mesh mutations after the unlock —
// the same ordering the standalone functions use.
func (n *Node) connTickProbeAndPrune(now time.Time) {
	n.mu.Lock()
	targets := n.collectPingTargetsLocked(now)
	lists := n.collectPruneLocked(now)
	n.mu.Unlock()
	n.sendPingTargets(targets)
	n.applyPruneLists(lists)
}

// closeSession closes the peer's transport session if it has one. A relayed peer reaches us through a supernode and has NO
// direct session (session is nil), so calling Close on it would panic on a nil
// receiver — this guards it.
func (p *peerConn) closeSession() {
	if p != nil && p.session != nil {
		_ = p.session.Close()
	}
}

func (n *Node) maintenanceLoop(ctx context.Context) {
	defer n.wg.Done()
	ticker := time.NewTicker(n.config.Heartbeat())
	defer ticker.Stop()
	connEvery := n.connMaintenanceEvery()
	// Conn-maintenance phases. Each job class keeps its own counter and fires
	// when the counter wraps its interval; the counters start at staggered
	// offsets so dial/promote/subs sweeps do not share a wake-up beat, and a
	// node-derived offset jitters WHICH conn-tick each phase lands on — so a
	// fleet of 100 peers does not redial, re-promote, and re-announce in
	// lockstep. See maintenancePhaseOffset.
	dialPhase := n.maintenancePhaseOffset(maintenancePhaseDialEvery)
	promotePhase := n.maintenancePhaseOffset(maintenancePhasePromoteEvery)
	subsPhase := n.maintenancePhaseOffset(maintenancePhaseSubsEvery)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ticks := atomic.AddUint64(&n.heartbeat, 1)
			// Pub/sub mesh upkeep and supernode-status broadcasts run at the
			// gossip heartbeat, which a chat client may set very low (e.g. 250ms)
			// for responsiveness. These are cheap and event-driven — they only
			// emit on an actual change — so their cadence is fine to keep fast.
			n.evaluateMeshDeliveryDeficits(time.Now())
			for _, channel := range n.pubsub.SnapshotLocal() {
				n.maintainTopicMesh(channel)
			}
			n.refreshSupernodeStatus()
			if ticks%supernodeReannounceEveryTicks == 0 {
				n.reannounceSupernodeStatus()
			}
			// Peer/connection upkeep runs at ~1s regardless of the gossip
			// heartbeat. Running score decay and prune/reconnect every tick at a
			// 250ms heartbeat aged peers ~4x too fast and flapped otherwise-
			// healthy connections continuously.
			if ticks%connEvery != 0 {
				continue
			}
			// Health checks are cheap and mostly no-op; they run every
			// conn-tick (~1s).
			n.scoring.Tick()
			// Probe + high-latency prune in one locked pass; the low-score
			// prune collects ids under RLock and scores them only after
			// releasing it (peerScore must never run under n.mu).
			n.connTickProbeAndPrune(time.Now())
			n.pruneLowScoringPeers()
			n.pruneStaleRelayRoutes()
			// Reclaim outbound queues orphaned by the eviction/relay paths
			// that delete peers without removePeer; one bounded pass per
			// conn-tick. See sweepOrphanOutboundQueues.
			n.sweepOrphanOutboundQueues()
			// The known-peers directory sweep self-throttles internally
			// (knownPeersSwept, once per knownPeerSweepEvery), so calling it on
			// every conn-tick is a timestamp check — the walk only runs on its
			// own cadence. Bounding the directory here is what keeps the dial
			// pass's candidate scan O(catalog) instead of O(ever-grown).
			n.sweepKnownPeers(time.Now())
			// Dials (known peers, explicit targets, bootstrap seeds): every
			// maintenancePhaseDialEvery conn-ticks. A dial pass costs a
			// lock-guarded snapshot plus up to DOut asynchronous handshakes;
			// running it every second had every node re-attempting the same
			// backoff-eligible peers as soon as their cooldowns expired,
			// together.
			if dialPhase++; dialPhase%maintenancePhaseDialEvery == 0 {
				n.connectKnownPeers()
				n.dialExplicitTargets()
				n.connectBootstrapSeeds(ctx)
			}
			// Relay promotion: every maintenancePhasePromoteEvery conn-ticks.
			// Promotion targets are already reachable via relay, so nobody is
			// waiting on a faster cadence.
			if promotePhase++; promotePhase%maintenancePhasePromoteEvery == 0 {
				n.promoteRelayPeers()
			}
			// Subscription re-announce: every maintenancePhaseSubsEvery
			// conn-ticks. Event paths (Subscribe, new-peer join via
			// sendKnownPeerSnapshot) announce immediately; this is only the
			// safety net for envelopes lost in flight.
			if subsPhase++; subsPhase%maintenancePhaseSubsEvery == 0 {
				n.refreshLocalSubscriptions()
			}
		}
	}
}

// connMaintenanceEvery returns how many heartbeat ticks make up ~1s, so
// peer/connection maintenance runs about once a second no matter how fast the
// gossip heartbeat is configured.
func (n *Node) connMaintenanceEvery() uint64 {
	hb := n.config.Heartbeat()
	if hb <= 0 {
		return 1
	}
	if every := uint64(time.Second / hb); every > 1 {
		return every
	}
	return 1
}

// maintenancePhaseOffset returns the initial value for a phase counter so that
// maintenance jobs with an every-N cadence start staggered across nodes. The
// offset is derived from the node's own peer id, so it is stable across
// restarts, needs no extra state, and does not require a per-tick RNG; two
func (n *Node) maintenancePhaseOffset(every uint64) uint64 {
	if every <= 1 {
		return 0
	}
	var h uint64
	for _, b := range []byte(n.localPeerID()) {
		h = h*131 + uint64(b)
	}
	// Fold the interval in so two phases with related periods (5 vs 10 vs 30)
	// decorrelate: a hash of the peer id alone makes every offset a residue of
	// the same number, and the phases line up again on a shared beat.
	h = h*131 + every
	return h % every
}

func (n *Node) pruneLowScoringPeers() {
	// Snapshot the peer ids only; the scores are computed after the RLock is
	// released. peerScore may run an application scoring callback, and that
	// callback must never be invoked while n.mu is held (the node_peer_
	// discovery.go:76 invariant): a blocking callback holding RLock stalls
	// every writer on the hot envelope path.
	n.mu.RLock()
	ids := make([]string, 0, len(n.peers))
	for id := range n.peers {
		ids = append(ids, id)
	}
	n.mu.RUnlock()
	for _, id := range ids {
		if n.peerScore(id) < 0 {
			n.prunePeerFromAllMeshes(id)
		}
	}
}

func (n *Node) prunePeerFromAllMeshes(peerID string) {
	until := time.Now().Add(n.peerPruneBackoff())
	n.mu.Lock()
	if peer := n.peers[peerID]; peer != nil && until.After(peer.meshBlocked) {
		peer.meshBlocked = until
	}
	n.mu.Unlock()
	for _, channel := range n.pubsub.SnapshotLocal() {
		if !n.pubsub.InMesh(channel, peerID) {
			continue
		}
		n.pubsub.SetMeshPeer(channel, peerID, false)
		n.mu.RLock()
		peer := n.peers[peerID]
		n.mu.RUnlock()
		if peer != nil {
			n.sendEnvelope(peer, gossip.Envelope{Type: gossip.TypePrune, Channel: channel})
		}
	}
}

func (n *Node) peerProbeInterval() time.Duration {
	interval := peerProbeIntervalFloor
	if heartbeat := n.config.Heartbeat(); heartbeat > interval {
		interval = heartbeat
	}
	return interval
}

func (n *Node) peerPruneBackoff() time.Duration {
	backoff := n.peerProbeInterval()
	if backoff < 30*time.Second {
		return 30 * time.Second
	}
	return backoff
}
