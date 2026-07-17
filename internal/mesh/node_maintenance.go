package mesh

import (
	"context"
	"encoding/json"
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
	delete(n.peers, peerID)
	delete(n.suppress, peerID)
	delete(n.relayBuckets, peerID)
	delete(n.directProbes, peerID)
	delete(n.peerDials, peerID)
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
	n.pubsub.RemovePeer(peerID)
	for _, relayedPeerID := range removedRelayed {
		n.pubsub.RemovePeer(relayedPeerID)
	}
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
	rtt := time.Since(current.pingSentAt)
	if rtt <= 0 {
		rtt = time.Nanosecond
	}
	current.lastRTT = rtt
	current.pingPending = ""
	current.pingSentAt = time.Time{}
	current.pingMisses = 0
}

func (n *Node) probePeerLatency(now time.Time) {
	type pingTarget struct {
		peer      *peerConn
		requestID string
	}
	interval := n.peerProbeInterval()
	targets := make([]pingTarget, 0)
	n.mu.Lock()
	for _, peer := range n.peers {
		if peer.pingPending != "" {
			continue
		}
		if peer.pingPending == "" && !peer.pingSentAt.IsZero() && now.Sub(peer.pingSentAt) < interval {
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
	n.mu.Unlock()
	failed := make([]pingTarget, 0)
	for _, target := range targets {
		ok := n.sendEnvelope(target.peer, gossip.Envelope{Type: gossip.TypePing, RequestID: target.requestID})
		if ok {
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

func (n *Node) pruneHighLatencyPeers() {
	now := time.Now()
	ids := make([]string, 0, len(n.peers))
	pruneOnly := make([]string, 0, len(n.peers))
	n.mu.Lock()
	for id, peer := range n.peers {
		if peer.lastRTT > peerLatencyPruneThreshold {
			pruneOnly = append(pruneOnly, id)
			continue
		}
		if peer.pingPending != "" && now.Sub(peer.pingSentAt) > peerPingTimeout {
			peer.pingPending = ""
			peer.pingSentAt = time.Time{}
			peer.pingMisses++
			pruneOnly = append(pruneOnly, id)
			if !n.shouldRetainPeerLocked(peer) && peer.pingMisses >= peerDisconnectMissLimit {
				ids = append(ids, id)
			}
		}
	}
	n.mu.Unlock()
	for _, id := range pruneOnly {
		n.prunePeerFromAllMeshes(id)
	}
	for _, id := range ids {
		n.mu.RLock()
		peer := n.peers[id]
		n.mu.RUnlock()
		peer.closeSession()
	}
}

// closeSession closes the peer's transport session if it has one, telling the
// far side first. A relayed peer reaches us through a supernode and has NO
// direct session (session is nil), so calling Close on it would panic on a nil
// receiver — this guards it.
func (p *peerConn) closeSession() {
	if p != nil && p.session != nil {
		farewell(p.session)
		_ = p.session.Close()
	}
}

// farewell tells the far side this session is over. Best-effort by nature: the
// link may already be gone, which is precisely when the write fails and there is
// nothing to do about it. It costs one datagram and saves the peer 37 seconds of
// talking to a socket nobody reads.
func farewell(session *transport.Session) {
	if session == nil {
		return
	}
	payload, err := json.Marshal(gossip.Envelope{Type: gossip.TypeGoodbye})
	if err != nil {
		return
	}
	_ = session.WritePacket(payload)
}

// farewellAndClose is farewell for a session we hold no peerConn for — the
// duplicate the dedup rejects on arrival. The far side may well have registered
// it, and without this it would keep writing into it until the pings run out.
func farewellAndClose(session *transport.Session) {
	farewell(session)
	_ = session.Close()
}

func (n *Node) maintenanceLoop(ctx context.Context) {
	defer n.wg.Done()
	ticker := time.NewTicker(n.config.Heartbeat())
	defer ticker.Stop()
	connEvery := n.connMaintenanceEvery()
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
			n.scoring.Tick()
			n.probePeerLatency(time.Now())
			n.pruneLowScoringPeers()
			n.pruneHighLatencyPeers()
			n.connectKnownPeers()
			n.dialExplicitTargets()
			n.connectBootstrapSeeds(ctx)
			n.promoteRelayPeers()
			n.refreshLocalSubscriptions()
			n.pruneStaleRelayRoutes()
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

func (n *Node) pruneLowScoringPeers() {
	n.mu.RLock()
	ids := make([]string, 0, len(n.peers))
	for id := range n.peers {
		if n.peerScore(id) < 0 {
			ids = append(ids, id)
		}
	}
	n.mu.RUnlock()
	for _, id := range ids {
		n.prunePeerFromAllMeshes(id)
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
