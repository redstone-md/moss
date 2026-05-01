package mesh

import (
	"context"
	"sync/atomic"
	"time"

	"moss/internal/gossip"
	"moss/internal/transport"
)

func (n *Node) removePeer(peerID string, session *transport.Session) {
	n.mu.Lock()
	peer := n.peers[peerID]
	if peer == nil || peer.session != session {
		n.mu.Unlock()
		return
	}
	delete(n.peers, peerID)
	delete(n.suppress, peerID)
	delete(n.relayBuckets, peerID)
	delete(n.directProbes, peerID)
	delete(n.peerDials, peerID)
	for sessionID, relaySession := range n.relayLocals {
		if relaySession.viaPeerID == peerID || relaySession.remotePeerID == peerID {
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
	n.recalculateIPColocationPenalties()
	if peer != nil {
		n.enqueueEvent(EventPeerLeft, map[string]string{"peer": peerID, "addr": peer.addr})
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
		if peer != nil {
			_ = peer.session.Close()
		}
	}
}

func (n *Node) maintenanceLoop(ctx context.Context) {
	defer n.wg.Done()
	ticker := time.NewTicker(n.config.Heartbeat())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			atomic.AddUint64(&n.heartbeat, 1)
			n.scoring.Tick()
			n.evaluateMeshDeliveryDeficits(time.Now())
			n.probePeerLatency(time.Now())
			n.pruneLowScoringPeers()
			n.pruneHighLatencyPeers()
			n.connectKnownPeers()
			n.connectBootstrapSeeds(ctx)
			n.promoteRelayPeers()
			n.refreshLocalSubscriptions()
			for _, channel := range n.pubsub.SnapshotLocal() {
				n.maintainTopicMesh(channel)
			}
			n.refreshSupernodeStatus()
		}
	}
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
