package mesh

func (n *Node) waitingMigrationTargets(via string) map[string]*peerConn {
	n.mu.RLock()
	defer n.mu.RUnlock()
	targets := make(map[string]*peerConn, len(n.activity.recovering))
	for id, peer := range n.activity.recovering {
		targets[id] = peer
	}
	for id, peer := range n.peers {
		if id != via && (!peer.relayed || peer.viaPeerID != via) && n.waitingApplicationPeerLocked(peer) {
			if _, retained := targets[id]; !retained {
				targets[id] = nil
			}
		}
	}
	return targets
}

// Use the same preservation rule for ordinary transitions and carrier loss.
// Caller holds n.mu; the pubsub manager's snapshots have their own lock.
func (n *Node) waitingApplicationPeerLocked(peer *peerConn) bool {
	if peer.applicationUsed.Load() || n.explicitTargets[peer.id] != nil {
		return true
	}
	for _, topic := range n.pubsub.SnapshotLocal() {
		if n.pubsub.HasPeerSubscription(peer.id, topic) {
			return true
		}
	}
	return false
}

// Carrier teardown removes virtual peers before the next Waiting attempt.
// Retain only application paths established through this lost incarnation,
// rather than treating every saved (possibly offline) explicit target as live.
// Caller holds n.mu.
func (n *Node) retainWaitingApplicationPathLocked(session relayLocalSession) {
	peer := n.peers[session.remotePeerID]
	if !session.established || peer == nil || !peer.relayed || peer.relaySessionID != session.sessionID ||
		!n.waitingApplicationPeerLocked(peer) {
		return
	}
	if n.activity.recovering == nil {
		n.activity.recovering = make(map[string]*peerConn)
	}
	n.activity.recovering[peer.id] = peer
}

// Consume only the captured loss after a verified replacement. A concurrent
// second carrier teardown must keep its newer recovery intent.
func (n *Node) completeWaitingRecovery(peerID, via string, lost *peerConn) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	peer := n.peers[peerID]
	if peer == nil || (peer.relayed && peer.viaPeerID != via) || (!peer.relayed && peerID != via) {
		return false
	}
	if n.waitingRequested() && via == n.activity.candidate {
		// Unlike drain timers, ownership survives a later target's refusal.
		n.activity.migrationOwned = true
	}
	if lost != nil {
		// The captured path carried application interest (possibly only a
		// subscription); teardown already removed that incarnation's claims.
		peer.applicationUsed.Store(true)
		if n.activity.recovering[peerID] == lost {
			delete(n.activity.recovering, peerID)
		}
	}
	return true
}

// A failed attempt may already have migrated another application path. Do not
// retire its working carrier while discarding a wholly unused candidate.
// Caller holds n.mu.
func (n *Node) waitingCandidateCarriesApplicationLocked(candidate *peerConn) bool {
	if n.waitingApplicationPeerLocked(candidate) {
		return true
	}
	for _, peer := range n.peers {
		if peer.relayed && peer.viaPeerID == candidate.id && n.waitingApplicationPeerLocked(peer) {
			return true
		}
	}
	return false
}
