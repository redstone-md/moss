package mesh

import "context"

// Carrier ownership belongs to the installation, independent of whether an old
// conversation is currently online. Only this attempt's admitted incarnation
// can replace the owner; unused old owners retire under the existing guards.
func (n *Node) retainWaitingRendezvous(ctx context.Context, peerID string, attempt uint64) {
	n.mu.Lock()
	peer := n.peers[peerID]
	if ctx.Err() != nil || !n.started || !n.waitingRequested() || n.activity.attempt != attempt || n.activity.candidate != peerID || !n.waitingCarrierAuthenticatedLocked(peer) {
		n.mu.Unlock()
		return
	}
	previous := n.activity.rendezvous
	if previous != nil && previous != peer && n.peers[previous.id] == previous && n.waitingCarrierAuthenticatedLocked(previous) && !n.waitingCandidateCarriesApplicationLocked(peer) {
		n.mu.Unlock()
		return
	}
	n.activity.rendezvous = peer
	n.mu.Unlock()
	if previous != nil && previous != peer {
		if n.removePeerWithPolicy(previous.id, previous.session, &attempt, true) {
			previous.closeSession()
		}
	}
}

// Missing historical targets retain recovery intent without preventing a true
// single-carrier Waiting state. Any existing useful route outside this carrier
// still makes handover incomplete and must survive before ordinary mesh trims.
// Caller holds Node.mu.
func (n *Node) waitingCarrierCanSettleLocked(via string) bool {
	if via == "" && len(n.activity.recovering) != 0 {
		return false
	}
	if via != "" && !n.waitingCarrierAuthenticatedLocked(n.peers[via]) {
		return false
	}
	for id, peer := range n.peers {
		if id != via && n.waitingApplicationPeerLocked(peer) && (!peer.relayed || peer.viaPeerID != via) {
			return false
		}
	}
	return true
}

func (n *Node) settledWaitingRendezvousLocked() string {
	owner := n.activity.rendezvous
	if n.started && n.waitingRequested() && owner != nil && n.peers[owner.id] == owner && n.waitingCarrierCanSettleLocked(owner.id) {
		return owner.id
	}
	return ""
}
