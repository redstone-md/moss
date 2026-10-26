package mesh

import "github.com/redstone-md/moss/internal/gossip"

// Virtual peers have no physical session to close. Retire the exact failed
// incarnation so an ordinary explicit target can resolve its current carrier.
func (n *Node) retireUnresponsiveRelayPeer(peer *peerConn) {
	n.mu.Lock()
	if peer == nil || !peer.relayed || n.peers[peer.id] != peer || n.waitingRequested() || peer.pingMisses < peerDisconnectMissLimit {
		n.mu.Unlock()
		return
	}
	session, ok := n.relayLocals[peer.relaySessionID]
	if !ok || !session.established || session.remotePeerID != peer.id || session.viaPeerID != peer.viaPeerID {
		n.mu.Unlock()
		return
	}
	via := n.peers[session.viaPeerID]
	n.removeRelayedPeerLocked(session)
	delete(n.relayLocals, session.sessionID)
	delete(n.directProbes, peer.id)
	n.cleanupDepartedPeersLocked([]string{peer.id})
	n.mu.Unlock()
	if via != nil {
		n.sendEnvelope(via, gossip.Envelope{Type: gossip.TypeRelayClose, RelaySession: session.sessionID, RelaySource: n.localPeerID(), RelayTarget: session.remotePeerID})
	}
	n.enqueueEvent(EventPeerLeft, map[string]string{"peer": peer.id, "addr": peer.addr})
}
