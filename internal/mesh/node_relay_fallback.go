package mesh

// Caller holds n.mu and has removed the closed incarnation. Reuse only an
// established alternative whose physical carrier is still admitted. This is
// directory continuity, not retransmission of data sent before a close.
func (n *Node) restoreAlternateRelayLocked(closing relayLocalSession, previous *peerConn) bool {
	var selected relayLocalSession
	for id, session := range n.relayLocals {
		via := n.peers[session.viaPeerID]
		if id == closing.sessionID || !session.established || session.remotePeerID != closing.remotePeerID ||
			via == nil || via.relayed || via.session == nil {
			continue
		}
		if selected.sessionID == "" || id < selected.sessionID {
			selected = session
		}
	}
	if selected.sessionID == "" {
		return false
	}
	peer, refused := n.registerRelayedPeerLocked(selected)
	if refused || peer == nil {
		return false
	}
	if previous.applicationUsed.Load() {
		peer.applicationUsed.Store(true)
	}
	return true
}
