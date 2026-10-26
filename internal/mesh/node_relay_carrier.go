package mesh

import "time"

// Conservative sealed-payload threshold: the relay JSON/base64, transport
// encryption and obfuscation also consume the physical datagram's MTU.
const relayDatagramPayloadBytes = 512

// Large relay sends prefer the existing stream transport on either endpoint,
// including ordinary hosts. Discovery and UDP-only fallback remain unchanged.
func (n *Node) preferRelayStream(peer *peerConn) *peerConn {
	if peer == nil || peer.session == nil || isStreamNetwork(peer.session.RemoteAddr()) {
		return peer
	}
	peer.streamUpgradeMu.Lock()
	defer peer.streamUpgradeMu.Unlock()
	n.mu.RLock()
	current, ctx, started, addr := n.peers[peer.id], n.rootCtx, n.started, n.knownPeers[peer.id].addr
	n.mu.RUnlock()
	if current != peer {
		return current
	}
	if !started || ctx == nil || addr == "" || time.Since(peer.streamUpgradeAt) < time.Minute {
		return peer
	}
	peer.streamUpgradeAt = time.Now()
	ctx, cancel := withTimeout(ctx, n.config.HandshakeTimeout())
	defer cancel()
	_ = n.connectPeerTCPWithHint(ctx, addr, peer.id)
	n.mu.RLock()
	current = n.peers[peer.id]
	n.mu.RUnlock()
	return current
}
