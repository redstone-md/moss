package mesh

import "time"

// ConnectToPeer registers peerID as an explicit priority target: the node
// dials it immediately and the maintenance loop keeps retrying (direct first,
// relay fallback) until a connection exists. Explicit targets bypass the
// discovery ranking and the public-public glare rule — those govern organic
// mesh growth, while an explicit target is the application saying "this
// specific peer matters" (e.g. a DM counterpart on the room-blind substrate,
// which organic discovery would only ever reach by chance). The registration
// survives disconnects and is dropped on Stop.
func (n *Node) ConnectToPeer(peerID string) int32 {
	if len(peerID) != 64 || peerID == n.localPeerID() {
		return MOSS_ERR_CONFIG_INVALID
	}
	n.mu.Lock()
	if !n.started {
		n.mu.Unlock()
		return MOSS_ERR_NOT_STARTED
	}
	n.explicitTargets[peerID] = time.Now()
	n.mu.Unlock()
	go n.dialExplicitTarget(peerID)
	return MOSS_OK
}

// dialExplicitTargets retries registered targets from the peer-maintenance
// tick, reusing the discovery cooldown so a target is attempted at most once
// per handshake window.
func (n *Node) dialExplicitTargets() {
	now := time.Now()
	cooldown := n.config.HandshakeTimeout()
	if cooldown < n.config.Heartbeat() {
		cooldown = n.config.Heartbeat()
	}
	if cooldown <= 0 {
		cooldown = time.Second
	}
	n.mu.Lock()
	due := make([]string, 0, len(n.explicitTargets))
	for peerID, lastDial := range n.explicitTargets {
		if _, connected := n.peers[peerID]; connected {
			continue
		}
		if now.Sub(lastDial) < cooldown {
			continue
		}
		n.explicitTargets[peerID] = now
		due = append(due, peerID)
	}
	n.mu.Unlock()
	for _, peerID := range due {
		go n.dialExplicitTarget(peerID)
	}
}

// dialExplicitTarget runs one connection attempt: the same direct → relay
// pipeline as dialKnownPeer. A relayed connection is enough — promoteRelayPeers
// keeps probing it for a direct upgrade.
func (n *Node) dialExplicitTarget(peerID string) {
	n.mu.RLock()
	_, connected := n.peers[peerID]
	n.mu.RUnlock()
	if connected {
		return
	}
	if !n.tryDirectConnect(peerID, n.config.HandshakeTimeout()) &&
		n.establishedRelaySession(peerID) == "" {
		_, _ = n.OpenRelaySessionAny(peerID, n.config.HandshakeTimeout())
	}
}
