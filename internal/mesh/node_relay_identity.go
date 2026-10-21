package mesh

import "github.com/redstone-md/moss/internal/gossip"

func (n *Node) sendRelayControlWithIdentity(peer *peerConn, env gossip.Envelope) bool {
	return n.sendEnvelope(peer, n.peerAnnouncementEnvelope(n.localKnownPeer())) &&
		n.sendEnvelope(peer, env)
}

// Called after signature verification. Only a live direct endpoint can supply
// this exact key claim; a directory entry cannot manufacture its signature.
func (n *Node) rememberPeerSelfAnnouncement(peer *peerConn, env gossip.Envelope) {
	if peer == nil || peer.relayed || peer.session == nil || env.Type != gossip.TypePeerAnnounce ||
		env.AdvertisedPeerID != peer.id || env.AdvertisedAddr == "" || len(env.AdvertisedNoiseStatic) != 32 {
		return
	}
	key := peer.session.RemoteStaticPublic()
	if !equalBytes(key[:], env.AdvertisedNoiseStatic) {
		return
	}
	claim := gossip.Envelope{
		Type: env.Type, AdvertisedPeerID: env.AdvertisedPeerID, AdvertisedAddr: env.AdvertisedAddr,
		AdvertisedNATType: env.AdvertisedNATType, AdvertisedReachable: env.AdvertisedReachable,
		AdvertisedRelayCapable: env.AdvertisedRelayCapable,
		AdvertisedNoiseStatic:  append([]byte(nil), env.AdvertisedNoiseStatic...),
		AdvertisedSignature:    append([]byte(nil), env.AdvertisedSignature...),
	}
	n.mu.Lock()
	if n.peers[peer.id] == peer {
		peer.selfAnnouncement = &claim
	}
	n.mu.Unlock()
}

// Introduce the requested endpoint before relay control. Synchronous writes
// avoid an outbound queue letting control overtake the key claim. Lost or
// reordered UDP claims still fail closed at relayed-peer admission.
func (n *Node) introduceRelayEndpoint(recipient *peerConn, endpointID string) bool {
	n.mu.RLock()
	endpoint := n.peers[endpointID]
	var claim *gossip.Envelope
	if endpoint != nil {
		claim = endpoint.selfAnnouncement
	}
	n.mu.RUnlock()
	if claim == nil {
		// Legacy/forwarded sessions may already know each other's keys.
		return true
	}
	if recipient == nil || !n.canSharePeerExchangeWithPeer(recipient.id) {
		return false
	}
	return n.sendEnvelope(recipient, *claim)
}
