package mesh

import (
	"moss/internal/gossip"
)

func (n *Node) handleEnvelope(peer *peerConn, env gossip.Envelope) {
	if peer != nil && n.isPeerGraylisted(peer.id) {
		return
	}
	switch env.Type {
	case gossip.TypeGraft:
		n.pubsub.SetPeerSubscription(peer.id, env.Channel, true)
		if n.pubsub.IsLocalSubscriber(env.Channel) && n.eligibleForMeshCandidate(peer.id) {
			n.pubsub.SetMeshPeer(env.Channel, peer.id, true)
			n.sendRecentIHave(peer, env.Channel)
		} else if peer != nil {
			n.sendEnvelope(peer, gossip.Envelope{Type: gossip.TypePrune, Channel: env.Channel})
		}
	case gossip.TypePrune:
		n.pubsub.SetMeshPeer(env.Channel, peer.id, false)
	case gossip.TypeIHave:
		n.handleIHave(peer, env)
	case gossip.TypeIWant:
		n.handleIWant(peer, env)
	case gossip.TypeIDontWant:
		if !n.canGossipWithPeer(peer.id) {
			return
		}
		n.rememberSuppression(peer.id, env.MessageIDs, env.MessageID)
	case gossip.TypePeerAnnounce:
		n.handlePeerAnnounce(peer, env)
	case gossip.TypeSupernodeAnnounce:
		n.handleSupernodeStatus(peer, env, true)
	case gossip.TypeSupernodeRevoke:
		n.handleSupernodeStatus(peer, env, false)
	case gossip.TypeBindingRequest:
		n.handleBindingRequest(peer, env)
	case gossip.TypeBindingResponse:
		n.handleBindingResponse(env)
	case gossip.TypeReachabilityRequest:
		n.handleReachabilityRequest(peer, env)
	case gossip.TypeReachabilityResponse:
		n.handleReachabilityResponse(env)
	case gossip.TypeHolePunchCoord:
		n.handleHolePunchCoord(peer, env)
	case gossip.TypeRelayRequest:
		n.handleRelayRequest(peer, env)
	case gossip.TypeRelayAccept:
		n.handleRelayAccept(peer, env)
	case gossip.TypeRelayData:
		n.handleRelayData(peer, env)
	case gossip.TypeRelayClose:
		n.handleRelayClose(peer, env)
	case gossip.TypePublish:
		if n.isPeerBelowPublishThreshold(peer.id) {
			return
		}
		if env.Channel == "" || env.MessageID == "" {
			n.scoring.PenalizeInvalid(peer.id)
			return
		}
		if len(env.Payload) > n.config.Security.MaxMessageSizeBytes {
			n.scoring.PenalizeInvalid(peer.id)
			return
		}
		n.observeMeshDelivery(env.Channel, env.MessageID, peer.id)
		if !n.cache.StoreIfNew(env) {
			return
		}
		n.scoring.RewardFirstDelivery(peer.id)
		n.deliverLocal(env)
		n.broadcastEnvelope(env, peer.id)
		n.broadcastIHave(env.Channel, []string{env.MessageID}, peer.id)
		if len(env.Payload) > 1024 {
			n.broadcastIDontWant(env.Channel, []string{env.MessageID}, peer.id)
		}
	case gossip.TypePing:
		n.sendEnvelope(peer, gossip.Envelope{Type: gossip.TypePong, RequestID: env.RequestID})
	case gossip.TypePong:
		n.handlePong(peer, env)
	}
}

func (n *Node) deliverLocal(env gossip.Envelope) {
	if !n.pubsub.IsLocalSubscriber(env.Channel) {
		return
	}
	var sender [32]byte
	copy(sender[:], env.SenderID)
	n.dispatchCh <- dispatchMessage{
		channel: env.Channel,
		sender:  sender,
		data:    append([]byte(nil), env.Payload...),
	}
}

func (n *Node) broadcastEnvelope(env gossip.Envelope, excludePeerID string) bool {
	targets := n.pubsub.MeshPeers(env.Channel)
	if len(targets) == 0 {
		return false
	}
	return n.sendToPeers(filterPeerIDs(targets, func(peerID string) bool {
		return peerID != excludePeerID && n.canGossipWithPeer(peerID)
	}), env)
}

func (n *Node) broadcastFloodPublish(env gossip.Envelope, excludePeerID string) bool {
	meshPeers := n.pubsub.MeshPeers(env.Channel)
	nonMeshSubscribers := n.pubsub.NonMeshSubscribers(env.Channel)
	targets := make([]string, 0, len(meshPeers)+len(nonMeshSubscribers))
	seen := make(map[string]struct{}, len(meshPeers)+len(nonMeshSubscribers))
	for _, peerID := range append(meshPeers, nonMeshSubscribers...) {
		if peerID == excludePeerID || !n.canGossipWithPeer(peerID) {
			continue
		}
		if _, ok := seen[peerID]; ok {
			continue
		}
		seen[peerID] = struct{}{}
		targets = append(targets, peerID)
	}
	if len(targets) == 0 {
		return false
	}
	return n.sendToPeers(targets, env)
}

func filterPeerIDs(peerIDs []string, keep func(string) bool) []string {
	filtered := make([]string, 0, len(peerIDs))
	for _, peerID := range peerIDs {
		if keep(peerID) {
			filtered = append(filtered, peerID)
		}
	}
	return filtered
}
