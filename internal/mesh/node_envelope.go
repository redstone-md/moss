package mesh

import (
	"github.com/redstone-md/moss/internal/gossip"
)

// A peer may only cost us so much announcement traffic.
//
// announceRatePerSecond is far above anything a correct peer needs: a node
// re-announces itself about every 10s and forwards a peer's state at most once
// per 10s, so even a large mesh stays orders of magnitude below this. It is a
// ceiling on damage, not a schedule.
const (
	announceRatePerSecond = 20
	announceBurst         = 60
)

// isAnnounceType reports whether an envelope is announcement traffic — the kind
// that is redundant by design, so discarding a surplus one costs nothing.
func isAnnounceType(t gossip.EnvelopeType) bool {
	switch t {
	case gossip.TypePeerAnnounce, gossip.TypeSupernodeAnnounce, gossip.TypeSupernodeRevoke:
		return true
	}
	return false
}

func (n *Node) handleEnvelope(peer *peerConn, env gossip.Envelope) {
	if peer != nil && n.isPeerGraylisted(peer.id) {
		return
	}
	// Charge announcements against the peer's budget BEFORE doing any work on
	// them, and drop the surplus.
	//
	// Handling one costs an Ed25519 verification and the node's central lock.
	// readPeer dispatches synchronously, so at ~900 announcements a second — what
	// the fleet actually sent — the read loop cannot keep up, the 256-packet
	// stream buffer fills, and every packet behind it is discarded without a
	// trace. The pings among them are why sessions die at six misses with a
	// healthy connection.
	//
	// Not re-telling unvouched announcements stops US from feeding that flood.
	// This is the other half: a node must survive a peer that floods it whatever
	// the reason — an old build, a broken client, or malice — rather than depend
	// on every peer being well-behaved. An announcement is redundant by design, so
	// dropping a surplus one costs nothing; being unable to read is what costs.
	if peer != nil && isAnnounceType(env.Type) && peer.announceBudget != nil && !peer.announceBudget.Allow(1) {
		n.countInbound("__announce_throttled__")
		return
	}
	switch env.Type {
	case gossip.TypeOverlayFindNode:
		n.handleOverlayFindNode(peer, env)
	case gossip.TypeOverlayFindValue:
		n.handleOverlayFindValue(peer, env)
	case gossip.TypeOverlayStore:
		n.handleOverlayStore(peer, env)
	case gossip.TypeOverlayNodes, gossip.TypeOverlayValues:
		n.handleOverlayResponse(env)
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
	case gossip.TypeStatDelta:
		n.handleStatDelta(peer, env)
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
	// Open the room seal; a payload we cannot authenticate (wrong room / PSK) is
	// dropped rather than handed up.
	plaintext, ok := n.openRoom(env.Payload)
	if !ok {
		return
	}
	var sender [32]byte
	copy(sender[:], env.SenderID)
	n.dispatchCh <- dispatchMessage{
		// Hand the application its bare channel, not the opaque room topic.
		channel: n.localChannel(env.Channel),
		sender:  sender,
		data:    plaintext,
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
