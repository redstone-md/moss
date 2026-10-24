package mesh

import "encoding/hex"

// RoomPeerDiscovery controls discovery work, never membership or authorization.
type RoomPeerDiscovery int32

const (
	RoomDiscoveryAutomatic RoomPeerDiscovery = iota
	RoomDiscoveryKnownPeers
)

// SetRoomPeerDiscovery replaces a room's discovery hints. KnownPeers requires
// the complete nonempty recipient set; callers retain ConnectToPeer requests
// for these identities. Subscriptions received from other peers still work.
func (n *Node) SetRoomPeerDiscovery(room string, mode RoomPeerDiscovery, peerIDs []string) int32 {
	peers, valid := discoveryPeerSet(mode, peerIDs, n.localPeerID())
	if !valid {
		return MOSS_ERR_CONFIG_INVALID
	}
	if room == "" {
		room = n.meshID
	}
	n.mu.Lock()
	if room != n.meshID && n.rooms[room] == nil {
		n.mu.Unlock()
		return MOSS_ERR_NOT_IN_ROOM
	}
	if n.roomDiscovery == nil {
		n.roomDiscovery = make(map[string]map[string]struct{})
	}
	wasKnown := n.roomDiscovery[room] != nil
	if mode == RoomDiscoveryAutomatic {
		delete(n.roomDiscovery, room)
	} else {
		n.roomDiscovery[room] = peers
	}
	n.mu.Unlock()
	if wasKnown && mode == RoomDiscoveryAutomatic {
		n.wakeRoomDiscovery(room)
	}
	return MOSS_OK
}

// The caller holds mu. Missing mappings and policies preserve legacy behavior.
func (n *Node) topicDiscoveryPeersLocked(topic string) map[string]struct{} {
	sub, ok := n.subChannels[topic]
	if !ok {
		return nil
	}
	room := sub.room
	if room == "" {
		room = n.meshID
	}
	return n.roomDiscovery[room]
}

func (n *Node) topicAllowsPeerLocked(topic, peer string) bool {
	peers := n.topicDiscoveryPeersLocked(topic)
	if peers == nil || n.pubsub.HasPeerSubscription(peer, topic) {
		return true
	}
	_, known := peers[peer]
	return known
}

func (n *Node) topicDiscoveryAutomatic(topic string) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.topicDiscoveryPeersLocked(topic) == nil
}

// Only opted-in recipients replace topic rendezvous with identity lookup.
// The existing explicit-target worker owns its retry spacing and cancellation.
func (n *Node) roomNeedsPeerIdentity(peerID string) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	for _, peers := range n.roomDiscovery {
		if _, known := peers[peerID]; known {
			return true
		}
	}
	return false
}

func discoveryPeerSet(mode RoomPeerDiscovery, ids []string, self string) (map[string]struct{}, bool) {
	if mode != RoomDiscoveryAutomatic && mode != RoomDiscoveryKnownPeers ||
		(mode == RoomDiscoveryKnownPeers) != (len(ids) > 0) {
		return nil, false
	}
	peers := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		key, err := hex.DecodeString(id)
		if err != nil || len(key) != 32 || hex.EncodeToString(key) == self {
			return nil, false
		}
		peers[hex.EncodeToString(key)] = struct{}{}
	}
	return peers, true
}

func (n *Node) wakeRoomDiscovery(room string) {
	n.mu.RLock()
	topics := make([]string, 0)
	for topic, sub := range n.subChannels {
		if sub.room == room || sub.room == "" && room == n.meshID {
			topics = append(topics, topic)
		}
	}
	n.mu.RUnlock()
	n.overlayMu.Lock()
	for _, topic := range topics {
		delete(n.overlayDiscovery, topic)
	}
	n.overlayMu.Unlock()
	// Coalesce resets on the existing publisher; no new retry loop.
	select {
	case n.overlayRepublish <- struct{}{}:
	default:
	}
}
