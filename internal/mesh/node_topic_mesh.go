package mesh

import (
	"time"

	"github.com/redstone-md/moss/internal/gossip"
)

func (n *Node) handleTopicGraft(peer *peerConn, channel string) {
	// Scoring may call host code, so evaluate it outside the node lock.
	score := n.peerScore(peer.id)
	n.mu.RLock()
	currentPeer := n.topicPeerLocked(peer)
	if currentPeer == nil {
		n.mu.RUnlock()
		return
	}
	current := snapshotMeshCandidate(currentPeer, time.Now())
	current.score = score
	n.pubsub.SetPeerSubscription(peer.id, channel, true)
	joined := current.eligible() && n.pubsub.IsLocalSubscriber(channel) &&
		!n.pubsub.InBackoff(channel, peer.id, time.Now())
	if joined {
		n.pubsub.SetMeshPeer(channel, peer.id, true)
	}
	n.mu.RUnlock()
	if joined {
		n.sendRecentIHave(peer, channel)
	} else {
		n.sendEnvelope(peer, gossip.Envelope{Type: gossip.TypePrune, Channel: channel})
	}
}

func (n *Node) handleTopicPrune(peer *peerConn, channel string) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.topicPeerLocked(peer) != nil {
		n.pubsub.Prune(channel, peer.id, time.Now().Add(meshGraftRetryInterval))
	}
}

// The caller holds n.mu. Relay gossip can arrive over another live session
// for the same remote, including while its preferred path upgrades to direct.
func (n *Node) topicPeerLocked(origin *peerConn) *peerConn {
	if origin == nil {
		return nil
	}
	current := n.peers[origin.id]
	if current == nil {
		return nil
	}
	if origin.relayed {
		session, ok := n.relayLocals[origin.relaySessionID]
		if ok && session.established && session.remotePeerID == origin.id &&
			session.viaPeerID == origin.viaPeerID {
			return current
		}
		return nil
	}
	if current == origin {
		return current
	}
	return nil
}

// Serialize retry cleanup with markMeshGrafted and queued control envelopes.
func (n *Node) unsubscribeTopic(channel string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.pubsub.Unsubscribe(channel)
	for _, peer := range n.peers {
		if peer != nil {
			delete(peer.graftedAt, channel)
		}
	}
}
