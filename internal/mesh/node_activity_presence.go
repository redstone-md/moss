package mesh

import (
	"time"

	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/overlay"
)

// A known healthy direct address keeps its original fast path. An opted-in
// host resolves a missing/refused path before another speculative handshake
// can briefly register, cancel the explicit worker, and disappear again.
func (n *Node) activityIdentityFirst(peerID string) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	previous := n.explicitTargets[peerID]
	retrying := previous != nil && previous.identityRetry
	return n.activity.optedIn && !n.waitingRequested() &&
		(n.knownPeers[peerID].addr == "" || n.peerDialFailures[peerID] > 0 || retrying)
}

// Refresh only our own records on the retained carrier. Claiming the deadline
// under mu coalesces callers; the existing bounded outbound queue owns writes.
// No lookup, extra connection, third-party provider, or new timer is needed.
func (n *Node) refreshWaitingPresence(now time.Time) {
	n.mu.Lock()
	peer := n.peers[n.activity.attachment]
	if !n.started || !n.waitingRequested() || n.activity.actual != ActivityWaiting ||
		peer == nil || peer.relayed || now.Before(n.activity.presenceAt) {
		n.mu.Unlock()
		return
	}
	n.activity.presenceAt = now.Add(overlayRepublishEvery)
	n.mu.Unlock()
	self, ok := n.localOverlayID()
	if !ok {
		return
	}
	payload := encodeHint(n.localReachabilityHint())
	n.enqueueWaitingPresence(peer, self, payload)
	for _, topic := range n.pubsub.SnapshotLocal() {
		if n.topicDiscoveryAutomatic(topic) {
			n.enqueueWaitingPresence(peer, overlay.ChannelKey(topic), payload)
		}
	}
}

func (n *Node) enqueueWaitingPresence(peer *peerConn, key overlay.NodeID, payload []byte) {
	n.mu.RLock()
	current := n.started && n.waitingRequested() && n.activity.attachment == peer.id
	n.mu.RUnlock()
	if current {
		n.sendOrEnqueue(peer, gossip.Envelope{
			Type: gossip.TypeOverlayStore, OverlayKey: key[:], Payload: payload,
		})
	}
}
