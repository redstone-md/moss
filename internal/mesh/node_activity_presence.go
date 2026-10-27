package mesh

import (
	"sort"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/overlay"
)

// A known healthy direct address keeps its original fast path. Lifecycle hosts,
// and ordinary explicit targets that lost an established relay, refresh their
// identity before a speculative direct handshake can briefly register and vanish.
func (n *Node) activityIdentityFirst(peerID string) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	previous := n.explicitTargets[peerID]
	retrying := previous != nil && previous.identityRetry
	return (n.activity.optedIn || retrying) && !n.waitingRequested() &&
		(n.knownPeers[peerID].addr == "" || n.peerDialFailures[peerID] > 0 || retrying)
}

// Refresh only our own records on the current attachment or useful partial
// carriers. Session snapshots coalesce retries and promptly replace stale hints
// without waiting for unrelated application targets or claiming Ready.
func (n *Node) refreshWaitingPresence(now time.Time) {
	n.mu.Lock()
	if !n.started || !n.waitingRequested() {
		n.mu.Unlock()
		return
	}
	peers := n.waitingPresencePeersLocked()
	if peers[0] == nil || (peers == n.activity.presencePeers && now.Before(n.activity.presenceAt)) {
		n.mu.Unlock()
		return
	}
	n.activity.presencePeers = peers
	n.activity.presenceAt = now.Add(overlayRepublishEvery)
	addresses := make([]string, 0, len(peers))
	for _, peer := range peers {
		if peer != nil {
			addresses = append(addresses, n.knownPeers[peer.id].addr)
		}
	}
	n.mu.Unlock()
	self, ok := n.localOverlayID()
	if !ok {
		return
	}
	hint := n.localReachabilityHint()
	hint.Attachments = addresses
	payload := encodeHint(hint)
	for _, peer := range peers {
		if peer == nil {
			continue
		}
		n.enqueueWaitingPresence(peer, self, payload)
		for _, topic := range n.pubsub.SnapshotLocal() {
			if n.topicDiscoveryAutomatic(topic) {
				n.enqueueWaitingPresence(peer, overlay.ChannelKey(topic), payload)
			}
		}
	}
}

// Caller holds Node.mu. Keep the committed carrier first, then a stable bounded
// selection of useful partial paths; initial ordinary mesh is not republished.
func (n *Node) waitingPresencePeersLocked() (peers [2]*peerConn) {
	attachment := n.peers[n.activity.attachment]
	count := 0
	if n.waitingPresencePeerAllowedLocked(attachment) {
		peers[0], count = attachment, 1
	}
	owner := n.activity.rendezvous
	if owner != attachment && owner != nil && n.peers[owner.id] == owner && n.waitingPresencePeerAllowedLocked(owner) {
		peers[count] = owner
		count++
	}
	ids := make([]string, 0, 2)
	for id, peer := range n.peers {
		if peer != attachment && peer != owner && n.waitingPresencePeerAllowedLocked(peer) {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	for _, id := range ids {
		if count == len(peers) {
			break
		}
		peers[count] = n.peers[id]
		count++
	}
	return peers
}

func (n *Node) waitingPresencePeerAllowedLocked(peer *peerConn) bool {
	return n.waitingCarrierAuthenticatedLocked(peer) && (peer == n.activity.rendezvous || peer.id == n.activity.attachment || n.waitingCandidateCarriesApplicationLocked(peer))
}

func (n *Node) waitingCarrierAuthenticatedLocked(peer *peerConn) bool {
	if peer == nil || peer.relayed || peer.session == nil || !n.isTrustedRelayCandidateLocked(peer.id) {
		return false
	}
	info := n.knownPeers[peer.id]
	if !info.verified || len(info.noiseStatic) != 32 || info.addr == "" {
		return false
	}
	if n.allowlist != nil {
		if _, allowed := n.allowlist[peer.id]; !allowed {
			return false
		}
	}
	return true
}

func (n *Node) enqueueWaitingPresence(peer *peerConn, key overlay.NodeID, payload []byte) {
	n.mu.RLock()
	selected := n.activity.presencePeers[0] == peer || n.activity.presencePeers[1] == peer
	current := n.started && n.waitingRequested() && selected && n.peers[peer.id] == peer && n.waitingPresencePeerAllowedLocked(peer)
	n.mu.RUnlock()
	if current {
		n.sendOrEnqueue(peer, gossip.Envelope{
			Type: gossip.TypeOverlayStore, OverlayKey: key[:], Payload: payload,
		})
	}
}
