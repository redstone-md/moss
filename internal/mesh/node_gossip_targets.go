package mesh

import (
	"encoding/hex"
	"sort"

	"golang.org/x/crypto/blake2s"
)

// gossipTargets bounds control gossip independently of transport peer count.
// Hash ranking spreads different announcements across different neighbors.
func (n *Node) gossipTargets(key, excludePeerID string, limit int) []string {
	n.mu.RLock()
	peerIDs := make([]string, 0, len(n.peers))
	for peerID, peer := range n.peers {
		if peer != nil && peerID != excludePeerID {
			peerIDs = append(peerIDs, peerID)
		}
	}
	n.mu.RUnlock()
	peerIDs = filterPeerIDs(peerIDs, n.canSharePeerExchangeWithPeer)
	if limit = max(1, limit); len(peerIDs) <= limit {
		return peerIDs
	}
	keys := make(map[string]string, len(peerIDs))
	for _, peerID := range peerIDs {
		keys[peerID] = gossipPeerKey(key, peerID)
	}
	sort.Slice(peerIDs, func(i, j int) bool {
		if keys[peerIDs[i]] == keys[peerIDs[j]] {
			return peerIDs[i] < peerIDs[j]
		}
		return keys[peerIDs[i]] < keys[peerIDs[j]]
	})
	return peerIDs[:limit]
}

func gossipPeerKey(messageID, peerID string) string {
	h, _ := blake2s.New256(nil)
	_, _ = h.Write([]byte(messageID))
	_, _ = h.Write([]byte("|"))
	_, _ = h.Write([]byte(peerID))
	return hex.EncodeToString(h.Sum(nil))
}
