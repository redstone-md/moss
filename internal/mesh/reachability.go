package mesh

import (
	"sort"
	"time"
)

func (n *Node) confirmReachability(addr string, deadline time.Time) bool {
	n.mu.RLock()
	peerIDs := n.reachabilityProbePeerIDsLocked()
	n.mu.RUnlock()
	for _, peerID := range peerIDs {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return false
		}
		if n.requestReachabilityProbe(peerID, addr, remaining) {
			return true
		}
	}
	return false
}

func (n *Node) reachabilityProbePeerIDsLocked() []string {
	peerIDs := make([]string, 0, len(n.peers))
	for peerID, peer := range n.peers {
		if n.canProbeExternalReachabilityLocked(peerID, peer) {
			peerIDs = append(peerIDs, peerID)
		}
	}
	sort.Strings(peerIDs)
	return peerIDs
}

func (n *Node) canProbeExternalReachabilityLocked(peerID string, peer *peerConn) bool {
	if peer == nil {
		return false
	}
	if peer.bootstrap {
		return true
	}
	info := n.knownPeers[peerID]
	if info.bootstrap || info.relayCapable || info.publicReachable {
		return true
	}
	return knownPeerAddrRank(peer.addr) >= 3
}
