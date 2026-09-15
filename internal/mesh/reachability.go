package mesh

import "sort"

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
