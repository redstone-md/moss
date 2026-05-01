package mesh

import (
	"encoding/hex"
	"errors"
	"net"
	"sort"
	"time"

	"moss/internal/gossip"
	"moss/internal/nat"
)

func (n *Node) relayBucketFor(peerID string) *nat.TokenBucket {
	n.mu.Lock()
	defer n.mu.Unlock()
	bucket := n.relayBuckets[peerID]
	if bucket == nil {
		burst, sustained := n.relayRateLimits()
		bucket = nat.NewTokenBucket(burst, sustained)
		n.relayBuckets[peerID] = bucket
	}
	return bucket
}

func (n *Node) relayRateLimits() (int, int) {
	burst := n.config.NAT.RelayMaxBandwidthKBPS * 1024
	if burst <= 0 {
		burst = n.config.Security.RateLimitBurst
	}
	if n.config.Security.RateLimitBurst > 0 && n.config.Security.RateLimitBurst < burst {
		burst = n.config.Security.RateLimitBurst
	}
	if burst <= 0 {
		burst = 1024
	}
	sustained := burst / 4
	if n.config.Security.RateLimitSustained > 0 && n.config.Security.RateLimitSustained < sustained {
		sustained = n.config.Security.RateLimitSustained
	}
	if sustained <= 0 {
		sustained = max(1, burst/4)
	}
	return burst, sustained
}

func (n *Node) selectRelayPeer(targetPeerID string) (string, error) {
	candidates, err := n.selectRelayPeers(targetPeerID)
	if err != nil {
		return "", err
	}
	return candidates[0], nil
}

func (n *Node) selectRelayPeers(targetPeerID string) ([]string, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	candidates := make([]string, 0, len(n.peers))
	for peerID := range n.peers {
		if peerID == targetPeerID {
			continue
		}
		if !n.isTrustedRelayCandidateLocked(peerID) {
			continue
		}
		candidates = append(candidates, peerID)
	}
	if len(candidates) == 0 {
		return nil, errors.New("no relay-capable peer is connected")
	}
	sort.Slice(candidates, func(i, j int) bool {
		infoI := n.knownPeers[candidates[i]]
		infoJ := n.knownPeers[candidates[j]]
		if rankI, rankJ := relayCandidateRank(infoI), relayCandidateRank(infoJ); rankI != rankJ {
			return rankI > rankJ
		}
		scoreI := n.peerScore(candidates[i])
		scoreJ := n.peerScore(candidates[j])
		if scoreI != scoreJ {
			return scoreI > scoreJ
		}
		loadI := n.relaySessionCountViaLocked(candidates[i])
		loadJ := n.relaySessionCountViaLocked(candidates[j])
		if loadI != loadJ {
			return loadI < loadJ
		}
		return candidates[i] < candidates[j]
	})
	return candidates, nil
}

func (n *Node) isTrustedRelayCandidateLocked(peerID string) bool {
	info, ok := n.knownPeers[peerID]
	return ok && info.natTrusted && info.relayCapable && info.publicReachable
}

func (n *Node) relaySessionCountViaLocked(peerID string) int {
	count := 0
	for _, session := range n.relayLocals {
		if session.viaPeerID == peerID {
			count++
		}
	}
	return count
}

func relayCandidateRank(info knownPeer) int {
	rank := 0
	if info.relayCapable {
		rank += 4
	}
	if info.publicReachable {
		rank += 2
	}
	if info.natTrusted {
		switch info.natType {
		case nat.TypePublic, nat.TypeFullCone:
			rank++
		}
	}
	return rank
}

func (n *Node) peerScore(peerID string) float64 {
	base := n.scoring.Score(peerID)
	n.scoringMu.RLock()
	cb := n.scoringCB
	n.scoringMu.RUnlock()
	if cb == nil {
		return base
	}
	return cb(decodePeerID(peerID), base)
}

func (n *Node) shouldPreferRelayForTarget(targetPeerID string) bool {
	localProfile := n.natProfile.Load().(nat.Profile)
	n.mu.RLock()
	targetInfo, ok := n.knownPeers[targetPeerID]
	n.mu.RUnlock()
	if !ok {
		return false
	}
	if !targetInfo.natTrusted {
		return false
	}
	return shouldPreferRelayBetween(localProfile.Type, targetInfo.natType)
}

func shouldPreferRelayBetween(local, remote nat.Type) bool {
	localRestricted := local == nat.TypeSymmetric || local == nat.TypeCGNAT
	remoteRestricted := remote == nat.TypeSymmetric || remote == nat.TypeCGNAT
	return localRestricted && remoteRestricted
}

func (n *Node) isPeerBelowBaseline(peerID string) bool {
	return n.peerScore(peerID) < gossip.BaselineThreshold
}

func (n *Node) eligibleForMeshCandidate(peerID string) bool {
	if n.isPeerBelowBaseline(peerID) {
		return false
	}
	now := time.Now()
	n.mu.RLock()
	defer n.mu.RUnlock()
	peer := n.peers[peerID]
	if peer == nil {
		return false
	}
	if now.Before(peer.meshBlocked) {
		return false
	}
	if peer.lastRTT > peerLatencyPruneThreshold {
		return false
	}
	return peer.pingMisses == 0
}

func (n *Node) canGossipWithPeer(peerID string) bool {
	return n.peerScore(peerID) >= gossip.GossipThreshold
}

func (n *Node) isPeerBelowPublishThreshold(peerID string) bool {
	return n.peerScore(peerID) < gossip.PublishThreshold
}

func (n *Node) isPeerGraylisted(peerID string) bool {
	return n.peerScore(peerID) < gossip.GraylistThreshold
}

func (n *Node) canSharePeerExchangeWithPeer(peerID string) bool {
	return n.peerScore(peerID) >= gossip.BaselineThreshold
}

func (n *Node) meshGossipPeers(channel, excludePeerID string) []string {
	meshPeers := n.pubsub.MeshPeers(channel)
	selected := make([]string, 0, len(meshPeers))
	for _, peerID := range meshPeers {
		if peerID == excludePeerID || !n.canGossipWithPeer(peerID) {
			continue
		}
		selected = append(selected, peerID)
	}
	return selected
}

func (n *Node) recalculateIPColocationPenalties() {
	type peerAddr struct {
		id   string
		host string
	}
	n.mu.RLock()
	peers := make([]peerAddr, 0, len(n.peers))
	for peerID, peer := range n.peers {
		host, _, err := net.SplitHostPort(peer.addr)
		if err != nil {
			host = peer.addr
		}
		peers = append(peers, peerAddr{id: peerID, host: host})
	}
	n.mu.RUnlock()

	counts := make(map[string]int, len(peers))
	for _, peer := range peers {
		if !eligibleForIPColocationPenalty(peer.host) {
			continue
		}
		counts[peer.host]++
	}
	for _, peer := range peers {
		n.scoring.ApplyIPColocationPenalty(peer.id, counts[peer.host])
	}
}

func (n *Node) medianMeshScore(peers []string) float64 {
	if len(peers) == 0 {
		return 0
	}
	scores := make([]float64, 0, len(peers))
	for _, peerID := range peers {
		scores = append(scores, n.peerScore(peerID))
	}
	sort.Float64s(scores)
	middle := len(scores) / 2
	if len(scores)%2 == 1 {
		return scores[middle]
	}
	return (scores[middle-1] + scores[middle]) / 2
}

func decodePeerID(peerID string) [32]byte {
	var out [32]byte
	raw, err := hex.DecodeString(peerID)
	if err != nil {
		return out
	}
	copy(out[:], raw)
	return out
}
