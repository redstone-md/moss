package mesh

import (
	"errors"
	"net"
	"sort"
	"time"

	"github.com/redstone-md/moss/internal/geo"
	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/nat"
)

// relayBucketFor returns the per-source bandwidth bucket, creating it on the
// source's first charge. Every relayed packet from an established source
// walks the hit path, so the read lock carries it: a busy supernode's
// forwarding rate must not queue on the global write lock — serializing with
// gossip, session bookkeeping, and every dispatch worker in the node — just
// to read a map. The write lock appears only on a new source's first packet,
// where the double-check keeps a concurrent first charge from allocating two
// buckets for one peer.
func (n *Node) relayBucketFor(peerID string) *nat.TokenBucket {
	n.mu.RLock()
	bucket := n.relayBuckets[peerID]
	n.mu.RUnlock()
	if bucket != nil {
		return bucket
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if bucket = n.relayBuckets[peerID]; bucket != nil {
		return bucket
	}
	burst, sustained := n.relayRateLimits()
	bucket = nat.NewTokenBucket(burst, sustained)
	n.relayBuckets[peerID] = bucket
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
	// Sustained refill. Two sources, in precedence order:
	//   1. NAT.RelaySustainedKiBPS — an explicit operator directive (a
	//      volunteer raising supply). It wins outright, clamped only to the
	//      burst ceiling so it can never exceed the token-bucket capacity.
	//   2. otherwise burst/4 (64 KiB/s at the 256 KiB default), and the
	//      legacy Security.RateLimitSustained acts as a governor cap on that
	//      derived value — the pre-existing behavior.
	// The legacy clamp deliberately does NOT apply to (1): otherwise an
	// operator who set the new field could not raise relay throughput past
	// Security.RateLimitSustained without also discovering and editing an
	// unrelated security knob, defeating the point of exposing the field.
	if wanted := n.config.NAT.RelaySustainedKiBPS * 1024; n.config.NAT.RelaySustainedKiBPS > 0 {
		sustained := clampInt(wanted, 1, burst)
		return burst, sustained
	}
	sustained := clampInt(burst/4, 1, burst)
	if n.config.Security.RateLimitSustained > 0 && n.config.Security.RateLimitSustained < sustained {
		sustained = n.config.Security.RateLimitSustained
	}
	return burst, max(1, sustained)
}

// clampInt bounds v to [lo, hi].
func clampInt(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// hostIP parses the IP out of a "host:port" (or bare host) address, or nil.
func hostIP(addr string) net.IP {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	return net.ParseIP(host)
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
	for peerID, peer := range n.peers {
		if peer == nil || peer.relayed {
			continue
		}
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
	// Prefer a relay geographically close to the target, shortening the
	// relay↔target leg. Unknown IPs carry no preference, so this only ever
	// breaks ties in favour of proximity — it never excludes a candidate.
	// Builds without the `geoip` tag embed no GeoLite2 database, so
	// geo.Proximity is neutral there and ordering falls through to
	// score/load alone.
	targetIP := hostIP(n.knownPeers[targetPeerID].addr)
	// Precompute each candidate's relay-session load once: the sort below
	// runs O(N·logN) comparisons, and rescanning every relayLocals session
	// per comparison made that O(N·logN·S) while n.mu is held for read.
	// One O(S) pass up front keeps each comparison to a map lookup.
	relayLoad := n.relaySessionCountsViaLocked()
	sort.Slice(candidates, func(i, j int) bool {
		infoI := n.knownPeers[candidates[i]]
		infoJ := n.knownPeers[candidates[j]]
		if rankI, rankJ := relayCandidateRank(infoI), relayCandidateRank(infoJ); rankI != rankJ {
			return rankI > rankJ
		}
		if targetIP != nil {
			proxI := geo.Proximity(hostIP(infoI.addr), targetIP)
			proxJ := geo.Proximity(hostIP(infoJ.addr), targetIP)
			if proxI != proxJ {
				return proxI > proxJ
			}
		}
		scoreI := n.peerScore(candidates[i])
		scoreJ := n.peerScore(candidates[j])
		if scoreI != scoreJ {
			return scoreI > scoreJ
		}
		loadI := relayLoad[candidates[i]]
		loadJ := relayLoad[candidates[j]]
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

// relaySessionCountsViaLocked reports how many relay sessions each peer is
// currently serving, in a single pass over relayLocals. It requires n.mu to
// be held (read or write): selectRelayPeers builds this map once before
// sorting so its comparator reads load from the map instead of rescanning
// relayLocals for every comparison.
func (n *Node) relaySessionCountsViaLocked() map[string]int {
	counts := make(map[string]int, len(n.relayLocals))
	for _, session := range n.relayLocals {
		counts[session.viaPeerID]++
	}
	return counts
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
	if n.scoring == nil {
		return 0
	}
	// The callback read stays behind scoringMu — SetScoringCallback fires
	// once at startup, so this is an uncontended RLock on a lock that no
	// hot path ever writes. The callback itself hands off to
	// AdjustedScore, which memoizes the (peer, base) → adjusted result so
	// the dozens of threshold gates and sort comparators that funnel here
	// per envelope neither re-invoke the (FFI-hosted) callback nor
	// re-decode the hex peer key on every comparison.
	n.scoringMu.RLock()
	cb := n.scoringCB
	n.scoringMu.RUnlock()
	if cb == nil {
		return n.scoring.Score(peerID)
	}
	return n.scoring.AdjustedScore(peerID, cb)
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
	if peer.relayed {
		return true
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

// recalculateIPColocationPenalties recomputes every peer's IP-colocation
// penalty on each join and leave. The peer snapshot is taken under n.mu and
// released before the scoring engine is touched, and the whole batch is
// applied under a single scoring.mu acquisition — one lock per recalculation,
// not one per peer.
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

	penalties := make(map[string]int, len(peers))
	for _, peer := range peers {
		penalties[peer.id] = counts[peer.host]
	}
	n.scoring.ApplyIPColocationPenalties(penalties)
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

// decodePeerID is the historical mesh-side name for the hex peer-key
// decode; tests reference it directly, so it now delegates to the single
// gossip-side implementation instead of keeping a second copy.
func decodePeerID(peerID string) [32]byte {
	return gossip.DecodePeerKey(peerID)
}
