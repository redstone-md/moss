package mesh

import (
	"sort"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/nat"
)

type discoveredPeerTarget struct {
	peerID string
	addr   string
	info   knownPeer
}

func (n *Node) discoveredPeerTargets() []discoveredPeerTarget {
	now := time.Now()
	cooldown := n.config.HandshakeTimeout()
	if cooldown < n.config.Heartbeat() {
		cooldown = n.config.Heartbeat()
	}
	if cooldown <= 0 {
		cooldown = time.Second
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.directPeerCountLocked() >= n.config.MaxPeers {
		return nil
	}

	localID := n.localPeerID()
	localPublic := n.natProfile.Load().(nat.Profile).PublicReachable

	targets := make([]discoveredPeerTarget, 0, min(len(n.knownPeers), n.config.MaxPeers))
	for peerID, info := range n.knownPeers {
		if peerID == localID || info.addr == "" {
			continue
		}
		if !info.verified && !info.thirdPartyDialable {
			continue
		}
		if _, connected := n.peers[peerID]; connected {
			continue
		}
		// Glare avoidance: when both ends are publicly dialable, only the
		// lower-id node initiates. Otherwise both dial each other's listen port,
		// producing two duplicate sessions; the direction-based dedup then closes
		// the redundant one on each side, which empties that peer slot and
		// triggers an immediate redial — a self-sustaining connect/disconnect
		// oscillation that got worse with round-trip latency between the pair.
		// The higher-id node relies on the lower-id node's inbound dial instead.
		// If either end is not publicly reachable the inbound dial may not land,
		// so we keep dialing regardless to preserve connectivity.
		if localPublic && info.publicReachable && localID > peerID {
			continue
		}
		lastDial := n.peerDials[peerID]
		if !lastDial.IsZero() && now.Sub(lastDial) < cooldown {
			continue
		}
		targets = append(targets, discoveredPeerTarget{
			peerID: peerID,
			addr:   info.addr,
			info:   info,
		})
	}

	// Neutral ordering: bootstrap seeds first (they are how the node joins at
	// all), then score / recency. Relay-capable peers get NO blanket priority —
	// when every node dials supernodes first the mesh degrades into a star that
	// funnels the whole network's gossip through them. Relay availability is
	// covered by the small quota in selectDialTargets instead.
	sort.Slice(targets, func(i, j int) bool {
		if targets[i].info.bootstrap != targets[j].info.bootstrap {
			return targets[i].info.bootstrap
		}
		scoreI := n.peerScore(targets[i].peerID)
		scoreJ := n.peerScore(targets[j].peerID)
		if scoreI != scoreJ {
			return scoreI > scoreJ
		}
		if !targets[i].info.lastSeen.Equal(targets[j].info.lastSeen) {
			return targets[i].info.lastSeen.After(targets[j].info.lastSeen)
		}
		return targets[i].peerID < targets[j].peerID
	})

	limit := n.config.GossipSub.DOut
	if limit <= 0 {
		limit = 2
	}
	available := n.config.MaxPeers - n.directPeerCountLocked()
	if available < limit {
		limit = available
	}
	selected := selectDialTargets(targets, limit, relayDialQuota-n.relayCapableConnectedLocked())
	for _, target := range selected {
		n.peerDials[target.peerID] = now
	}
	return selected
}

// relayDialQuota is how many relay-capable connections a node keeps alive for
// the relay fallback path (and relay_ready); beyond it, relay-capable peers
// are dialed only when no plain candidate is available.
const relayDialQuota = 2

func (n *Node) relayCapableConnectedLocked() int {
	count := 0
	for peerID, peer := range n.peers {
		if peer == nil || peer.relayed {
			continue
		}
		if n.knownPeers[peerID].relayCapable {
			count++
		}
	}
	return count
}

// selectDialTargets takes up to limit targets from the ordered candidate list.
// While the relay quota has a deficit, that many slots are RESERVED for the
// best relay-capable candidates regardless of where they sort — otherwise a
// handful of fresher plain peers could starve the relay fallback. The rest of
// the slots go to bootstrap/plain candidates in ranking order, and leftover
// relay-capable candidates fill only what remains.
func selectDialTargets(targets []discoveredPeerTarget, limit, relayDeficit int) []discoveredPeerTarget {
	if limit <= 0 {
		return nil
	}
	selected := make([]discoveredPeerTarget, 0, limit)
	taken := make(map[string]bool, limit)
	for _, target := range targets {
		if relayDeficit <= 0 || len(selected) == limit {
			break
		}
		if target.info.relayCapable {
			selected = append(selected, target)
			taken[target.peerID] = true
			relayDeficit--
		}
	}
	skipped := make([]discoveredPeerTarget, 0)
	for _, target := range targets {
		if len(selected) == limit {
			break
		}
		if taken[target.peerID] {
			continue
		}
		if target.info.relayCapable && !target.info.bootstrap {
			skipped = append(skipped, target)
			continue
		}
		selected = append(selected, target)
	}
	for _, target := range skipped {
		if len(selected) == limit {
			break
		}
		selected = append(selected, target)
	}
	return selected
}

func (n *Node) dialKnownPeer(peerID, addr string) {
	_ = addr
	// Discovered peers use direct/NAT/hole-punch first. If that path does not
	// materialize, promote an encrypted relay session into the pubsub peer set.
	started := time.Now()
	if n.tryDirectConnect(peerID, n.config.HandshakeTimeout()) {
		n.reportConnectAttempt(outcomeDirect, reasonNone, started, false)
	} else if n.establishedRelaySession(peerID) != "" {
		n.reportConnectAttempt(outcomeRelayed, reasonNone, started, false)
	} else if _, err := n.OpenRelaySessionAny(peerID, n.config.HandshakeTimeout()); err == nil {
		n.reportConnectAttempt(outcomeRelayed, reasonPunchTimeout, started, false)
	} else {
		n.reportConnectAttempt(outcomeFailed, reasonNoRelayPeer, started, false)
	}
	n.mu.Lock()
	delete(n.peerDials, peerID)
	n.mu.Unlock()
}

func (n *Node) rememberSuppression(peerID string, ids []string, fallback string) {
	if len(ids) == 0 && fallback != "" {
		ids = []string{fallback}
	}
	if len(ids) == 0 {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	entry := n.suppress[peerID]
	if entry == nil {
		entry = make(map[string]time.Time)
		n.suppress[peerID] = entry
	}
	now := time.Now()
	for _, id := range ids {
		if _, ok := entry[id]; !ok && len(entry) >= maxSuppressionEntriesPerPeer {
			continue
		}
		entry[id] = now
	}
}

func (n *Node) isSuppressed(peerID, messageID string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	entry := n.suppress[peerID]
	if entry == nil {
		return false
	}
	ts, ok := entry[messageID]
	if !ok {
		return false
	}
	if time.Since(ts) > 2*time.Minute {
		delete(entry, messageID)
		return false
	}
	return true
}

func (n *Node) maintainTopicMesh(channel string) {
	if !n.pubsub.IsLocalSubscriber(channel) {
		return
	}
	n.ensureTopicMeshMinimum(channel)
	n.opportunisticGraft(channel)
	n.pruneTopicMeshExcess(channel)
	n.gossipRecentMessages(channel)
	n.maybeDiscoverTopicPeers(channel)
}

func (n *Node) ensureTopicMeshMinimum(channel string) {
	meshPeers := n.pubsub.MeshPeers(channel)
	if len(meshPeers) >= n.config.GossipSub.DLo {
		return
	}
	candidates := n.selectMeshCandidates(channel, n.config.GossipSub.D-len(meshPeers))
	for _, peerID := range candidates {
		n.mu.RLock()
		peer := n.peers[peerID]
		n.mu.RUnlock()
		if peer == nil {
			continue
		}
		n.pubsub.SetMeshPeer(channel, peerID, true)
		n.sendEnvelope(peer, gossip.Envelope{Type: gossip.TypeGraft, Channel: channel})
		n.sendRecentIHave(peer, channel)
	}
}

func (n *Node) pruneTopicMeshExcess(channel string) {
	meshPeers := n.pubsub.MeshPeers(channel)
	if len(meshPeers) <= n.config.GossipSub.DHigh {
		return
	}
	sort.Slice(meshPeers, func(i, j int) bool {
		scoreI := n.peerScore(meshPeers[i])
		scoreJ := n.peerScore(meshPeers[j])
		if scoreI == scoreJ {
			return meshPeers[i] > meshPeers[j]
		}
		return scoreI < scoreJ
	})
	excess := len(meshPeers) - n.config.GossipSub.D
	if excess <= 0 {
		excess = len(meshPeers) - n.config.GossipSub.DHigh
	}
	if excess <= 0 {
		return
	}
	outboundLeft := n.countOutboundMesh(channel)
	for _, peerID := range meshPeers {
		if excess == 0 {
			return
		}
		n.mu.RLock()
		peer := n.peers[peerID]
		n.mu.RUnlock()
		if peer == nil {
			continue
		}
		if peer.outbound && outboundLeft <= n.config.GossipSub.DOut {
			continue
		}
		n.pubsub.SetMeshPeer(channel, peerID, false)
		n.sendEnvelope(peer, gossip.Envelope{Type: gossip.TypePrune, Channel: channel})
		if peer.outbound {
			outboundLeft--
		}
		excess--
	}
}

func (n *Node) selectMeshCandidates(channel string, limit int) []string {
	if limit <= 0 {
		return nil
	}
	candidates := n.pubsub.NonMeshSubscribers(channel)
	if len(candidates) == 0 {
		n.mu.RLock()
		candidates = make([]string, 0, len(n.peers))
		for peerID := range n.peers {
			if n.pubsub.InMesh(channel, peerID) {
				continue
			}
			candidates = append(candidates, peerID)
		}
		n.mu.RUnlock()
	}
	if len(candidates) == 0 {
		return nil
	}
	sort.Slice(candidates, func(i, j int) bool {
		outI := n.isOutboundPeer(candidates[i])
		outJ := n.isOutboundPeer(candidates[j])
		if outI != outJ {
			return outI
		}
		scoreI := n.peerScore(candidates[i])
		scoreJ := n.peerScore(candidates[j])
		if scoreI == scoreJ {
			return candidates[i] < candidates[j]
		}
		return scoreI > scoreJ
	})
	filtered := make([]string, 0, len(candidates))
	for _, peerID := range candidates {
		if !n.eligibleForMeshCandidate(peerID) {
			continue
		}
		filtered = append(filtered, peerID)
		if len(filtered) == limit {
			break
		}
	}
	return filtered
}

func (n *Node) opportunisticGraft(channel string) {
	meshPeers := n.pubsub.MeshPeers(channel)
	if len(meshPeers) < 2 {
		return
	}
	if n.medianMeshScore(meshPeers) >= 1.0 {
		return
	}
	candidates := n.selectHighScoringCandidates(channel, 2, 1.0)
	for _, peerID := range candidates {
		n.mu.RLock()
		peer := n.peers[peerID]
		n.mu.RUnlock()
		if peer == nil {
			continue
		}
		n.pubsub.SetMeshPeer(channel, peerID, true)
		n.sendEnvelope(peer, gossip.Envelope{Type: gossip.TypeGraft, Channel: channel})
		n.sendRecentIHave(peer, channel)
	}
}

func (n *Node) selectHighScoringCandidates(channel string, limit int, threshold float64) []string {
	if limit <= 0 {
		return nil
	}
	candidates := n.selectMeshCandidates(channel, n.config.MaxPeers)
	filtered := make([]string, 0, len(candidates))
	for _, peerID := range candidates {
		if !n.eligibleForMeshCandidate(peerID) {
			continue
		}
		if n.peerScore(peerID) <= threshold {
			continue
		}
		filtered = append(filtered, peerID)
		if len(filtered) == limit {
			break
		}
	}
	return filtered
}

func (n *Node) countOutboundMesh(channel string) int {
	count := 0
	for _, peerID := range n.pubsub.MeshPeers(channel) {
		if n.isOutboundPeer(peerID) {
			count++
		}
	}
	return count
}

func (n *Node) isOutboundPeer(peerID string) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	peer := n.peers[peerID]
	return peer != nil && peer.outbound
}
