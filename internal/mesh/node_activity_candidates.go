package mesh

import (
	"context"
	"sort"
)

type waitingCandidate struct {
	id, addr  string
	connected bool
	stream    bool
}

// Caller holds n.mu. A refused UDP peer must not receive a confirm probe
// before this same policy is revalidated at final physical admission.
func (n *Node) waitingAdmissionAllowedLocked(peerID string, outbound bool, attempt uint64) bool {
	peer := n.peers[peerID]
	if n.waitingRequested() && n.waitingRecoveryBoundedLocked() && (peer == nil || peer.relayed) && n.directPeerCountLocked() >= 2 {
		return false
	}
	if !n.waitingRequested() || peerID == n.activity.attachment || peerID == n.activity.candidate {
		return true
	}
	return outbound && attempt != 0 && attempt == n.activity.attempt &&
		n.activity.candidateAddr != "" && n.activity.candidate == ""
}

// Initial ordinary mesh may still be draining; subsequent handovers retain at
// most the current carrier plus one candidate, including partial migrations.
func (n *Node) waitingRecoveryBoundedLocked() bool {
	return n.activity.migrationOwned || n.activity.actual == ActivityWaiting || n.activity.attachment != "" || len(n.activity.recovering) != 0
}

// Cache/config addresses are candidates, never evidence of relay capability.
// Only current mesh admission and signed capability make one ready.
func (n *Node) waitingCandidatesLocked() []waitingCandidate {
	byAddr := make(map[string]waitingCandidate)
	for id, info := range n.knownPeers {
		if id == n.localPeerID() || info.addr == "" || !n.isTrustedRelayCandidateLocked(id) {
			continue
		}
		if n.allowlist != nil {
			if _, ok := n.allowlist[id]; !ok {
				continue
			}
		}
		peer := n.peers[id]
		connected := peer != nil && !peer.relayed
		stream := connected && peer.session != nil && isStreamNetwork(peer.session.RemoteAddr())
		byAddr[info.addr] = waitingCandidate{id: id, addr: info.addr, connected: connected, stream: stream}
	}
	add := func(addr string) {
		if addr != "" {
			if _, ok := byAddr[addr]; !ok {
				byAddr[addr] = waitingCandidate{addr: addr}
			}
		}
	}
	// The ordinary registration gate still checks the authenticated identity
	// against any allowlist; a cached address cannot bypass admission.
	for _, addr := range n.config.StaticPeers {
		add(addr)
	}
	for addr := range n.trackerSeeds {
		add(addr)
	}
	for _, relay := range n.config.Veil.Relays {
		add(relay.Addr)
	}
	candidates := make([]waitingCandidate, 0, len(byAddr))
	for _, candidate := range byAddr {
		candidates = append(candidates, candidate)
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].stream != candidates[j].stream {
			return candidates[i].stream
		}
		if candidates[i].connected != candidates[j].connected {
			return candidates[i].connected
		}
		if (candidates[i].id != "") != (candidates[j].id != "") {
			return candidates[i].id != ""
		}
		return candidates[i].addr < candidates[j].addr
	})
	return candidates
}

func (n *Node) waitingCandidateLocked() (string, string) {
	current := n.activity.attachment
	if peer := n.peers[current]; peer != nil && !peer.relayed && n.isTrustedRelayCandidateLocked(current) {
		return current, n.knownPeers[current].addr
	}
	candidates := n.waitingCandidatesLocked()
	if len(candidates) == 0 {
		return "", ""
	}
	selected := 0
	for i, candidate := range candidates {
		if candidate.addr == n.activity.lastCandidate {
			selected = (i + 1) % len(candidates)
			break
		}
	}
	return candidates[selected].id, candidates[selected].addr
}

func (n *Node) awaitWaitingCandidate(ctx context.Context) string {
	n.mu.RLock()
	id, ready := n.activity.candidate, n.activity.candidateReady
	peer := n.peers[id]
	trusted := peer != nil && !peer.relayed && n.isTrustedRelayCandidateLocked(id)
	n.mu.RUnlock()
	if trusted {
		return id
	}
	select {
	case <-ctx.Done():
		return ""
	case <-ready:
	}
	n.mu.RLock()
	defer n.mu.RUnlock()
	id = n.activity.candidate
	if peer = n.peers[id]; peer != nil && !peer.relayed && n.isTrustedRelayCandidateLocked(id) {
		return id
	}
	return ""
}

func (n *Node) signalWaitingCandidate(advertisedID string, previouslyTrusted bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if !n.waitingRequested() {
		return
	}
	if previouslyTrusted && n.activity.attachment == advertisedID && !n.isTrustedRelayCandidateLocked(advertisedID) {
		select {
		case n.activity.wake <- struct{}{}:
		default:
		}
	}
	if !n.isTrustedRelayCandidateLocked(n.activity.candidate) {
		return
	}
	select {
	case n.activity.candidateReady <- struct{}{}:
	default:
	}
}
