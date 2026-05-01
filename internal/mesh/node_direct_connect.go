package mesh

import (
	"context"
	"net"
	"net/netip"
	"time"

	"moss/internal/nat"
)

func (n *Node) directPeerConnected(peerID string) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	_, ok := n.peers[peerID]
	return ok
}

func (n *Node) currentPeerCount() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return len(n.peers)
}

func (n *Node) establishedRelaySession(targetPeerID string) string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	for _, session := range n.relayLocals {
		if session.remotePeerID == targetPeerID && session.established {
			return session.sessionID
		}
	}
	return ""
}

func (n *Node) tryDirectConnect(targetPeerID string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	n.mu.RLock()
	targetInfo, ok := n.knownPeers[targetPeerID]
	n.mu.RUnlock()
	if remaining := time.Until(deadline); remaining > 0 {
		refreshBudget := initialDirectRefreshBudget(remaining)
		if refreshBudget > 0 {
			n.refreshExternalAddress(time.Now().Add(refreshBudget))
			if n.waitForDirectPeer(targetPeerID, minDuration(100*time.Millisecond, time.Until(deadline))) {
				return true
			}
		}
	}
	if ok && targetInfo.addr != "" {
		dialBudget := initialDirectDialBudget(targetInfo, time.Until(deadline))
		if dialBudget > 0 {
			ctx, cancel := context.WithTimeout(context.Background(), dialBudget)
			n.connectPeerWithHint(ctx, targetInfo.addr, targetPeerID)
			cancel()
			if n.waitForDirectPeer(targetPeerID, minDuration(250*time.Millisecond, time.Until(deadline))) {
				return true
			}
		}
	}
	if time.Until(deadline) <= 0 {
		return n.directPeerConnected(targetPeerID)
	}
	if !ok || targetInfo.addr == "" {
		return n.waitForDirectPeer(targetPeerID, time.Until(deadline))
	}
	if n.shouldPreferRelayForTarget(targetPeerID) {
		return n.waitForDirectPeer(targetPeerID, time.Until(deadline))
	}
	if n.attemptHolePunch(targetPeerID, time.Until(deadline)) {
		return true
	}
	finalBudget := finalDirectDialBudget(targetInfo, time.Until(deadline))
	if finalBudget > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), finalBudget)
		n.connectPeerWithHint(ctx, targetInfo.addr, targetPeerID)
		cancel()
		if n.waitForDirectPeer(targetPeerID, minDuration(250*time.Millisecond, time.Until(deadline))) {
			return true
		}
	}
	return n.waitForDirectPeer(targetPeerID, time.Until(deadline))
}

func initialDirectRefreshBudget(total time.Duration) time.Duration {
	if total <= 0 {
		return 0
	}
	budget := total / 3
	if budget > time.Second {
		budget = time.Second
	}
	if budget < 250*time.Millisecond {
		return total
	}
	return budget
}

func initialDirectDialBudget(targetInfo knownPeer, total time.Duration) time.Duration {
	if total <= 0 {
		return 0
	}
	if shouldUseShortDirectProbe(targetInfo) {
		budget := total / 4
		if budget > 750*time.Millisecond {
			budget = 750 * time.Millisecond
		}
		if budget < 250*time.Millisecond {
			budget = minDuration(total, 250*time.Millisecond)
		}
		return budget
	}
	return total
}

func finalDirectDialBudget(targetInfo knownPeer, total time.Duration) time.Duration {
	if total <= 0 {
		return 0
	}
	if shouldUseShortDirectProbe(targetInfo) {
		return minDuration(total, time.Second)
	}
	return total
}

func shouldUseShortDirectProbe(targetInfo knownPeer) bool {
	if targetInfo.addr == "" {
		return false
	}
	host, _, err := net.SplitHostPort(targetInfo.addr)
	if err == nil {
		if ip, err := netip.ParseAddr(host); err == nil {
			ip = ip.Unmap()
			if ip.IsLoopback() || ip.IsPrivate() {
				return false
			}
		}
	}
	if targetInfo.lan || targetInfo.publicReachable {
		return false
	}
	switch targetInfo.natType {
	case nat.TypePublic:
		return false
	default:
		return true
	}
}

func preferredKnownPeerAddr(current knownPeer, candidate string) string {
	if candidate == "" {
		return current.addr
	}
	if current.addr == "" {
		return candidate
	}
	currentRank := knownPeerAddrRank(current.addr)
	candidateRank := knownPeerAddrRank(candidate)
	if candidateRank > currentRank {
		return candidate
	}
	if candidateRank < currentRank {
		return current.addr
	}
	return candidate
}

func shouldFreezeDirectKnownPeerAddr(current knownPeer, candidate, liveSessionAddr string) bool {
	if (!current.direct && !current.verified) || current.addr == "" {
		return false
	}
	currentRank := knownPeerAddrRank(current.addr)
	candidateRank := knownPeerAddrRank(candidate)
	if candidateRank < currentRank {
		return true
	}
	selfAnnounced := liveSessionAddr != ""
	if !selfAnnounced {
		return true
	}
	if currentRank < 3 || candidateRank < 3 {
		return current.addr == liveSessionAddr || liveSessionAddr == ""
	}
	return false
}

func knownPeerAddrRank(addr string) int {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return 0
	}
	ip = ip.Unmap()
	switch {
	case ip.IsGlobalUnicast() && !ip.IsPrivate() && !ip.IsLoopback() && !ip.IsLinkLocalUnicast() && !isCarrierGradeAddr(ip):
		return 3
	case isCarrierGradeAddr(ip):
		return 2
	case ip.IsPrivate():
		return 1
	default:
		return 0
	}
}
