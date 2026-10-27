package mesh

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/overlay"
)

// Discovery and route establishment share the existing parent deadline.
// Preserve half its remaining time for actual path attempts even if a core
// never answers; fast returned provider records survive collection cancellation.
func (n *Node) collectIdentityProviders(ctx context.Context, key overlay.NodeID) []gossip.OverlayProvider {
	deadline, _ := ctx.Deadline()
	collection, cancel := context.WithTimeout(ctx, time.Until(deadline)/2)
	defer cancel()
	providers, _ := n.lookupOverlay(collection, key, gossip.TypeOverlayFindValue, false)
	return providers
}

// Core records can retain old attachment hints for their normal TTL. Give each
// unique hint a fair share of the parent's remaining budget so a silent stale
// carrier cannot hide a reachable later one. No deadline or retry is extended.
func (n *Node) reachIdentityHints(ctx context.Context, peerID string, providers []gossip.OverlayProvider) {
	hints := identityAttachmentHints(peerID, providers)
	deadline, _ := ctx.Deadline()
	for i, addr := range hints {
		if ctx.Err() != nil {
			return
		}
		budget := time.Until(deadline) / time.Duration(len(hints)-i)
		attempt, cancel := context.WithTimeout(ctx, budget)
		reached := n.reachPeerViaHint(attempt, peerID, reachabilityHint{Attachments: []string{addr}})
		cancel()
		if reached {
			return
		}
	}
}

func identityAttachmentHints(peerID string, providers []gossip.OverlayProvider) []string {
	seen := make(map[string]bool)
	var hints []string
	for _, provider := range providers {
		if hex.EncodeToString(provider.Peer) != peerID {
			continue
		}
		hint, ok := decodeHint(provider.Payload)
		if !ok {
			continue
		}
		for _, addr := range hint.Attachments {
			if addr != "" && !seen[addr] {
				seen[addr] = true
				hints = append(hints, addr)
			}
		}
	}
	return hints
}

// A carrier loss can isolate a live Waiting recipient's only provider record.
// Reuse the exact former carrier only after fresh identity hints failed, within
// the same lookup deadline; an authenticated replacement is the only success.
func (n *Node) reachPreviousRelayCarrier(ctx context.Context, peerID string) bool {
	n.mu.RLock()
	via, addr := n.previousRelayCarrierLocked(peerID)
	n.mu.RUnlock()
	if via == "" {
		return false
	}
	if !n.directPeerConnected(via) {
		if err := n.connectPeerWithHint(ctx, addr, via); err != nil {
			return false
		}
	}
	n.mu.RLock()
	current, _ := n.previousRelayCarrierLocked(peerID)
	n.mu.RUnlock()
	if current != via {
		return n.peerByID(peerID) != nil
	}
	_, err := n.openRelaySession(ctx, via, peerID, n.config.HandshakeTimeout())
	return err == nil
}

// The remembered ID owns no admission; current signed policy and the actual
// Noise identity must permit every new attempt. Caller holds n.mu.
func (n *Node) previousRelayCarrierLocked(peerID string) (string, string) {
	target := n.explicitTargets[peerID]
	if !n.started || n.waitingRequested() || target == nil || !target.identityRetry || target.recoveryVia == "" {
		return "", ""
	}
	via := target.recoveryVia
	info := n.knownPeers[via]
	if via == peerID || !info.verified || len(info.noiseStatic) != 32 || info.addr == "" || !n.isTrustedRelayCandidateLocked(via) {
		return "", ""
	}
	if n.allowlist != nil {
		if _, ok := n.allowlist[via]; !ok {
			return "", ""
		}
		if _, ok := n.allowlist[peerID]; !ok {
			return "", ""
		}
	}
	return via, info.addr
}
