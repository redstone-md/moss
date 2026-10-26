//go:build !js

package mesh

import (
	"context"
	"encoding/hex"
)

func (n *Node) connectWaitingCandidate(ctx context.Context, addr, peerID string) error {
	for _, relay := range n.config.Veil.Relays {
		if relay.Addr != addr {
			continue
		}
		key, err := hex.DecodeString(relay.PubKeyHex)
		if err != nil {
			return err
		}
		_, err = n.veilDial(ctx, relay.Addr, relay.CoverSNI, key)
		return err
	}
	// A single background carrier must carry application-sized envelopes.
	// Prefer the existing stream transport over UDP IP fragmentation, which
	// middleboxes can silently discard while its small keepalives still work.
	n.mu.RLock()
	peer := n.peers[peerID]
	datagram := peer != nil && !peer.relayed && peer.session != nil && !isStreamNetwork(peer.session.RemoteAddr())
	n.mu.RUnlock()
	if datagram {
		_ = n.connectPeerTCPWithHint(ctx, addr, peerID)
		if n.directPeerConnected(peerID) {
			return nil
		}
	}
	// Match static-peer fallback without racing two physical admissions.
	_ = n.connectPeerWithHint(ctx, addr, peerID)
	if n.hasPeerAddr(addr) || (peerID != "" && n.directPeerConnected(peerID)) {
		return nil
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return n.connectPeerUDPWithHint(ctx, peerID, addr)
}
