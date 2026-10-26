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
