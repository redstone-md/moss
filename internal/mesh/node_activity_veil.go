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
	return n.connectPeerWithHint(ctx, addr, peerID)
}
