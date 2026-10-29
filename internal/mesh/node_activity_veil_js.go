//go:build js

package mesh

import "context"

func (n *Node) connectWaitingCandidate(ctx context.Context, addr, peerID string) error {
	return n.connectPeerWithHint(ctx, addr, peerID)
}
