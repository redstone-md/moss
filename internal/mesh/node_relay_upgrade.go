package mesh

import (
	"context"
	"encoding/hex"
	"errors"

	"github.com/redstone-md/moss/internal/transport"
)

func (n *Node) relayUpgradeNeedsConfirmation(session *transport.Session) bool {
	id := session.RemoteID()
	n.mu.RLock()
	defer n.mu.RUnlock()
	peer := n.peers[hex.EncodeToString(id[:])]
	return peer != nil && (peer.relayed ||
		(peer.session != nil && !isStreamNetwork(peer.session.RemoteAddr())))
}

// A TCP Noise handshake may precede a mesh-level refusal. Keep an existing
// virtual path or datagram carrier until a mesh packet confirms admission;
// replacing a carrier would otherwise also strand all its virtual sessions.
// Cold direct connections retain their original path.
func (n *Node) confirmAndRegisterRelayUpgrade(ctx context.Context, session *transport.Session, attempt uint64) error {
	packet, ok := n.awaitMeshConfirm(ctx, session)
	if !ok || !n.registerPeerFromContext(ctx, session, true, originDialTCP, attempt) {
		_ = session.Close()
		return errors.New("mesh: relay TCP upgrade unconfirmed")
	}
	n.dispatchConfirmedPacket(session, packet)
	return nil
}
