package mesh

import (
	"context"
	"encoding/hex"
	"errors"

	"github.com/redstone-md/moss/internal/transport"
)

func (n *Node) relayUpgradeNeedsConfirmation(session *transport.Session, attempt uint64) bool {
	id := session.RemoteID()
	n.mu.RLock()
	defer n.mu.RUnlock()
	peerID := hex.EncodeToString(id[:])
	if n.waitingRequested() && attempt != 0 && attempt == n.activity.attempt && n.waitingAdmissionAllowedLocked(peerID, true, attempt) {
		return true
	}
	if target := n.explicitTargets[peerID]; target != nil && target.identityRetry {
		return true
	}
	peer := n.peers[peerID]
	return peer != nil && (peer.relayed ||
		(peer.session != nil && !isStreamNetwork(peer.session.RemoteAddr())))
}

// A TCP Noise handshake may precede a mesh-level refusal. Keep an existing
// virtual path or datagram carrier until a mesh packet confirms admission;
// replacing a carrier would otherwise also strand all its virtual sessions.
// A permitted Waiting candidate also needs fresh admission: cached signed
// capability plus a cold Noise handshake must not announce Ready for a peer
// that refuses mesh traffic. An explicit target recovering from an established
// relay also needs confirmation after its virtual entry leaves the directory.
// Ordinary cold direct startup remains unchanged.
func (n *Node) confirmAndRegisterRelayUpgrade(ctx context.Context, session *transport.Session, attempt uint64) error {
	packet, ok := n.awaitMeshConfirm(ctx, session)
	if !ok || !n.registerPeerFromContext(ctx, session, true, originDialTCP, attempt) {
		_ = session.Close()
		return errors.New("mesh: relay TCP upgrade unconfirmed")
	}
	n.dispatchConfirmedPacket(session, packet)
	return nil
}
