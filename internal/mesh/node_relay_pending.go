package mesh

import (
	"context"

	"github.com/redstone-md/moss/internal/gossip"
)

// One existing relay waiter owns its signed Request and original deadline.
// Node.mu guards the last physical incarnation claimed for transmission.
// Established routes and application payloads are never replayed here.
type pendingRelayControl struct {
	context context.Context
	carrier *peerConn
}

func (n *Node) transmitPendingRelayRequest(sessionID string) (bool, *peerConn) {
	var failed *peerConn
	// One captured-write race can join the replacement's already claimed send.
	// Repeated physical churn remains bounded by the same original waiter.
	for sent := 0; sent < 2; {
		n.mu.Lock()
		session, ok := n.relayLocals[sessionID]
		owner := session.pending
		if !ok || session.established || owner == nil || owner.context.Err() != nil || !n.started {
			n.mu.Unlock()
			return ok && session.established, failed
		}
		peer := n.peers[session.viaPeerID]
		if peer == nil || peer.relayed {
			failed = owner.carrier
			n.mu.Unlock()
			return false, failed
		}
		// Strict mode gates new remotes only. The carrier is a live admitted
		// peer, and an already admitted target keeps its existing exemption.
		if n.allowlist != nil {
			_, listed := n.allowlist[session.remotePeerID]
			if !listed && n.peers[session.remotePeerID] == nil {
				n.mu.Unlock()
				return false, failed
			}
		}
		if replacement := peer.replacement.Load(); replacement != nil && !replacement.finished && !replacement.committed.Load() && replacement.old.readClosed.Load() {
			n.mu.Unlock()
			select {
			case <-replacement.done:
			case <-owner.context.Done():
				return false, failed
			}
			continue
		}
		selected := carrierWritePeer(peer, gossip.TypeRelayRequest)
		if owner.carrier == selected {
			n.mu.Unlock()
			return selected != failed, failed
		}
		owner.carrier = selected
		sent++
		n.mu.Unlock()
		if n.sendRelayControlWithIdentity(selected, n.signRelayRequestEnvelope(gossip.Envelope{Type: gossip.TypeRelayRequest, RelaySession: sessionID, RelaySource: n.localPeerID(), RelayTarget: session.remotePeerID})) {
			return true, nil
		}
		failed = selected
	}
	return false, failed
}

// A normal reader already owns this authenticated physical connection. Resend
// only still-pending controls on its exact current incarnation; this is the
// existing operation, not a new relay session or background retry timer.
func (n *Node) replayPendingRelayRequests(peer *peerConn) {
	n.mu.RLock()
	if !n.started || n.peers[peer.id] != peer {
		n.mu.RUnlock()
		return
	}
	var sessions []string
	for id, session := range n.relayLocals {
		if !session.established && session.viaPeerID == peer.id && session.pending != nil && session.pending.context.Err() == nil {
			sessions = append(sessions, id)
		}
	}
	n.mu.RUnlock()
	for _, id := range sessions {
		n.transmitPendingRelayRequest(id)
	}
}
