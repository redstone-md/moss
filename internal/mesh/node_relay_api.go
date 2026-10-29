package mesh

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
)

func (n *Node) OpenRelaySession(viaPeerID, targetPeerID string, timeout time.Duration) (string, error) {
	return n.openRelaySession(context.Background(), viaPeerID, targetPeerID, timeout)
}

func (n *Node) openRelaySession(ctx context.Context, viaPeerID, targetPeerID string, timeout time.Duration) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if viaPeerID == "" || targetPeerID == "" {
		return "", errors.New("via and target peer IDs are required")
	}
	n.mu.RLock()
	peer := n.peers[viaPeerID]
	target := n.peers[targetPeerID]
	applicationUsed := target != nil && target.applicationUsed.Load()
	n.mu.RUnlock()
	if peer == nil || peer.relayed {
		return "", errors.New("relay peer is not connected")
	}
	sessionID, err := newRelaySessionID()
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeoutCause(ctx, timeout, errors.New("relay session open timed out"))
	defer cancel()
	n.mu.RLock()
	root := n.rootCtx
	n.mu.RUnlock()
	if root != nil {
		stop := context.AfterFunc(root, cancel)
		defer stop()
	}
	owner := &pendingRelayControl{context: ctx}
	wait := make(chan struct{})
	n.mu.Lock()
	n.relayLocals[sessionID] = relayLocalSession{
		sessionID:       sessionID,
		viaPeerID:       viaPeerID,
		remotePeerID:    targetPeerID,
		wait:            wait,
		applicationUsed: applicationUsed,
		pending:         owner,
	}
	n.mu.Unlock()
	if owned, failed := n.transmitPendingRelayRequest(sessionID); !owned {
		n.mu.Lock()
		current := n.relayLocals[sessionID]
		if current.established {
			n.mu.Unlock()
			return sessionID, nil
		}
		if current.pending == owner && owner.context.Err() == nil && owner.carrier != failed {
			n.mu.Unlock()
			return n.waitRelaySession(ctx, sessionID, wait)
		}
		if current.pending == owner {
			n.retireRelayLocalLocked(sessionID)
		}
		n.mu.Unlock()
		return "", errors.New("relay request send failed")
	}
	return n.waitRelaySession(ctx, sessionID, wait)
}

// A successful admission owns the session even if cancellation wakes its waiter.
func (n *Node) waitRelaySession(ctx context.Context, sessionID string, wait <-chan struct{}) (string, error) {
	var waitErr error
	select {
	case <-wait:
	case <-ctx.Done():
		waitErr = context.Cause(ctx)
	}
	n.mu.Lock()
	// Admission can win the race with cancellation or a timeout. Its session
	// is already carrying data; a late waiter must not delete that success.
	session, exists := n.relayLocals[sessionID]
	if exists && session.established {
		n.mu.Unlock()
		return sessionID, nil
	}
	n.retireRelayLocalLocked(sessionID)
	n.mu.Unlock()
	if waitErr == nil {
		waitErr = errors.New("relay session closed before establishment")
	}
	return "", waitErr
}

func (n *Node) RelaySend(sessionID string, data []byte) error {
	n.mu.RLock()
	session, ok := n.relayLocals[sessionID]
	peer := n.peers[session.viaPeerID]
	n.mu.RUnlock()
	if !ok || peer == nil || !session.established {
		return errors.New("relay session is not established")
	}
	// Size-gate BEFORE the send: an oversized payload would frame into a
	// stream packet the middle node's transport cap rejects on arrival —
	// and that rejection tears down sessions the relay still needs. Refuse
	// the send instead, leaving the session (and every other relay session
	// sharing this peer) alive for legal payloads.
	if len(data) > maxRelayPayloadBytes {
		return fmt.Errorf("relay payload of %d bytes exceeds the %d-byte limit", len(data), maxRelayPayloadBytes)
	}
	n.markRelayApplicationUsed(sessionID)
	if !n.sendRelayPayload(sessionID, data) {
		return errors.New("relay send failed")
	}
	return nil
}

func (n *Node) markRelayApplicationUsed(sessionID string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	session, ok := n.relayLocals[sessionID]
	if !ok || !session.established {
		return
	}
	session.applicationUsed = true
	n.relayLocals[sessionID] = session
	if peer := n.peers[session.remotePeerID]; peer != nil {
		peer.applicationUsed.Store(true)
	}
}

func (n *Node) RelaySendTo(targetPeerID string, data []byte, timeout time.Duration) error {
	if targetPeerID == "" {
		return errors.New("target peer ID is required")
	}
	// A directly-connected target is not an error — it is the fast path.
	// SendToPeer owns the routing choice between direct and relay; this
	// method is the explicit relay one, so it sends through a relay session
	// even when a direct session exists (a caller may want the relay path
	// specifically, e.g. to keep a direct session's traffic profile clean).
	n.mu.RLock()
	for _, session := range n.relayLocals {
		if session.remotePeerID == targetPeerID && session.established {
			n.mu.RUnlock()
			return n.RelaySend(session.sessionID, data)
		}
	}
	n.mu.RUnlock()

	sessionID, err := n.OpenRelaySessionAny(targetPeerID, timeout)
	if err != nil {
		return err
	}
	return n.RelaySend(sessionID, data)
}

func (n *Node) OpenRelaySessionAny(targetPeerID string, timeout time.Duration) (string, error) {
	return n.openRelaySessionAny(context.Background(), targetPeerID, timeout)
}

func (n *Node) openRelaySessionAny(ctx context.Context, targetPeerID string, timeout time.Duration) (string, error) {
	candidates, err := n.selectRelayPeers(targetPeerID)
	if err != nil {
		return "", err
	}
	if timeout <= 0 {
		timeout = n.config.HandshakeTimeout()
	}
	perCandidate := timeout / time.Duration(len(candidates))
	if perCandidate < 300*time.Millisecond {
		perCandidate = 300 * time.Millisecond
	}
	var lastErr error
	deadline := time.Now().Add(timeout)
	for _, viaPeerID := range candidates {
		remaining := time.Until(deadline)
		if remaining <= 0 || ctx.Err() != nil {
			break
		}
		attemptTimeout := perCandidate
		if remaining < attemptTimeout {
			attemptTimeout = remaining
		}
		sessionID, err := n.openRelaySession(ctx, viaPeerID, targetPeerID, attemptTimeout)
		if err == nil {
			return sessionID, nil
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = errors.New("relay session open timed out")
	}
	return "", lastErr
}

// SendToPeer sends one directed payload to one peer, choosing the transport
// itself: a live session (direct or already-relayed) carries the bytes over
// that session; a peer we are not connected to is reached through the relay
// path (OpenRelaySessionAny → RelaySend). The payload is opaque application
// data — never sealed with the room AEAD, because a direct session is already
// Noise-encrypted and the relay path seals inside its own session AEAD.
//
// The size gate matches Publish's: Security.MaxMessageSizeBytes on the raw
// payload. Callers wanting larger directed transfers must chunk.
func (n *Node) SendToPeer(peerID string, payload []byte, timeout time.Duration) error {
	if peerID == "" {
		return errors.New("target peer ID is required")
	}
	if len(payload) > n.config.Security.MaxMessageSizeBytes {
		return errors.New("payload exceeds the maximum message size")
	}
	n.mu.RLock()
	peer := n.peers[peerID]
	if peer != nil {
		peer.applicationUsed.Store(true)
	}
	n.mu.RUnlock()
	if peer != nil {
		env := gossip.Envelope{
			Type:     gossip.TypeDirect,
			SenderID: n.identity.PublicKeyBytes(),
			Payload:  payload,
		}
		if n.sendOrEnqueue(peer, env) {
			return nil
		}
		// A session write failure on a connected peer is a delivery failure
		// the caller can act on (the session is dying); falling back to the
		// relay path here would mask it and double the traffic.
		return errors.New("direct send failed")
	}
	// Not connected: reach the peer through a relay session.
	return n.RelaySendTo(peerID, payload, timeout)
}

// handleDirectPacket delivers one inbound TypeDirect envelope to the
// application's packet callback. It is the receive half of SendToPeer: the
// payload rides the envelope unsealed (the direct session's Noise layer is
// the confidentiality), so the only gate is the size cap, matching the send
// side. A peer sending over the limit is dropped and penalized, not crashed.
func (n *Node) handleDirectPacket(peer *peerConn, env gossip.Envelope) {
	if peer == nil {
		return
	}
	if len(env.Payload) > n.config.Security.MaxMessageSizeBytes {
		n.scoring.PenalizeInvalid(peer.id)
		n.emitPenalty(peer.id, "direct packet over the message size limit")
		return
	}
	n.mu.RLock()
	if current := n.peers[peer.id]; current != nil {
		current.applicationUsed.Store(true)
	}
	n.mu.RUnlock()
	var sender [32]byte
	copy(sender[:], env.SenderID)
	// queueKey is the authenticated session identity (peer.id is the 64-char
	// hex of the Noise static key bound at handshake, never attacker-chosen),
	// while sender stays the envelope's CLAIMED key that SendToPeer's receive
	// half reports to the app. Keying the per-sender delivery queue on the
	// claim instead would let one peer mint up to MaxPeers queues and flood
	// each — the queue bound must ride the connection, not the payload.
	select {
	case n.dispatchCh <- dispatchPacket{sender: sender, queueKey: decodePeerID(peer.id), data: append([]byte(nil), env.Payload...)}:
	default:
		n.countInbound("__packet_dispatch_dropped__")
	}
}

// PeerRTT returns the last measured round-trip time to a connected peer —
// the same value the maintenance loop's ping/pong probes refresh on
// handlePong, and that peer selection (mesh grafting, relay ranking) already
// sorts by. Zero when the peer is unknown or has not yet been probed.
func (n *Node) PeerRTT(peerID string) time.Duration {
	n.mu.RLock()
	defer n.mu.RUnlock()
	peer := n.peers[peerID]
	if peer == nil {
		return 0
	}
	return peer.lastRTT
}

// SetPacketCallback registers the unified sink for directed payloads. When
// set, it receives BOTH direct packets (SendToPeer over a direct session)
// and raw relayed payloads; the legacy relayCB still fires for relayed
// payloads when no packet callback is registered.
func (n *Node) SetPacketCallback(cb PacketCallback) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.packetCB = cb
}
