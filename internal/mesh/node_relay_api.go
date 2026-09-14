package mesh

import (
	"errors"
	"fmt"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
)

func (n *Node) OpenRelaySession(viaPeerID, targetPeerID string, timeout time.Duration) (string, error) {
	if viaPeerID == "" || targetPeerID == "" {
		return "", errors.New("via and target peer IDs are required")
	}
	n.mu.RLock()
	peer := n.peers[viaPeerID]
	n.mu.RUnlock()
	if peer == nil || peer.relayed {
		return "", errors.New("relay peer is not connected")
	}
	sessionID, err := newRelaySessionID()
	if err != nil {
		return "", err
	}
	wait := make(chan struct{})
	n.mu.Lock()
	n.relayLocals[sessionID] = relayLocalSession{
		sessionID:    sessionID,
		viaPeerID:    viaPeerID,
		remotePeerID: targetPeerID,
		wait:         wait,
	}
	n.mu.Unlock()
	n.sendEnvelope(peer, n.signRelayRequestEnvelope(gossip.Envelope{
		Type:         gossip.TypeRelayRequest,
		RelaySession: sessionID,
		RelaySource:  n.localPeerID(),
		RelayTarget:  targetPeerID,
	}))
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-wait:
		return sessionID, nil
	case <-timer.C:
		n.mu.Lock()
		delete(n.relayLocals, sessionID)
		n.mu.Unlock()
		return "", errors.New("relay session open timed out")
	}
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
	if !n.sendRelayPayload(sessionID, data) {
		return errors.New("relay send failed")
	}
	return nil
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
		if remaining <= 0 {
			break
		}
		attemptTimeout := perCandidate
		if remaining < attemptTimeout {
			attemptTimeout = remaining
		}
		sessionID, err := n.OpenRelaySession(viaPeerID, targetPeerID, attemptTimeout)
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
	var sender [32]byte
	copy(sender[:], env.SenderID)
	select {
	case n.dispatchCh <- dispatchPacket{sender: sender, data: append([]byte(nil), env.Payload...)}:
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
