package mesh

import (
	"errors"
	"time"

	"moss/internal/gossip"
)

func (n *Node) OpenRelaySession(viaPeerID, targetPeerID string, timeout time.Duration) (string, error) {
	if viaPeerID == "" || targetPeerID == "" {
		return "", errors.New("via and target peer IDs are required")
	}
	n.mu.RLock()
	peer := n.peers[viaPeerID]
	n.mu.RUnlock()
	if peer == nil {
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
	n.sendEnvelope(peer, gossip.Envelope{
		Type:         gossip.TypeRelayData,
		RelaySession: sessionID,
		RelaySource:  n.localPeerID(),
		RelayTarget:  session.remotePeerID,
		Payload:      append([]byte(nil), data...),
	})
	return nil
}

func (n *Node) RelaySendTo(targetPeerID string, data []byte, timeout time.Duration) error {
	if targetPeerID == "" {
		return errors.New("target peer ID is required")
	}
	n.mu.RLock()
	if _, direct := n.peers[targetPeerID]; direct {
		n.mu.RUnlock()
		return errors.New("target peer is directly connected")
	}
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
