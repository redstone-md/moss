package mesh

import (
	"crypto/rand"
	"encoding/hex"
	"io"
	"time"

	"moss/internal/gossip"
)

func (n *Node) handleRelayRequest(peer *peerConn, env gossip.Envelope) {
	if env.RelaySession == "" || env.RelaySource == "" || env.RelayTarget == "" {
		return
	}
	if env.RelayTarget == n.localPeerID() {
		if peer == nil || !verifyRelayRequestEnvelope(env) {
			return
		}
		n.mu.Lock()
		n.relayLocals[env.RelaySession] = relayLocalSession{
			sessionID:    env.RelaySession,
			viaPeerID:    peer.id,
			remotePeerID: env.RelaySource,
			established:  true,
		}
		n.mu.Unlock()
		n.sendEnvelope(peer, n.signRelayAcceptEnvelope(gossip.Envelope{
			Type:         gossip.TypeRelayAccept,
			RelaySession: env.RelaySession,
			RelaySource:  env.RelayTarget,
			RelayTarget:  env.RelaySource,
		}))
		return
	}
	n.mu.RLock()
	targetPeer := n.peers[env.RelayTarget]
	n.mu.RUnlock()
	if targetPeer == nil {
		return
	}
	if !n.relaySessions.Acquire(env.RelaySession) {
		return
	}
	n.mu.Lock()
	n.relayRoutes[env.RelaySession] = relayRoute{initiator: env.RelaySource, target: env.RelayTarget}
	n.mu.Unlock()
	n.refreshSupernodeStatus()
	n.sendEnvelope(targetPeer, env)
}

func (n *Node) handleRelayAccept(peer *peerConn, env gossip.Envelope) {
	if env.RelaySession == "" || env.RelaySource == "" || env.RelayTarget == "" {
		return
	}
	if env.RelayTarget == n.localPeerID() {
		if peer == nil {
			return
		}
		if !verifyRelayAcceptEnvelope(env) {
			return
		}
		n.mu.Lock()
		session, ok := n.relayLocals[env.RelaySession]
		if ok && session.viaPeerID == peer.id && session.remotePeerID == env.RelaySource {
			session.established = true
			n.relayLocals[env.RelaySession] = session
			if session.wait != nil {
				close(session.wait)
				session.wait = nil
				n.relayLocals[env.RelaySession] = session
			}
		}
		n.mu.Unlock()
		return
	}
	n.mu.RLock()
	targetPeer := n.peers[env.RelayTarget]
	n.mu.RUnlock()
	if targetPeer != nil {
		n.sendEnvelope(targetPeer, env)
	}
}

func (n *Node) handleRelayData(peer *peerConn, env gossip.Envelope) {
	if env.RelaySession == "" || env.RelaySource == "" || env.RelayTarget == "" {
		return
	}
	if env.RelayTarget == n.localPeerID() {
		n.mu.RLock()
		session, ok := n.relayLocals[env.RelaySession]
		n.mu.RUnlock()
		if !ok || !session.established || session.viaPeerID != peer.id || session.remotePeerID != env.RelaySource {
			return
		}
		var sender [32]byte
		raw, err := hex.DecodeString(env.RelaySource)
		if err == nil {
			copy(sender[:], raw)
		}
		n.dispatchCh <- dispatchRelay{sender: sender, data: append([]byte(nil), env.Payload...)}
		return
	}
	n.mu.RLock()
	route, hasRoute := n.relayRoutes[env.RelaySession]
	targetPeer := n.peers[env.RelayTarget]
	n.mu.RUnlock()
	if !hasRoute || !route.allows(env.RelaySource, env.RelayTarget) {
		return
	}
	if peer.id != env.RelaySource {
		return
	}
	if targetPeer == nil {
		return
	}
	bucket := n.relayBucketFor(peer.id)
	if !bucket.Allow(len(env.Payload)) {
		n.markRelayOverloaded(time.Now())
		return
	}
	n.sendEnvelope(targetPeer, env)
}

func (n *Node) markRelayOverloaded(now time.Time) {
	cooldown := n.relayOverloadCooldown()
	if cooldown <= 0 {
		cooldown = 500 * time.Millisecond
	}
	n.mu.Lock()
	until := now.Add(cooldown)
	if until.After(n.overloadedUntil) {
		n.overloadedUntil = until
	}
	n.mu.Unlock()
	n.refreshSupernodeStatus()
}

func (n *Node) relayOverloadCooldown() time.Duration {
	cooldown := 2 * n.config.Heartbeat()
	if cooldown < 500*time.Millisecond {
		cooldown = 500 * time.Millisecond
	}
	return cooldown
}

func (n *Node) gossipRecentMessages(channel string) {
	ids := n.cache.RecentIDs(channel, n.config.GossipSub.DLazy)
	if len(ids) == 0 {
		return
	}
	targets := n.selectLazyPeers(channel, "", n.config.GossipSub.DLazy)
	n.sendToPeers(targets, gossip.Envelope{
		Type:       gossip.TypeIHave,
		Channel:    channel,
		MessageIDs: ids,
	})
}

func (n *Node) handleRelayClose(peer *peerConn, env gossip.Envelope) {
	if env.RelaySession == "" {
		return
	}
	n.mu.Lock()
	delete(n.relayLocals, env.RelaySession)
	delete(n.relayRoutes, env.RelaySession)
	n.mu.Unlock()
	n.relaySessions.Release(env.RelaySession)
	n.refreshSupernodeStatus()
	if env.RelayTarget == "" || env.RelayTarget == n.localPeerID() {
		return
	}
	n.mu.RLock()
	targetPeer := n.peers[env.RelayTarget]
	n.mu.RUnlock()
	if targetPeer != nil && targetPeer.id != peer.id {
		n.sendEnvelope(targetPeer, env)
	}
}

func (n *Node) migrateRelaySessions(peerID string) {
	n.mu.RLock()
	sessions := make([]relayLocalSession, 0, len(n.relayLocals))
	for _, session := range n.relayLocals {
		if session.remotePeerID == peerID && session.established {
			sessions = append(sessions, session)
		}
	}
	n.mu.RUnlock()
	for _, session := range sessions {
		n.closeRelaySession(session)
	}
}

func (n *Node) closeRelaySession(session relayLocalSession) {
	n.mu.RLock()
	viaPeer := n.peers[session.viaPeerID]
	n.mu.RUnlock()
	if viaPeer != nil {
		n.sendEnvelope(viaPeer, gossip.Envelope{
			Type:         gossip.TypeRelayClose,
			RelaySession: session.sessionID,
			RelaySource:  n.localPeerID(),
			RelayTarget:  session.remotePeerID,
		})
	}
	n.mu.Lock()
	delete(n.relayLocals, session.sessionID)
	delete(n.directProbes, session.remotePeerID)
	n.mu.Unlock()
	n.enqueueEvent(EventRelayMigrated, map[string]string{
		"peer":    session.remotePeerID,
		"session": session.sessionID,
		"via":     session.viaPeerID,
	})
}

func (n *Node) promoteRelayPeers() {
	targets := n.relayPromotionTargets()
	for _, peerID := range targets {
		go n.tryDirectConnect(peerID, n.config.HandshakeTimeout())
	}
}

func (n *Node) relayPromotionTargets() []string {
	now := time.Now()
	cooldown := n.config.Heartbeat()
	if cooldown <= 0 {
		cooldown = 250 * time.Millisecond
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	targets := make([]string, 0, len(n.relayLocals))
	for _, session := range n.relayLocals {
		if !session.established {
			continue
		}
		if _, direct := n.peers[session.remotePeerID]; direct {
			continue
		}
		lastAttempt := n.directProbes[session.remotePeerID]
		if !lastAttempt.IsZero() && now.Sub(lastAttempt) < cooldown {
			continue
		}
		n.directProbes[session.remotePeerID] = now
		targets = append(targets, session.remotePeerID)
	}
	return targets
}

func newRelaySessionID() (string, error) {
	var raw [16]byte
	if _, err := io.ReadFull(rand.Reader, raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}
