package mesh

import (
	"crypto/rand"
	"encoding/hex"
	"io"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
)

const relayMigrationGracePeriod = time.Second

// maxRelayPayloadBytes bounds one relayed application payload. The wire form
// is a JSON envelope whose Payload field carries the base64 of the bytes
// (x4/3), plus envelope fields and the DM AEAD's expansion (12-byte nonce +
// 16-byte tag — the origin pays it before the frame, the middle node
// forwards opaque bytes), all sealed inside a stream frame that the 256 KiB
// data-frame cap (transport maxDataFrameSize) hard-rejects on arrival —
// killing the middle→target session. 190 KiB leaves ~2.4 KiB of slack
// inside that cap after the worst-case envelope overhead, so a payload at
// the limit rides the wire while anything larger is refused by the sender,
// not by the receiver's teardown. The gate lives in RelaySend (origin) and
// handleRelayData (middle), never in the target's delivery path.
const maxRelayPayloadBytes = 190 * 1024

func (n *Node) handleRelayRequest(peer *peerConn, env gossip.Envelope) {
	if env.RelaySession == "" || env.RelaySource == "" || env.RelayTarget == "" {
		return
	}
	if env.RelayTarget == n.localPeerID() {
		if peer == nil || !verifyRelayRequestEnvelope(env) {
			return
		}
		session := relayLocalSession{
			sessionID:    env.RelaySession,
			viaPeerID:    peer.id,
			remotePeerID: env.RelaySource,
			established:  true,
		}
		n.mu.Lock()
		n.relayLocals[env.RelaySession] = session
		relayPeer, capped := n.registerRelayedPeerLocked(session)
		if capped {
			delete(n.relayLocals, env.RelaySession)
		}
		n.mu.Unlock()
		if capped {
			// No slot or no policy admission for another relayed peer:
			// refuse with a close, not an accept that would leave the
			// opener holding a half-open session. The opener's
			// handleRelayClose tears it down.
			n.sendEnvelope(peer, gossip.Envelope{
				Type:         gossip.TypeRelayClose,
				RelaySession: env.RelaySession,
				RelaySource:  env.RelayTarget,
				RelayTarget:  env.RelaySource,
			})
			return
		}
		n.sendEnvelope(peer, n.signRelayAcceptEnvelope(gossip.Envelope{
			Type:         gossip.TypeRelayAccept,
			RelaySession: env.RelaySession,
			RelaySource:  env.RelayTarget,
			RelayTarget:  env.RelaySource,
		}))
		n.activateRelayedPeer(relayPeer)
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
		var relayPeer *peerConn
		capped := false
		n.mu.Lock()
		session, ok := n.relayLocals[env.RelaySession]
		if ok && session.viaPeerID == peer.id && session.remotePeerID == env.RelaySource {
			session.established = true
			relayPeer, capped = n.registerRelayedPeerLocked(session)
			if !capped {
				n.relayLocals[env.RelaySession] = session
				if session.wait != nil {
					close(session.wait)
					session.wait = nil
					n.relayLocals[env.RelaySession] = session
				}
			}
		}
		n.mu.Unlock()
		if capped {
			// The via-peer already registered a forwarding route on our
			// behalf, so a refused target — at relayed fan-out cap or not
			// admitted by the allowlist — must tear the whole session down
			// or that route outlives its endpoint. session.wait stays open
			// on purpose: OpenRelaySession's select times out honestly
			// instead of reporting success with no peer behind it.
			n.closeRelaySession(session)
			return
		}
		n.activateRelayedPeer(relayPeer)
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
		if !ok || !session.established || peer == nil || session.viaPeerID != peer.id || session.remotePeerID != env.RelaySource {
			return
		}
		if inner, err := n.openRelayGossipEnvelope(session, env.RelaySource, env.Payload); err == nil {
			n.handleEnvelope(n.relayPeerForSession(session), inner)
			return
		}
		// Not session-sealed gossip: an application DM, end-to-end sealed to
		// us by the source. There is no plaintext path — a payload we
		// cannot open is counted and dropped, never handed to the
		// application unread.
		if plaintext, err := n.openDMPayload(env.RelaySource, env.Payload); err == nil {
			var sender [32]byte
			if raw, err := hex.DecodeString(env.RelaySource); err == nil {
				copy(sender[:], raw)
			}
			n.dispatchCh <- dispatchRelay{sender: sender, data: plaintext}
			return
		}
		n.countInbound("__relay_payload_unopenable__")
		return
	}
	n.mu.RLock()
	route, hasRoute := n.relayRoutes[env.RelaySession]
	targetPeer := n.peers[env.RelayTarget]
	n.mu.RUnlock()
	if !hasRoute || !route.allows(env.RelaySource, env.RelayTarget) {
		return
	}
	if peer == nil || peer.id != env.RelaySource {
		return
	}
	if targetPeer == nil {
		return
	}
	// Oversize payloads are dropped here rather than forwarded: the forward
	// would land on the target's transport frame cap, and the teardown that
	// cap triggers kills the middle→target session we would have just used.
	// A misbehaving origin gets its traffic counted and dropped at the first
	// hop, never a session belonging to us. The origin's own RelaySend gate is
	// the primary bound (see maxRelayPayloadBytes); this guards against a
	// hostile or version-skewed origin. The gate stays on WIRE bytes — it
	// protects the transport frame cap, which sees the wire form — while
	// the bandwidth bucket charges application bytes
	// (relayChargeableBytes): the relay bills what the app sent, not the
	// origin's cryptography.
	if len(env.Payload) > maxRelayPayloadBytes {
		n.countInbound("__relay_oversize__")
		return
	}
	bucket := n.relayBucketFor(peer.id)
	if !bucket.Allow(relayChargeableBytes(len(env.Payload))) {
		n.countInbound("__relay_rate_limited__")
		n.markRelayOverloaded(time.Now())
		return
	}
	if n.relayConsumerCapped(peer.id, int64(len(env.Payload)), time.Now()) {
		n.countInbound("__relay_consumer_capped__")
		n.closeRelayRouteWithClose(env.RelaySession, env.RelaySource, env.RelayTarget)
		return
	}
	n.sendEnvelope(targetPeer, env)
	// Keep the session alive while traffic flows so the route GC only reaps
	// genuinely idle forwarding entries.
	n.relaySessions.Touch(env.RelaySession)
}

// pruneStaleRelayRoutes reaps middle-node forwarding entries whose relay
// session has gone idle past the TTL. Routes were previously removed only when
// an endpoint peer disconnected or sent an explicit teardown, so a relayed pair
// that vanished without a teardown (client crash, silent network drop) left its
// route behind indefinitely — on a busy supernode these piled up into hundreds
// of dead entries while the underlying sessions had long since expired. Tying
// route liveness to the session (refreshed by Touch on every forwarded packet)
// lets genuinely idle routes expire on their own.
func (n *Node) pruneStaleRelayRoutes() {
	n.mu.RLock()
	var stale []string
	for sessionID := range n.relayRoutes {
		if !n.relaySessions.Active(sessionID) {
			stale = append(stale, sessionID)
		}
	}
	n.mu.RUnlock()
	if len(stale) == 0 {
		return
	}
	n.mu.Lock()
	for _, sessionID := range stale {
		delete(n.relayRoutes, sessionID)
	}
	n.mu.Unlock()
	n.refreshSupernodeStatus()
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

// lazyAnnounceDepth is how deep into the channel's recent ids the heartbeat
// sweep announces. DLazy ids per envelope is the publish-side convention;
// the sweep is the recovery net for ids a subscriber missed, and a payload
// is only requestable while an announcement still names it. Announcing
// just DLazy meant the tail of the freshest payloads stopped being named
// as soon as DLazy newer ones existed — a subscriber that missed a payload
// in its announce window (a congested tick, a lost IHAVE) could never ask
// for it. Twice DLazy keeps the per-envelope cost bounded while giving
// every payload a recoverable window measured in multiple publish
// intervals, not one.
const lazyAnnounceDepthFactor = 2

func (n *Node) gossipRecentMessages(channel string) {
	depth := n.config.GossipSub.DLazy * lazyAnnounceDepthFactor
	if depth <= 0 {
		depth = n.config.GossipSub.DLazy
	}
	ids := n.cache.RecentIDs(channel, depth)
	if len(ids) == 0 {
		return
	}
	targets := n.selectLazyPeersCovering(channel)
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
	// Always defer the close by a grace period. Tearing the relay session down
	// the instant a direct connection registers loses any payload still in
	// flight over the relay — including on a pure receiver, whose lastSendAt is
	// always zero and would otherwise close immediately.
	for _, session := range sessions {
		n.deferRelayMigration(session)
	}
}

func (n *Node) deferRelayMigration(session relayLocalSession) {
	go func() {
		wait := relayMigrationGracePeriod
		if !session.lastSendAt.IsZero() {
			if remaining := relayMigrationGracePeriod - time.Since(session.lastSendAt); remaining > 0 {
				wait = remaining
			}
		}
		timer := time.NewTimer(wait)
		defer timer.Stop()
		<-timer.C
		n.mu.RLock()
		started := n.started
		current, ok := n.relayLocals[session.sessionID]
		n.mu.RUnlock()
		if !started || !ok || current.remotePeerID != session.remotePeerID {
			return
		}
		n.closeRelaySession(current)
	}()
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
	removedPeer := n.removeRelayedPeerLocked(session)
	delete(n.relayLocals, session.sessionID)
	delete(n.directProbes, session.remotePeerID)
	n.mu.Unlock()
	if removedPeer {
		n.pubsub.RemovePeer(session.remotePeerID)
	}
	n.enqueueEvent(EventRelayMigrated, map[string]string{
		"peer":    session.remotePeerID,
		"session": session.sessionID,
		"via":     session.viaPeerID,
	})
}

// promoteRelayPeers keeps trying to replace a relayed path with a direct one.
//
// It uses the upgrade policy deliberately: these peers are already reachable
// through a relay, so nobody is waiting and a punch costs nothing but effort —
// while success frees a volunteer's bandwidth and drops a hop. Routed through
// the ordinary connect policy, the relay preference applied here too and a
// symmetric pair was never retried once relayed: relay became the destination
// rather than the fallback it is meant to be.
func (n *Node) promoteRelayPeers() {
	targets := n.relayPromotionTargets()
	for _, peerID := range targets {
		go n.tryDirectUpgrade(peerID, n.config.HandshakeTimeout())
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
		if peer := n.peers[session.remotePeerID]; peer != nil && !peer.relayed {
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

// relayedPeerCap bounds how many relayed peers may occupy n.peers. Relayed
// peers deliberately do not count toward MaxPeers — a leaf at MaxPeers=1
// holds its one direct hop plus the peers it reaches through it — but
// "uncounted" must not mean "uncapped": relayed peers entered the map with
// no ceiling at all, so a node fed relay sessions by an eager supernode
// grew without bound. The floor keeps tiny/test nodes (MaxPeers=1 relay
// leaves) holding their relay fan-out.
func relayedPeerCap(maxPeers int) int {
	if maxPeers < 2 {
		return 2
	}
	return maxPeers
}

func (n *Node) relayedPeerCountLocked() int {
	count := 0
	for _, peer := range n.peers {
		if peer != nil && peer.relayed {
			count++
		}
	}
	return count
}

// registerRelayedPeerLocked adds a relayed peer to n.peers. The bool return
// reports a refusal the caller must act on rather than treat the nil peer as
// a silent no-op — CAPACITY (relayed fan-out at cap) or POLICY (allowlist):
// either way the relay session must be torn down, or a refused node would
// half-ack the opener and leave it holding a session nobody answers.
func (n *Node) registerRelayedPeerLocked(session relayLocalSession) (*peerConn, bool) {
	if session.remotePeerID == "" || session.viaPeerID == "" || session.sessionID == "" {
		return nil, false
	}
	if len(n.knownPeers[session.remotePeerID].noiseStatic) != 32 {
		return nil, false
	}
	existing := n.peers[session.remotePeerID]
	if existing != nil && !existing.relayed {
		return nil, false
	}
	// Allowlist gate — the same admission policy as the direct path
	// (registerPeerFrom), so a strict node refuses an unlisted remote over
	// relay exactly as over a direct dial, and the refusal is counted, never
	// silently dropped. Only genuinely NEW remotes are gated: a session
	// migration (existing remote, new sessionID) replaces the entry,
	// mirroring the cap exemption below. Inlined rather than IsPeerAllowed
	// because this runs under n.mu. Policy is checked before capacity so the
	// counter reflects the operator's intent, not the node's load.
	if existing == nil && n.allowlist != nil {
		if _, ok := n.allowlist[session.remotePeerID]; !ok {
			n.countInbound("__allowlist_rejected__")
			return nil, true
		}
	}
	// A session migration (same remote, new sessionID) replaces the old
	// entry and nets no growth; only a genuinely new remote consumes cap.
	if existing == nil && n.relayedPeerCountLocked() >= relayedPeerCap(n.config.MaxPeers) {
		n.countInbound("__relay_peer_capped__")
		return nil, true
	}
	peer := &peerConn{
		id:             session.remotePeerID,
		addr:           "relay:" + session.viaPeerID,
		relayed:        true,
		viaPeerID:      session.viaPeerID,
		relaySessionID: session.sessionID,
		connectedAt:    time.Now(),
	}
	n.peers[session.remotePeerID] = peer
	info := n.knownPeers[session.remotePeerID]
	info.id = session.remotePeerID
	info.direct = false
	info.lastSeen = time.Now()
	n.knownPeers[session.remotePeerID] = info
	n.scoring.Ensure(session.remotePeerID)
	return peer, false
}

func (n *Node) activateRelayedPeer(peer *peerConn) {
	if peer == nil {
		return
	}
	n.sendKnownPeerSnapshot(peer)
	for _, channel := range n.pubsub.SnapshotLocal() {
		n.maintainTopicMesh(channel)
	}
	n.enqueueEvent(EventPeerJoined, map[string]string{"peer": peer.id, "addr": peer.addr})
}

func (n *Node) relayPeerForSession(session relayLocalSession) *peerConn {
	n.mu.RLock()
	peer := n.peers[session.remotePeerID]
	n.mu.RUnlock()
	if peer != nil && peer.relayed && peer.relaySessionID == session.sessionID {
		return peer
	}
	return &peerConn{id: session.remotePeerID, addr: "relay:" + session.viaPeerID, relayed: true, viaPeerID: session.viaPeerID, relaySessionID: session.sessionID}
}

func (n *Node) removeRelayedPeerLocked(session relayLocalSession) bool {
	peer := n.peers[session.remotePeerID]
	if peer == nil || !peer.relayed || peer.relaySessionID != session.sessionID {
		return false
	}
	delete(n.peers, session.remotePeerID)
	return true
}

// relayConsumerCapped charges bytes to the source peer's rolling per-minute
// budget and reports whether it now exceeds NAT.RelayConsumerCapBytes. The
// entry is dropped when it trips: the caller tears the session down, and a
// fresh consumer gets a clean window rather than an inherited full one.
// Zero cap (the default) disables the guard entirely.
func (n *Node) relayConsumerCapped(consumerID string, bytes int64, now time.Time) bool {
	cap := n.config.NAT.RelayConsumerCapBytes
	if cap <= 0 {
		return false
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.relayConsumers == nil {
		n.relayConsumers = make(map[string]*relayConsumer)
	}
	budget := n.relayConsumers[consumerID]
	if budget == nil {
		budget = &relayConsumer{windowStart: now}
		n.relayConsumers[consumerID] = budget
	}
	if budget.charge(now, bytes) <= cap {
		return false
	}
	delete(n.relayConsumers, consumerID)
	return true
}

// closeRelayRouteWithClose reaps a middle-node route whose consumer tripped
// the cap and tells both endpoints, so neither keeps sending into a route
// that no longer exists. A silent drop here would leave the origin convinced
// the relay still carries it: the refusal has to be observable, and an
// explicit close on both legs is the only thing that propagates past the
// relay itself.
func (n *Node) closeRelayRouteWithClose(sessionID, source, target string) {
	n.mu.Lock()
	_, hadRoute := n.relayRoutes[sessionID]
	delete(n.relayRoutes, sessionID)
	sourcePeer := n.peers[source]
	targetPeer := n.peers[target]
	n.mu.Unlock()
	if !hadRoute {
		return
	}
	n.relaySessions.Release(sessionID)
	n.refreshSupernodeStatus()
	msg := gossip.Envelope{
		Type:         gossip.TypeRelayClose,
		RelaySession: sessionID,
		RelaySource:  source,
		RelayTarget:  target,
	}
	if sourcePeer != nil {
		n.sendEnvelope(sourcePeer, msg)
	}
	if targetPeer != nil && targetPeer != sourcePeer {
		n.sendEnvelope(targetPeer, msg)
	}
}
