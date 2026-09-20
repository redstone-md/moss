package mesh

import (
	"context"
	"errors"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/nat"
)

// holePunchCoordGrace is how long after a coordination timestamp a punch waits
// for the target to connect before re-sending the offer through the relay.
// The target's coordinator blocks on its own address observation (up to
// 750ms) before it dials, so a retry landing that late is coordination noise,
// not failure — re-sending keeps the punch alive across a missed offer.
const holePunchCoordGrace = 750 * time.Millisecond

// holePunchCoordRetryLimit bounds offer re-sends per punch attempt. The
// target re-dials idempotently on a duplicate offer, so a retry only
// re-coordinates; beyond this the relay path itself is presumed broken.
const holePunchCoordRetryLimit = 2

// bindingSampleHistoryCap bounds the raw observed-binding history kept for
// classification. Raw means consecutive duplicates survive: "the last three
// mappings were identical" is exactly the evidence that walks a symmetric
// verdict back down to a cone once the NAT's mappings stabilise, and a
// collapsing writer would erase it.
const bindingSampleHistoryCap = 8

// classifierBindingWindow is how many raw binding samples the NAT classifier
// is handed: enough for WithBindingObservations to see stability, few enough
// that a stale symmetric era cannot outweigh a freshly settled cone.
const classifierBindingWindow = 3

// appendBindingSample records a raw observed binding address, keeping
// consecutive duplicates and capping the history at bindingSampleHistoryCap.
func appendBindingSample(history []string, observed string) []string {
	if observed == "" {
		return history
	}
	history = append(history, observed)
	if len(history) > bindingSampleHistoryCap {
		history = history[len(history)-bindingSampleHistoryCap:]
	}
	return history
}

// recentBindingWindow copies out the newest raw binding samples for the
// profiler. Callers must not hold n.mu.
func (n *Node) recentBindingWindow() []string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if len(n.bindingHistory) <= classifierBindingWindow {
		return append([]string(nil), n.bindingHistory...)
	}
	return append([]string(nil), n.bindingHistory[len(n.bindingHistory)-classifierBindingWindow:]...)
}

// attemptHolePunch honours the relay preference; attemptHolePunchPolicy with
// force=true is the upgrade path, where a relayed peer is retried for a direct
// path with nobody waiting on the result.
func (n *Node) attemptHolePunch(targetPeerID string, timeout time.Duration) bool {
	return n.attemptHolePunchPolicy(targetPeerID, timeout, false)
}

func (n *Node) attemptHolePunchPolicy(targetPeerID string, timeout time.Duration, force bool) bool {
	if timeout <= 0 {
		return false
	}
	if !force && n.shouldPreferRelayForTarget(targetPeerID) {
		return false
	}
	n.mu.RLock()
	targetInfo, ok := n.knownPeers[targetPeerID]
	n.mu.RUnlock()
	if !ok || targetInfo.addr == "" {
		return false
	}
	viaPeerID, err := n.selectRelayPeer(targetPeerID)
	if err != nil {
		return false
	}
	n.mu.RLock()
	viaPeer := n.peers[viaPeerID]
	n.mu.RUnlock()
	if viaPeer == nil {
		return false
	}
	requestID, err := newRelaySessionID()
	if err != nil {
		return false
	}
	sourceAddr := n.freshObservedUDPAddr(viaPeerID, minDuration(750*time.Millisecond, timeout/3))
	coordAt := time.Now().Add(holePunchCoordGrace)
	coordRetries := 0
	deadline := time.Now().Add(timeout)
	// The dial goroutines inherit the attempt's deadline so they cannot
	// outlive it by a fresh handshake budget.
	go n.tryHolePunchDialAt(targetPeerID, targetInfo.addr, coordAt, deadline)
	n.mu.Lock()
	n.holePunchWait[requestID] = holePunchRequest{targetPeerID: targetPeerID, relayPeerID: viaPeerID}
	n.mu.Unlock()
	defer func() {
		n.mu.Lock()
		delete(n.holePunchWait, requestID)
		n.mu.Unlock()
	}()
	n.countInbound("__punch_attempt__")
	n.sendEnvelope(viaPeer, n.signedHolePunchCoordEnvelope(gossip.Envelope{
		Type:           gossip.TypeHolePunchCoord,
		RequestID:      requestID,
		CoordStage:     "offer",
		CoordAt:        coordAt.UnixMilli(),
		RelaySource:    n.localPeerID(),
		RelayTarget:    targetPeerID,
		AdvertisedAddr: sourceAddr,
	}))
	n.emitPunchAttempt(targetPeerID, n.knownPeerNATType(targetPeerID), viaPeerID)
	punchStarted := time.Now()

	triedAddr := targetInfo.addr
	for time.Now().Before(deadline) {
		if n.directPeerConnected(targetPeerID) {
			n.countInbound("__punch_success__")
			n.emitPunchResult(targetPeerID, n.knownPeerNATType(targetPeerID), true, time.Since(punchStarted))
			return true
		}
		if coordRetries < holePunchCoordRetryLimit && time.Now().After(coordAt.Add(holePunchCoordGrace)) {
			n.mu.RLock()
			_, waitAlive := n.holePunchWait[requestID]
			viaNow := n.peers[viaPeerID]
			n.mu.RUnlock()
			if !waitAlive || viaNow == nil {
				coordRetries = holePunchCoordRetryLimit
			} else {
				// The offer can be lost in relay transit or the target can miss
				// its coordination window: it blocks on its own address
				// observation (up to holePunchCoordGrace) before dialling, so
				// it may dial after the coordinated moment. Re-send the offer
				// under the same request ID — the target re-dials idempotently
				// and a duplicate reply is discarded by request validation.
				coordAt = time.Now().Add(holePunchCoordGrace)
				coordRetries++
				n.countInbound("__punch_coord_retry__")
				n.sendEnvelope(viaNow, n.signedHolePunchCoordEnvelope(gossip.Envelope{
					Type:           gossip.TypeHolePunchCoord,
					RequestID:      requestID,
					CoordStage:     "offer",
					CoordAt:        coordAt.UnixMilli(),
					RelaySource:    n.localPeerID(),
					RelayTarget:    targetPeerID,
					AdvertisedAddr: sourceAddr,
				}))
			}
		}
		n.mu.RLock()
		updated := n.knownPeers[targetPeerID].addr
		n.mu.RUnlock()
		if updated != "" && updated != triedAddr {
			triedAddr = updated
			go n.tryHolePunchDialAt(targetPeerID, updated, time.Time{}, deadline)
		}
		time.Sleep(25 * time.Millisecond)
	}
	// A punch that ran out of time is the single most useful negative result in
	// the whole NAT layer: it is what fills in which pairs of NAT types never
	// work, and that cannot be inferred from successes alone.
	ok = n.directPeerConnected(targetPeerID)
	if ok {
		n.countInbound("__punch_success__")
	} else {
		n.countInbound("__punch_timeout__")
	}
	n.emitPunchResult(targetPeerID, n.knownPeerNATType(targetPeerID), ok, time.Since(punchStarted))
	return ok
}

// signedHolePunchCoordEnvelope stamps a coordination envelope with this
// node's self profile so the far side learns its NAT type at punch time.
// The claims are the same facts a signed supernode-status announce carries.
// A node with no NAT classification yet sends an unsigned envelope, which a
// receiver treats exactly as a legacy one — an "unknown" profile would only
// overwrite what the directory already knows.
func (n *Node) signedHolePunchCoordEnvelope(env gossip.Envelope) gossip.Envelope {
	info := n.localKnownPeer()
	if info.natType == "" || info.natType == nat.TypeUnknown {
		return env
	}
	env.AdvertisedPeerID = info.id
	env.AdvertisedNATType = string(info.natType)
	env.AdvertisedReachable = info.publicReachable
	env.AdvertisedRelayCapable = info.relayCapable
	return n.signHolePunchCoordEnvelope(env)
}

func (n *Node) tryHolePunchDial(targetPeerID, addr string) {
	// The standalone entrypoints (coordinator reply, tests) run on their own:
	// there is no parent attempt whose budget should bound them, so they keep
	// a fresh per-plan handshake budget per dial.
	n.tryHolePunchDialAt(targetPeerID, addr, time.Time{}, time.Time{})
}

func (n *Node) tryHolePunchDialAt(targetPeerID, addr string, at time.Time, deadline time.Time) {
	if addr == "" || n.directPeerConnected(targetPeerID) {
		return
	}
	if !at.IsZero() {
		delay := time.Until(at)
		if delay > 0 {
			time.Sleep(delay)
		}
	}
	n.mu.RLock()
	localHistory := append([]string(nil), n.bindingHistory...)
	targetInfo := n.knownPeers[targetPeerID]
	remoteHistory := append([]string(nil), targetInfo.predictionObservations...)
	enablePrediction := n.config.NAT.PortPredictionEnabled
	n.mu.RUnlock()
	plan := nat.Coordinator{
		Attempts:           max(1, n.config.NAT.HolePunchAttempts),
		EnablePrediction:   enablePrediction,
		LocalObservations:  localHistory,
		RemoteObservations: remoteHistory,
	}.Plan(n.advertisedListenAddr(), addr)
	for _, pair := range plan {
		if n.directPeerConnected(targetPeerID) {
			return
		}
		budget := n.config.HandshakeTimeout()
		if !deadline.IsZero() {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				return
			}
			budget = minDuration(budget, remaining)
		}
		ctx, cancel := context.WithTimeout(context.Background(), budget)
		n.connectPeerUDP(ctx, targetPeerID, pair.Remote)
		cancel()
		if n.directPeerConnected(targetPeerID) {
			return
		}
		if !deadline.IsZero() && !time.Now().Add(75*time.Millisecond).Before(deadline) {
			return
		}
		time.Sleep(75 * time.Millisecond)
	}
}

// freshObservedUDPAddr samples a live mapping through a relay peer, folding it
// into the binding history, and falls back to the advertised address.
func (n *Node) freshObservedUDPAddr(peerID string, timeout time.Duration) string {
	if timeout > 0 {
		if observed, ok := n.requestUDPBindingObservation(peerID, timeout); ok && observed != "" {
			previous := n.natProfile.Load().(nat.Profile)
			profile := n.profiler.WithExternalAddress(previous, observed)
			n.mu.Lock()
			n.bindingHistory = appendBindingSample(n.bindingHistory, observed)
			n.mu.Unlock()
			profile = n.profiler.WithBindingObservations(profile, n.recentBindingWindow())
			n.natProfile.Store(profile)
			return observed
		}
	}
	return n.advertisedListenAddr()
}

func (n *Node) connectPeerUDP(ctx context.Context, targetPeerID, addr string) {
	_ = n.connectPeerUDPWithHint(ctx, targetPeerID, addr)
}

func (n *Node) waitForDirectPeer(targetPeerID string, timeout time.Duration) bool {
	if timeout <= 0 {
		return n.directPeerConnected(targetPeerID)
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if n.directPeerConnected(targetPeerID) {
			return true
		}
		time.Sleep(25 * time.Millisecond)
	}
	return n.directPeerConnected(targetPeerID)
}

func (n *Node) updateKnownPeer(peerID, addr string, direct bool) {
	if peerID == "" || addr == "" || peerID == n.localPeerID() {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	current, ok := n.knownPeers[peerID]
	if ok && current.direct {
		direct = true
	}
	addr = preferredKnownPeerAddr(current, addr)
	n.knownPeers[peerID] = knownPeer{
		id:                     peerID,
		addr:                   addr,
		direct:                 direct,
		verified:               current.verified || direct,
		bootstrap:              current.bootstrap,
		lan:                    current.lan && knownPeerAddrRank(addr) <= 1,
		natType:                current.natType,
		natTrusted:             current.natTrusted,
		publicReachable:        current.publicReachable,
		relayCapable:           current.relayCapable,
		lastSeen:               time.Now(),
		observations:           appendObservation(current.observations, addr),
		predictionObservations: append([]string(nil), current.predictionObservations...),
		noiseStatic:            append([]byte(nil), current.noiseStatic...),
	}
}

func (n *Node) cachedRemoteStatic(peerID, addr string) []byte {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if peerID != "" {
		if info, ok := n.knownPeers[peerID]; ok && len(info.noiseStatic) == 32 {
			return append([]byte(nil), info.noiseStatic...)
		}
	}
	if addr == "" {
		return nil
	}
	for _, info := range n.knownPeers {
		if info.addr == addr && len(info.noiseStatic) == 32 {
			return append([]byte(nil), info.noiseStatic...)
		}
	}
	return nil
}

func (n *Node) connectPeerUDPWithHint(ctx context.Context, targetPeerID, addr string) error {
	if n.udpListener == nil || addr == "" {
		return errors.New("udp transport unavailable")
	}
	remoteStatic := n.cachedRemoteStatic(targetPeerID, addr)
	session, err := n.udpListener.DialPeerContext(ctx, addr, remoteStatic)
	if err != nil && len(remoteStatic) == 32 && ctx.Err() == nil {
		session, err = n.udpListener.DialContext(ctx, addr)
	}
	if err != nil {
		return err
	}
	// The handshake only proves the path at handshake time. The confirm phase
	// demands a mesh-level reply through this same candidate before the
	// session becomes a peer — a candidate that stays silent is returned as a
	// dial failure, so the punch plan keeps hunting and the dial budget
	// charges it instead of squatting a peer slot for six missed pings.
	return n.confirmAndRegisterUDPPeer(ctx, session, true, originHolePunchUDP)
}

func (n *Node) connectBootstrapPeer(ctx context.Context, addr string) error {
	if addr == "" {
		return errors.New("peer address is required")
	}
	// The TCP and UDP halves below each derive from this context
	// (context.WithCancel), and connectPeerOnce refuses a nil one before
	// DialContext can panic on it. Refuse here for the same reason: the
	// double dial is launched from goroutines whose errors come back through
	// a channel, so a panic in either half would take the whole node down.
	if ctx == nil {
		return errors.New("mesh: bootstrap dial requires a non-nil context")
	}
	if n.udpListener == nil {
		return n.connectPeer(ctx, addr)
	}
	attemptCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan error, 2)
	go func() {
		results <- n.connectPeer(attemptCtx, addr)
	}()
	go func() {
		results <- n.connectPeerUDPWithHint(attemptCtx, "", addr)
	}()
	var firstErr error
	for range 2 {
		// Never block forever on a hung dial: if the caller's context is
		// cancelled while a goroutine is still dialing, bail out instead of
		// waiting. The channel is buffered (cap 2), so both goroutines
		// complete their send and exit once attemptCtx winds the dials down.
		select {
		case err := <-results:
			if err == nil {
				cancel()
				return nil
			}
			if firstErr == nil {
				firstErr = err
			}
		case <-ctx.Done():
			// A real dial error already in hand explains the failure better
			// than the context expiry that merely stopped the wait.
			if firstErr != nil {
				return firstErr
			}
			return ctx.Err()
		}
	}
	if firstErr != nil {
		return firstErr
	}
	return nil
}

func (n *Node) connectBootstrapSeed(ctx context.Context, addr string) error {
	if knownPeerAddrRank(addr) < 3 {
		return n.connectPeer(ctx, addr)
	}
	return n.connectBootstrapPeer(ctx, addr)
}
