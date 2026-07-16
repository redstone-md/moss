package mesh

import (
	"context"
	"errors"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/nat"
)

func (n *Node) attemptHolePunch(targetPeerID string, timeout time.Duration) bool {
	if timeout <= 0 {
		return false
	}
	if n.shouldPreferRelayForTarget(targetPeerID) {
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
	coordAt := time.Now().Add(750 * time.Millisecond)
	go n.tryHolePunchDialAt(targetPeerID, targetInfo.addr, coordAt)
	n.mu.Lock()
	n.holePunchWait[requestID] = holePunchRequest{targetPeerID: targetPeerID, relayPeerID: viaPeerID}
	n.mu.Unlock()
	defer func() {
		n.mu.Lock()
		delete(n.holePunchWait, requestID)
		n.mu.Unlock()
	}()
	n.sendEnvelope(viaPeer, gossip.Envelope{
		Type:           gossip.TypeHolePunchCoord,
		RequestID:      requestID,
		CoordStage:     "offer",
		CoordAt:        coordAt.UnixMilli(),
		RelaySource:    n.localPeerID(),
		RelayTarget:    targetPeerID,
		AdvertisedAddr: sourceAddr,
	})
	deadline := time.Now().Add(timeout)
	triedAddr := targetInfo.addr
	for time.Now().Before(deadline) {
		if n.directPeerConnected(targetPeerID) {
			return true
		}
		n.mu.RLock()
		updated := n.knownPeers[targetPeerID].addr
		n.mu.RUnlock()
		if updated != "" && updated != triedAddr {
			triedAddr = updated
			go n.tryHolePunchDial(targetPeerID, updated)
		}
		time.Sleep(25 * time.Millisecond)
	}
	return n.directPeerConnected(targetPeerID)
}

func (n *Node) tryHolePunchDial(targetPeerID, addr string) {
	n.tryHolePunchDialAt(targetPeerID, addr, time.Time{})
}

func (n *Node) tryHolePunchDialAt(targetPeerID, addr string, at time.Time) {
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
		ctx, cancel := context.WithTimeout(context.Background(), n.config.HandshakeTimeout())
		n.connectPeerUDP(ctx, targetPeerID, pair.Remote)
		cancel()
		if n.directPeerConnected(targetPeerID) {
			return
		}
		time.Sleep(75 * time.Millisecond)
	}
}

func (n *Node) freshObservedUDPAddr(peerID string, timeout time.Duration) string {
	if timeout > 0 {
		if observed, ok := n.requestUDPBindingObservation(peerID, timeout); ok && observed != "" {
			previous := n.natProfile.Load().(nat.Profile)
			profile := n.profiler.WithExternalAddress(previous, observed)
			n.mu.Lock()
			n.bindingHistory = appendObservation(n.bindingHistory, observed)
			bindingHistory := append([]string(nil), n.bindingHistory...)
			n.mu.Unlock()
			profile = n.profiler.WithBindingObservations(profile, bindingHistory)
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
	n.registerPeer(session, true)
	return nil
}

func (n *Node) connectBootstrapPeer(ctx context.Context, addr string) error {
	if addr == "" {
		return errors.New("peer address is required")
	}
	if n.udpListener == nil {
		return n.connectPeer(ctx, addr)
	}
	// TCP first, UDP only as a fallback — do NOT race them. A connection-
	// oriented session to a public seed is stable and bidirectional through any
	// NAT. Racing a UDP session alongside it left the seed holding a datagram
	// session whose return path a symmetric-NAT dialer strands (its mapping
	// differs per destination / rebinds), so the seed's pings all missed and it
	// pruned the peer after the 30s grace — an endless ~38s flap. We fall back
	// to UDP only when TCP genuinely cannot connect (e.g. TCP blocked on the
	// path), never when it merely loses a race.
	if err := n.connectPeer(ctx, addr); err == nil {
		return nil
	} else if ctx.Err() != nil {
		return err
	}
	return n.connectPeerUDPWithHint(ctx, "", addr)
}

func (n *Node) connectBootstrapSeed(ctx context.Context, addr string) error {
	if knownPeerAddrRank(addr) < 3 {
		return n.connectPeer(ctx, addr)
	}
	return n.connectBootstrapPeer(ctx, addr)
}
