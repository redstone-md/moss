package mesh

import (
	"context"
	"net"
	"net/netip"
	"net/url"
	"strings"
	"time"

	"moss/internal/gossip"
	"moss/internal/nat"
)

func (n *Node) refreshExternalAddress(deadline time.Time) bool {
	n.mu.RLock()
	peerIDs := make([]string, 0, len(n.peers))
	for peerID := range n.peers {
		peerIDs = append(peerIDs, peerID)
	}
	n.mu.RUnlock()
	updated := false
	if len(peerIDs) == 0 {
		if remaining := time.Until(deadline); remaining > 0 {
			if observed, ok := n.requestSTUNBindingObservation(minDuration(remaining, n.config.HandshakeTimeout())); ok {
				updated = n.applyExternalObservation(observed, deadline) || updated
			}
		}
		return updated
	}
	for _, peerID := range peerIDs {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		observed, ok := n.requestUDPBindingObservation(peerID, remaining)
		if !ok {
			observed, ok = n.requestBindingObservation(peerID, remaining)
		}
		if !ok {
			continue
		}
		updated = n.applyExternalObservation(observed, deadline) || updated
	}
	if !updated {
		if remaining := time.Until(deadline); remaining > 0 {
			if observed, ok := n.requestSTUNBindingObservation(minDuration(remaining, n.config.HandshakeTimeout()/2)); ok {
				updated = n.applyExternalObservation(observed, deadline) || updated
			}
		}
	}
	return updated
}

func (n *Node) requestSTUNBindingObservation(timeout time.Duration) (string, bool) {
	if n.udpListener == nil || timeout <= 0 || !n.shouldUseSTUNBootstrap() {
		return "", false
	}
	deadline := time.Now().Add(timeout)
	for _, server := range defaultSTUNServers {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		ctx, cancel := context.WithTimeout(context.Background(), minDuration(remaining, 1500*time.Millisecond))
		observed, err := n.udpListener.ObserveSTUNContext(ctx, server)
		cancel()
		if err == nil && observed != "" {
			return observed, true
		}
	}
	return "", false
}

func (n *Node) shouldUseSTUNBootstrap() bool {
	for _, tracker := range n.config.Trackers {
		if trackerUsesPublicBootstrap(tracker) {
			return true
		}
	}
	for _, peer := range n.config.StaticPeers {
		host, _, err := net.SplitHostPort(peer)
		if err == nil && !isLoopbackHost(host) {
			return true
		}
	}
	return false
}

func trackerUsesPublicBootstrap(tracker string) bool {
	u, err := url.Parse(tracker)
	if err != nil {
		return false
	}
	host := u.Hostname()
	if host == "" || strings.EqualFold(host, "localhost") {
		return false
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return true
	}
	ip = ip.Unmap()
	return ip.IsGlobalUnicast() && !ip.IsPrivate() && !ip.IsLoopback() && !ip.IsLinkLocalUnicast() && !isCarrierGradeAddr(ip)
}

func (n *Node) applyExternalObservation(observed string, deadline time.Time) bool {
	if observed == "" {
		return false
	}
	previous := n.natProfile.Load().(nat.Profile)
	observed = preferredExternalAddr(previous.ExternalAddress, observed)
	profile := n.profiler.WithExternalAddress(previous, observed)
	n.mu.Lock()
	n.bindingHistory = appendObservation(n.bindingHistory, observed)
	bindingHistory := append([]string(nil), n.bindingHistory...)
	n.mu.Unlock()
	profile = n.profiler.WithBindingObservations(profile, bindingHistory)
	if requiresReachabilityConfirmation(observed) {
		profile = n.profiler.WithReachability(profile, n.confirmReachability(observed, deadline))
	}
	n.natProfile.Store(profile)
	if profile.ExternalAddress != previous.ExternalAddress || profile.Type != previous.Type || profile.PublicReachable != previous.PublicReachable {
		n.broadcastPeerAnnouncement(n.localKnownPeer(), "")
	}
	return true
}

func (n *Node) requestUDPBindingObservation(peerID string, timeout time.Duration) (string, bool) {
	if n.udpListener == nil || timeout <= 0 {
		return "", false
	}
	n.mu.RLock()
	addr := n.knownPeers[peerID].addr
	if addr == "" {
		if peer := n.peers[peerID]; peer != nil {
			addr = peer.addr
		}
	}
	n.mu.RUnlock()
	if addr == "" {
		return "", false
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	observed, err := n.udpListener.ObserveContext(ctx, addr)
	if err != nil || observed == "" {
		return "", false
	}
	return observed, true
}

func (n *Node) requestBindingObservation(peerID string, timeout time.Duration) (string, bool) {
	requestID, err := newRelaySessionID()
	if err != nil {
		return "", false
	}
	wait := make(chan string, 1)
	n.mu.Lock()
	peer := n.peers[peerID]
	if peer == nil {
		n.mu.Unlock()
		return "", false
	}
	n.bindingWait[requestID] = wait
	n.mu.Unlock()
	defer func() {
		n.mu.Lock()
		delete(n.bindingWait, requestID)
		n.mu.Unlock()
	}()
	n.sendEnvelope(peer, gossip.Envelope{
		Type:           gossip.TypeBindingRequest,
		RequestID:      requestID,
		AdvertisedAddr: n.advertisedListenAddr(),
	})
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case observed := <-wait:
		return observed, true
	case <-timer.C:
		return "", false
	}
}

func (n *Node) requestReachabilityProbe(peerID, addr string, timeout time.Duration) bool {
	requestID, err := newRelaySessionID()
	if err != nil {
		return false
	}
	wait := make(chan bool, 1)
	n.mu.Lock()
	peer := n.peers[peerID]
	if peer == nil {
		n.mu.Unlock()
		return false
	}
	n.reachabilityWait[requestID] = wait
	n.mu.Unlock()
	defer func() {
		n.mu.Lock()
		delete(n.reachabilityWait, requestID)
		n.mu.Unlock()
	}()
	n.sendEnvelope(peer, gossip.Envelope{
		Type:           gossip.TypeReachabilityRequest,
		RequestID:      requestID,
		AdvertisedAddr: addr,
	})
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case reachable := <-wait:
		return reachable
	case <-timer.C:
		return false
	}
}
