package mesh

import (
	"context"
	"net"
	"net/netip"
	"net/url"
	"strings"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/nat"
)

// requestSTUNBindingObservations samples up to `want` DISTINCT vantage points
// and returns everything it got.
//
// One observation cannot classify a NAT. Symmetric NAT is *defined* by the
// mapped port differing per destination, so a single vantage point has nothing
// to compare against and the profiler can only answer "unknown" — which is what
// it answered for the entire fleet, always. The telemetry made it plain: every
// event carried observations=1, so ports_differed was never once computed and
// no node ever discovered it was behind symmetric NAT.
//
// That is not cosmetic. shouldPreferRelayBetween suppresses a hole punch only
// when BOTH ends are known symmetric, so an unclassified node kept punching at
// targets it could never reach — ~10s burnt per attempt, and the bulk of the
// failures the fleet reports.
func (n *Node) requestSTUNBindingObservations(timeout time.Duration, want int) []string {
	if n.udpListener == nil || timeout <= 0 || want <= 0 || !n.shouldUseSTUNBootstrap() {
		return nil
	}
	deadline := time.Now().Add(timeout)
	observations := make([]string, 0, want)
	for _, server := range defaultSTUNServers {
		if len(observations) >= want {
			break
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		ctx, cancel := context.WithTimeout(context.Background(), minDuration(remaining, 1500*time.Millisecond))
		observed, err := n.udpListener.ObserveSTUNContext(ctx, server)
		cancel()
		if err == nil && observed != "" {
			observations = append(observations, observed)
		}
	}
	return observations
}

// refreshNATClassification compares vantage points sampled in one round and
// lets the profiler decide.
//
// The comparison must run on that round's own set: appendObservation collapses
// consecutive duplicates, so feeding these through the long-lived history would
// fold a cone NAT's two identical mappings back into one and destroy the very
// evidence proving it is not symmetric.
func (n *Node) refreshNATClassification(timeout time.Duration) bool {
	observations := n.requestSTUNBindingObservations(timeout, 2)
	if len(observations) < 2 {
		return false
	}
	profile, ok := n.natProfile.Load().(nat.Profile)
	if !ok {
		return false
	}
	classified := n.profiler.WithBindingObservations(profile, observations)
	// Adopt ONLY a symmetric verdict.
	//
	// When the ports agree, the profiler also upgrades Unknown →
	// port_restricted_cone — and that is poison this early. A public node starts
	// out Unknown and reaches public only once an inbound probe confirms it, via
	// a path that fires solely from Unknown (labelExternalReachability). Taking
	// the cone label first locks a relay out of being public forever: deployed
	// to the fleet, it turned every box into port_restricted_cone with
	// supernode_ready=false and stopped the relays from relaying.
	//
	// The upgrade never used to fire because the classifier was only ever handed
	// one observation and returned unchanged. Giving it two woke it up — so the
	// verdict worth having here is the one that needed two samples in the first
	// place, and nothing else.
	if classified.Type != nat.TypeSymmetric {
		return false
	}
	n.natProfile.Store(classified)
	return true
}

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
		// A peer's UDP observe reports the mapping it genuinely saw, so it can
		// inform classification. The gossip reply cannot: it echoes back the
		// port we advertised ourselves, which is a fine address to advertise and
		// worthless as evidence about our NAT.
		if observed, ok := n.requestUDPBindingObservation(peerID, remaining); ok {
			updated = n.applyExternalObservation(observed, deadline) || updated
			continue
		}
		if observed, ok := n.requestBindingObservation(peerID, remaining); ok {
			updated = n.applySyntheticObservation(observed, deadline) || updated
		}
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

// applyExternalObservation records a genuine observation of our own mapping —
// STUN, a port mapper, or a peer's UDP observe. Only these may reach the NAT
// classifier, because only these actually report what the far side saw.
func (n *Node) applyExternalObservation(observed string, deadline time.Time) bool {
	return n.applyObservation(observed, deadline, true)
}

// applySyntheticObservation records an address that is usable for advertising
// but says nothing about our NAT.
//
// The gossip binding reply is built from the observed HOST plus the port the
// asker itself advertised (see handleBindingRequest) — the right answer to
// "what is my dialable address", and a non-answer to "what is my mapping": it
// hands back our own port, so it is a constant no matter who we ask. Feeding it
// to the classifier is what pinned the whole fleet at observations=1 and
// nat_type=unknown, since appendObservation then collapsed every identical
// echo into a single entry and left nothing to compare.
func (n *Node) applySyntheticObservation(observed string, deadline time.Time) bool {
	return n.applyObservation(observed, deadline, false)
}

// applyObservation folds an observed address into the profile. mapping reports
// whether it is a true observation of our NAT mapping and may inform
// classification, or merely an address good enough to advertise.
func (n *Node) applyObservation(observed string, deadline time.Time, mapping bool) bool {
	if observed == "" {
		return false
	}
	previous := n.natProfile.Load().(nat.Profile)
	observed = preferredExternalAddr(previous.ExternalAddress, observed)
	profile := n.profiler.WithExternalAddress(previous, observed)

	if mapping {
		n.mu.Lock()
		n.bindingHistory = appendObservation(n.bindingHistory, observed)
		bindingHistory := append([]string(nil), n.bindingHistory...)
		n.mu.Unlock()
		profile = n.profiler.WithBindingObservations(profile, bindingHistory)
	}
	if requiresReachabilityConfirmation(observed) && !profile.PublicReachable {
		profile = n.profiler.WithReachability(profile, n.confirmReachability(observed, deadline))
	}
	// Decide the public/CGNAT label from *confirmed inbound reachability*, never
	// from address shape. A public reflexive address with no inbound reach and a
	// private local interface is carrier/provider NAT (CGNAT) — it must not be
	// labelled public nor become a supernode.
	profile = n.labelExternalReachability(profile, observed)
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
