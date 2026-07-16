package mesh

import (
	"encoding/json"
	"net"
	"net/netip"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/nat"
)

func (n *Node) sendEnvelope(peer *peerConn, env gossip.Envelope) bool {
	if peer == nil {
		return false
	}
	if n.isPeerGraylisted(peer.id) {
		return false
	}
	if peer.relayed {
		return n.sendRelayedEnvelope(peer, env)
	}
	return n.sendDirectEnvelope(peer, env)
}

func (n *Node) sendDirectEnvelope(peer *peerConn, env gossip.Envelope) bool {
	if peer == nil || peer.session == nil {
		return false
	}
	payload, err := json.Marshal(env)
	if err != nil {
		return false
	}
	return peer.session.WritePacket(payload) == nil
}

func (n *Node) sendKnownPeerSnapshot(peer *peerConn) {
	if peer == nil || !n.canSharePeerExchangeWithPeer(peer.id) {
		return
	}
	n.sendEnvelope(peer, n.peerAnnouncementEnvelope(n.localKnownPeer()))

	n.mu.RLock()
	known := make([]knownPeer, 0, len(n.knownPeers))
	for _, info := range n.knownPeers {
		known = append(known, info)
	}
	n.mu.RUnlock()
	for _, info := range known {
		if info.id == peer.id || info.addr == "" {
			continue
		}
		n.sendEnvelope(peer, n.peerAnnouncementEnvelope(info))
	}
	n.announceLocalSubscriptionsToPeer(peer)
}

func (n *Node) announceLocalSubscription(channel string) {
	if !validChannel(channel) {
		return
	}
	n.mu.RLock()
	peers := make([]*peerConn, 0, len(n.peers))
	for _, peer := range n.peers {
		peers = append(peers, peer)
	}
	n.mu.RUnlock()
	for _, peer := range peers {
		if !n.canGossipWithPeer(peer.id) {
			continue
		}
		n.sendEnvelope(peer, gossip.Envelope{Type: gossip.TypeGraft, Channel: channel})
	}
}

// introduceSelfTo tells a peer that just joined what we are, immediately.
//
// Capabilities are never taken from a plain peer-announce — deliberately, since
// anyone could claim them — so a peer learns we are publicly reachable only from
// a signed SupernodeAnnounce. refreshSupernodeStatus emits one on a status
// CHANGE, and reannounceSupernodeStatus re-broadcasts periodically; a peer that
// joins in between waits for the next tick to find out what it is attached to.
//
// The overlay makes that wait expensive: only a peer known to be publicly
// reachable becomes a routing contact, so until this lands the joiner has an
// empty table — nobody to look up through and nobody to publish to, and since
// publishing needs the same table a lookup does, the layer cannot bootstrap at
// all. Sending it on join costs one envelope and removes the wait.
func (n *Node) introduceSelfTo(peer *peerConn) {
	if peer == nil {
		return
	}
	info := n.localKnownPeer()
	if info.id == "" || info.addr == "" || !info.relayCapable {
		return // nothing trustworthy to say yet
	}
	n.sendEnvelope(peer, n.signSupernodeEnvelope(gossip.Envelope{
		Type:                   gossip.TypeSupernodeAnnounce,
		AdvertisedPeerID:       info.id,
		AdvertisedAddr:         info.addr,
		AdvertisedNATType:      string(info.natType),
		AdvertisedReachable:    info.publicReachable,
		AdvertisedRelayCapable: true,
	}))
}

func (n *Node) announceLocalSubscriptionsToPeer(peer *peerConn) {
	if peer == nil || !n.canGossipWithPeer(peer.id) {
		return
	}
	for _, channel := range n.pubsub.SnapshotLocal() {
		if !validChannel(channel) {
			continue
		}
		n.sendEnvelope(peer, gossip.Envelope{Type: gossip.TypeGraft, Channel: channel})
	}
}

func (n *Node) refreshLocalSubscriptions() {
	n.mu.RLock()
	peers := make([]*peerConn, 0, len(n.peers))
	for _, peer := range n.peers {
		peers = append(peers, peer)
	}
	n.mu.RUnlock()
	if len(peers) == 0 {
		return
	}
	for _, peer := range peers {
		n.announceLocalSubscriptionsToPeer(peer)
	}
}

func (n *Node) broadcastPeerAnnouncement(info knownPeer, excludePeerID string) {
	if info.id == "" || info.addr == "" {
		return
	}
	n.broadcastToAll(n.peerAnnouncementEnvelope(info), excludePeerID)
}

func (n *Node) peerAnnouncementEnvelope(info knownPeer) gossip.Envelope {
	env := gossip.Envelope{
		Type:                   gossip.TypePeerAnnounce,
		AdvertisedPeerID:       info.id,
		AdvertisedAddr:         info.addr,
		AdvertisedNATType:      string(info.natType),
		AdvertisedReachable:    info.publicReachable,
		AdvertisedRelayCapable: info.relayCapable,
		AdvertisedNoiseStatic:  append([]byte(nil), info.noiseStatic...),
	}
	if info.id == n.localPeerID() {
		return n.signPeerAnnouncementEnvelope(env)
	}
	env.AdvertisedSignature = append([]byte(nil), info.signature...)
	return env
}

func (n *Node) localKnownPeer() knownPeer {
	profile := n.natProfile.Load().(nat.Profile)
	return knownPeer{
		id:              n.localPeerID(),
		addr:            n.advertisedListenAddr(),
		direct:          true,
		verified:        true,
		bootstrap:       false,
		lan:             false,
		natType:         profile.Type,
		natTrusted:      true,
		publicReachable: profile.PublicReachable,
		relayCapable:    n.supernodeReady(profile),
		lastSeen:        time.Now(),
		noiseStatic:     n.identity.NoiseStaticPublic(),
	}
}

func (n *Node) handlePeerAnnounce(peer *peerConn, env gossip.Envelope) {
	verified := directSenderMatches(peer, env) && verifyPeerAnnouncementEnvelope(env)
	n.handleKnownPeerEnvelope(peer, env, gossip.TypePeerAnnounce, verified)
}

func (n *Node) handleSupernodeStatus(peer *peerConn, env gossip.Envelope, relayCapable bool) {
	env.AdvertisedRelayCapable = relayCapable
	if !verifySupernodeStatusEnvelope(env) {
		if peer != nil {
			n.scoring.PenalizeInvalid(peer.id)
		}
		return
	}
	n.handleKnownPeerEnvelope(peer, env, env.Type, directSenderMatches(peer, env))
}

func directSenderMatches(peer *peerConn, env gossip.Envelope) bool {
	return peer != nil && env.AdvertisedPeerID == peer.id
}

// isLoopbackAddr reports whether a host:port advertises a loopback host.
func isLoopbackAddr(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	return isLoopbackHost(host)
}

// peerOnLoopback reports whether the peer we received an announcement from is
// itself on loopback — the only context where a loopback advertisement is real.
func peerOnLoopback(peer *peerConn) bool {
	return peer != nil && isLoopbackAddr(peer.addr)
}

// sameHostIP reports whether two host:port addresses share the same host,
// ignoring the port — so an ephemeral port flip on the same peer is not treated
// as an address change worth re-flooding to the whole network.
func sameHostIP(a, b string) bool {
	ha, _, err := net.SplitHostPort(a)
	if err != nil {
		ha = a
	}
	hb, _, err := net.SplitHostPort(b)
	if err != nil {
		hb = b
	}
	return ha == hb
}

func (n *Node) handleKnownPeerEnvelope(peer *peerConn, env gossip.Envelope, forwardType gossip.EnvelopeType, verifiedEnvelope bool) {
	if env.AdvertisedPeerID == "" || env.AdvertisedAddr == "" || env.AdvertisedPeerID == n.localPeerID() {
		return
	}
	// On the shared substrate a peer that advertises a loopback address is
	// unreachable to everyone else — storing it just pollutes the directory and
	// re-gossips junk network-wide. Drop it unless the announcement arrives over
	// a loopback path (a genuine local/offline test, where loopback is real).
	if isLoopbackAddr(env.AdvertisedAddr) && !peerOnLoopback(peer) {
		return
	}
	trustedSelfAnnouncement := peer != nil && env.AdvertisedPeerID == peer.id
	validSignedAnnouncement := false
	if forwardType == gossip.TypePeerAnnounce {
		validSignedAnnouncement = verifyPeerAnnouncementEnvelope(env)
		if !trustedSelfAnnouncement && !verifiedEnvelope && !validSignedAnnouncement {
			return
		}
	}
	trustCapabilities := verifySupernodeStatusEnvelope(env)
	changed := false
	n.mu.Lock()
	current, ok := n.knownPeers[env.AdvertisedPeerID]
	addr := preferredKnownPeerAddr(current, env.AdvertisedAddr)
	liveSessionAddr := ""
	if peer != nil && env.AdvertisedPeerID == peer.id {
		liveSessionAddr = peer.addr
	}
	if shouldFreezeDirectKnownPeerAddr(current, env.AdvertisedAddr, liveSessionAddr) {
		addr = current.addr
	}
	lan := current.lan && knownPeerAddrRank(addr) <= 1
	verified := current.verified || verifiedEnvelope || (peer != nil && env.AdvertisedPeerID == peer.id)
	thirdPartyDialable := current.thirdPartyDialable && current.addr == addr
	if verified {
		thirdPartyDialable = true
	} else if validSignedAnnouncement && !trustedSelfAnnouncement && env.AdvertisedAddr == addr {
		thirdPartyDialable = thirdPartyAnnouncementDialable(peer, env.AdvertisedAddr)
	}
	natType := current.natType
	natTrusted := current.natTrusted
	publicReachable := current.publicReachable
	relayCapable := current.relayCapable
	if trustCapabilities {
		natType = nat.Type(env.AdvertisedNATType)
		natTrusted = true
		publicReachable = env.AdvertisedReachable
		relayCapable = env.AdvertisedRelayCapable
	}
	signature := knownPeerSignature(current, addr, env, verifiedEnvelope || validSignedAnnouncement)
	predictionObservations := current.predictionObservations
	if peer != nil && env.AdvertisedPeerID == peer.id {
		predictionObservations = appendObservation(predictionObservations, liveSessionAddr)
	}
	noiseStatic := knownPeerNoiseStaticFromEnvelope(current, env, verifiedEnvelope || validSignedAnnouncement)
	if !ok || current.addr != addr || !current.direct || current.verified != verified || current.thirdPartyDialable != thirdPartyDialable || current.natType != natType || current.natTrusted != natTrusted || current.publicReachable != publicReachable || current.relayCapable != relayCapable || !equalBytes(current.noiseStatic, noiseStatic) || !equalBytes(current.signature, signature) {
		direct := false
		if ok && current.direct {
			direct = true
		}
		bootstrap := current.bootstrap
		if peer != nil && peer.outbound && env.AdvertisedPeerID == peer.id && peer.bootstrap {
			bootstrap = true
		}
		n.knownPeers[env.AdvertisedPeerID] = knownPeer{
			id:                     env.AdvertisedPeerID,
			addr:                   addr,
			direct:                 direct,
			verified:               verified,
			bootstrap:              bootstrap,
			lan:                    lan,
			natType:                natType,
			natTrusted:             natTrusted,
			publicReachable:        publicReachable,
			relayCapable:           relayCapable,
			lastSeen:               time.Now(),
			observations:           appendObservation(current.observations, env.AdvertisedAddr),
			predictionObservations: predictionObservations,
			noiseStatic:            noiseStatic,
			signature:              signature,
			thirdPartyDialable:     thirdPartyDialable,
		}
		changed = true
	}
	n.mu.Unlock()
	// Re-flood a peer announcement only on a MEANINGFUL change — a new peer, or
	// a change in capability / reachability / NAT type / identity / IP. A bare
	// port flip (symmetric-NAT and ephemeral-egress peers churn their source
	// port constantly) is useless to everyone else — you cannot dial an
	// ephemeral port — yet re-broadcasting each flip to every peer turned the
	// shared substrate into a gossip storm (large sustained egress). The local
	// directory still records the latest addr above; we just do not re-flood it.
	meaningfulChange := !ok ||
		current.natType != natType ||
		current.natTrusted != natTrusted ||
		current.publicReachable != publicReachable ||
		current.relayCapable != relayCapable ||
		!equalBytes(current.noiseStatic, noiseStatic) ||
		!sameHostIP(current.addr, addr)
	if changed && meaningfulChange {
		advertisedSignature := append([]byte(nil), env.AdvertisedSignature...)
		if forwardType != gossip.TypePeerAnnounce && (nat.Type(env.AdvertisedNATType) != natType || env.AdvertisedReachable != publicReachable || env.AdvertisedRelayCapable != relayCapable) {
			advertisedSignature = nil
		}
		excludePeerID := ""
		if peer != nil {
			excludePeerID = peer.id
		}
		n.broadcastToAll(gossip.Envelope{
			Type:                   forwardType,
			AdvertisedPeerID:       env.AdvertisedPeerID,
			AdvertisedAddr:         env.AdvertisedAddr,
			AdvertisedNATType:      string(natType),
			AdvertisedReachable:    publicReachable,
			AdvertisedRelayCapable: relayCapable,
			AdvertisedNoiseStatic:  noiseStatic,
			AdvertisedSignature:    advertisedSignature,
		}, excludePeerID)
	}
}

func knownPeerSignature(current knownPeer, addr string, env gossip.Envelope, valid bool) []byte {
	if valid && env.AdvertisedAddr == addr {
		return append([]byte(nil), env.AdvertisedSignature...)
	}
	if current.addr == addr {
		return append([]byte(nil), current.signature...)
	}
	return nil
}

func thirdPartyAnnouncementDialable(peer *peerConn, addr string) bool {
	if knownPeerAddrRank(addr) >= 3 {
		return true
	}
	if peer == nil || peer.addr == "" {
		return false
	}
	return sameHostPortHost(peer.addr, addr)
}

func sameHostPortHost(a, b string) bool {
	aHost, _, err := net.SplitHostPort(a)
	if err != nil {
		return false
	}
	bHost, _, err := net.SplitHostPort(b)
	if err != nil {
		return false
	}
	aIP, err := netip.ParseAddr(aHost)
	if err != nil {
		return false
	}
	bIP, err := netip.ParseAddr(bHost)
	if err != nil {
		return false
	}
	return aIP.Unmap() == bIP.Unmap()
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func knownPeerNoiseStaticFromEnvelope(current knownPeer, env gossip.Envelope, valid bool) []byte {
	if valid && len(env.AdvertisedNoiseStatic) == 32 {
		return append([]byte(nil), env.AdvertisedNoiseStatic...)
	}
	return append([]byte(nil), current.noiseStatic...)
}
