package mesh

import (
	"encoding/json"
	"net"
	"net/netip"
	"time"

	"moss/internal/gossip"
	"moss/internal/nat"
)

func (n *Node) sendEnvelope(peer *peerConn, env gossip.Envelope) bool {
	if peer == nil || peer.session == nil {
		return false
	}
	if n.isPeerGraylisted(peer.id) {
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

func (n *Node) handleKnownPeerEnvelope(peer *peerConn, env gossip.Envelope, forwardType gossip.EnvelopeType, verifiedEnvelope bool) {
	if env.AdvertisedPeerID == "" || env.AdvertisedAddr == "" || env.AdvertisedPeerID == n.localPeerID() {
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
	if !ok || current.addr != addr || !current.direct || current.verified != verified || current.thirdPartyDialable != thirdPartyDialable || current.natType != natType || current.natTrusted != natTrusted || current.publicReachable != publicReachable || current.relayCapable != relayCapable || !equalBytes(current.signature, signature) {
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
			noiseStatic:            append([]byte(nil), current.noiseStatic...),
			signature:              signature,
			thirdPartyDialable:     thirdPartyDialable,
		}
		changed = true
	}
	n.mu.Unlock()
	if changed {
		advertisedSignature := append([]byte(nil), env.AdvertisedSignature...)
		if forwardType != gossip.TypePeerAnnounce && (nat.Type(env.AdvertisedNATType) != natType || env.AdvertisedReachable != publicReachable || env.AdvertisedRelayCapable != relayCapable) {
			advertisedSignature = nil
		}
		n.broadcastToAll(gossip.Envelope{
			Type:                   forwardType,
			AdvertisedPeerID:       env.AdvertisedPeerID,
			AdvertisedAddr:         env.AdvertisedAddr,
			AdvertisedNATType:      string(natType),
			AdvertisedReachable:    publicReachable,
			AdvertisedRelayCapable: relayCapable,
			AdvertisedSignature:    advertisedSignature,
		}, peer.id)
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
