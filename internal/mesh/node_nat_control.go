package mesh

import (
	"net"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/nat"
)

func (n *Node) handleBindingRequest(peer *peerConn, env gossip.Envelope) {
	if peer == nil || env.RequestID == "" {
		return
	}
	observedAddr := peer.addr
	if env.AdvertisedAddr != "" {
		observedHost, _, errObserved := net.SplitHostPort(peer.addr)
		_, advertisedPort, errAdvertised := net.SplitHostPort(env.AdvertisedAddr)
		if errObserved == nil && errAdvertised == nil {
			observedAddr = net.JoinHostPort(observedHost, advertisedPort)
		}
	}
	n.sendEnvelope(peer, gossip.Envelope{
		Type:         gossip.TypeBindingResponse,
		RequestID:    env.RequestID,
		ObservedAddr: observedAddr,
	})
}

func (n *Node) handleBindingResponse(env gossip.Envelope) {
	if env.RequestID == "" || env.ObservedAddr == "" {
		return
	}
	n.mu.RLock()
	wait := n.bindingWait[env.RequestID]
	n.mu.RUnlock()
	if wait == nil {
		return
	}
	select {
	case wait <- env.ObservedAddr:
	default:
	}
}

func (n *Node) handleReachabilityRequest(peer *peerConn, env gossip.Envelope) {
	if env.RequestID == "" || env.AdvertisedAddr == "" {
		return
	}
	if peer == nil || !sameAdvertisedEndpoint(env.AdvertisedAddr, peer.addr) {
		return
	}
	reachable := probeTCPAddress(env.AdvertisedAddr, minDuration(500*time.Millisecond, n.config.HandshakeTimeout()), n.bindIfIndex)
	n.sendEnvelope(peer, gossip.Envelope{
		Type:      gossip.TypeReachabilityResponse,
		RequestID: env.RequestID,
		Reachable: reachable,
	})
}

func (n *Node) handleReachabilityResponse(env gossip.Envelope) {
	if env.RequestID == "" {
		return
	}
	n.mu.RLock()
	wait := n.reachabilityWait[env.RequestID]
	n.mu.RUnlock()
	if wait == nil {
		return
	}
	select {
	case wait <- env.Reachable:
	default:
	}
}

func normalizeHolePunchCoordAt(coordAtMillis int64, now time.Time) time.Time {
	const (
		offset  = 600 * time.Millisecond
		maxLead = 2 * time.Second
	)
	if coordAtMillis == 0 {
		return now.Add(offset)
	}
	coordAt := time.UnixMilli(coordAtMillis)
	lead := coordAt.Sub(now)
	if lead > maxLead {
		return now.Add(offset)
	}
	return coordAt
}

// applyHolePunchCoordProfile records the NAT profile a coordination envelope
// carries, when it is a valid self-signed claim from the peer it names. A
// punch target is one of the few peers a fresh node knows by address long
// before an announce flood reaches it, and the first punches are the ones
// that decide how fast it spins up — without this, the punch layer spends its
// first round NAT-blind (target_nat:"" for every early punch in the census)
// and the relay preference cannot classify a symmetric pair. Unsigned
// envelopes — legacy senders — are left alone, so a relay peer cannot forge
// a profile on anyone's behalf.
func (n *Node) applyHolePunchCoordProfile(env gossip.Envelope) {
	if env.AdvertisedNATType == "" || env.AdvertisedPeerID == "" || env.AdvertisedPeerID != env.RelaySource {
		return
	}
	if !verifyHolePunchCoordEnvelope(env) {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	info, ok := n.knownPeers[env.RelaySource]
	if !ok {
		return
	}
	info.natType = nat.Type(env.AdvertisedNATType)
	info.natTrusted = true
	info.publicReachable = env.AdvertisedReachable
	info.relayCapable = env.AdvertisedRelayCapable
	n.knownPeers[env.RelaySource] = info
}

// knownPeerNATType reads the directory's current view of a peer's NAT type,
// including profiles just learned from a coordination reply.
func (n *Node) knownPeerNATType(peerID string) nat.Type {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.knownPeers[peerID].natType
}

func (n *Node) handleHolePunchCoord(peer *peerConn, env gossip.Envelope) {
	if peer == nil || env.RelaySource == "" || env.RelayTarget == "" || env.AdvertisedAddr == "" {
		return
	}
	coordAt := normalizeHolePunchCoordAt(env.CoordAt, time.Now())
	if env.RelayTarget == n.localPeerID() {
		if env.CoordStage == "reply" {
			n.mu.Lock()
			request, ok := n.holePunchWait[env.RequestID]
			validReply := ok && request.targetPeerID == env.RelaySource && request.relayPeerID == peer.id
			if validReply {
				delete(n.holePunchWait, env.RequestID)
			}
			n.mu.Unlock()
			if !validReply {
				return
			}
		}
		n.updateKnownPeer(env.RelaySource, env.AdvertisedAddr, false)
		n.applyHolePunchCoordProfile(env)
		if env.CoordStage == "offer" {
			replyAddr := n.freshObservedUDPAddr(peer.id, minDuration(750*time.Millisecond, n.config.HandshakeTimeout()/2))
			go n.tryHolePunchDialAt(env.RelaySource, env.AdvertisedAddr, coordAt, coordAt.Add(n.config.HandshakeTimeout()))
			n.sendEnvelope(peer, gossip.Envelope{
				Type:             gossip.TypePeerAnnounce,
				AdvertisedPeerID: n.localPeerID(),
				AdvertisedAddr:   replyAddr,
			})
			n.sendEnvelope(peer, n.signedHolePunchCoordEnvelope(gossip.Envelope{
				Type:           gossip.TypeHolePunchCoord,
				RequestID:      env.RequestID,
				CoordStage:     "reply",
				CoordAt:        coordAt.UnixMilli(),
				RelaySource:    n.localPeerID(),
				RelayTarget:    env.RelaySource,
				AdvertisedAddr: replyAddr,
			}))
		}
		return
	}
	n.mu.RLock()
	targetPeer := n.peers[env.RelayTarget]
	targetInfo := n.knownPeers[env.RelayTarget]
	n.mu.RUnlock()
	if env.CoordStage == "offer" && targetInfo.addr != "" {
		n.sendEnvelope(peer, gossip.Envelope{
			Type:           gossip.TypeHolePunchCoord,
			RequestID:      env.RequestID,
			CoordStage:     "reply",
			CoordAt:        coordAt.UnixMilli(),
			RelaySource:    env.RelayTarget,
			RelayTarget:    env.RelaySource,
			AdvertisedAddr: targetInfo.addr,
		})
	}
	if targetPeer != nil {
		n.sendEnvelope(targetPeer, env)
	}
}
