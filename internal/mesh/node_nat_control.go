package mesh

import (
	"net"
	"time"

	"moss/internal/gossip"
)

func (n *Node) handleBindingRequest(peer *peerConn, env gossip.Envelope) {
	if env.RequestID == "" {
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
	reachable := probeTCPAddress(env.AdvertisedAddr, minDuration(500*time.Millisecond, n.config.HandshakeTimeout()))
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

func (n *Node) handleHolePunchCoord(peer *peerConn, env gossip.Envelope) {
	if env.RelaySource == "" || env.RelayTarget == "" || env.AdvertisedAddr == "" {
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
		if env.CoordStage == "offer" {
			replyAddr := n.freshObservedUDPAddr(peer.id, minDuration(750*time.Millisecond, n.config.HandshakeTimeout()/2))
			go n.tryHolePunchDialAt(env.RelaySource, env.AdvertisedAddr, coordAt)
			n.sendEnvelope(peer, gossip.Envelope{
				Type:             gossip.TypePeerAnnounce,
				AdvertisedPeerID: n.localPeerID(),
				AdvertisedAddr:   replyAddr,
			})
			n.sendEnvelope(peer, gossip.Envelope{
				Type:           gossip.TypeHolePunchCoord,
				RequestID:      env.RequestID,
				CoordStage:     "reply",
				CoordAt:        coordAt.UnixMilli(),
				RelaySource:    n.localPeerID(),
				RelayTarget:    env.RelaySource,
				AdvertisedAddr: replyAddr,
			})
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
