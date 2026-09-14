package mesh

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strconv"
	"sync/atomic"
	"time"

	"golang.org/x/crypto/blake2s"

	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/nat"
)

func (n *Node) dispatchLoop(ctx context.Context) {
	defer n.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case item := <-n.dispatchCh:
			switch v := item.(type) {
			case dispatchMessage:
				n.mu.RLock()
				cb := n.messageCB
				n.mu.RUnlock()
				if cb != nil {
					cb(v.channel, v.sender, v.data)
				}
			case dispatchEvent:
				n.mu.RLock()
				cb := n.eventCB
				n.mu.RUnlock()
				if cb != nil {
					cb(v.eventType, v.detail)
				}
			case dispatchRelay:
				n.mu.RLock()
				cb := n.relayCB
				packet := n.packetCB
				n.mu.RUnlock()
				// The unified packet sink takes precedence when
				// registered: an application that asked for one
				// directed-payload stream must not ALSO see the same
				// bytes twice through the legacy relay callback.
				if packet != nil {
					packet(v.sender, v.data)
				} else if cb != nil {
					cb(v.sender, v.data)
				}
			case dispatchPacket:
				n.mu.RLock()
				packet := n.packetCB
				n.mu.RUnlock()
				if packet != nil {
					packet(v.sender, v.data)
				}
			}
		}
	}
}

func (n *Node) makePublishEnvelope(channel string, data []byte) gossip.Envelope {
	seq := atomic.AddUint64(&n.seq, 1)
	sender := n.identity.PublicKeyBytes()
	hash, _ := blake2s.New256(nil)
	hash.Write(sender)
	hash.Write([]byte(channel))
	hash.Write(data)
	hash.Write([]byte(strconv.FormatUint(seq, 10)))
	return gossip.Envelope{
		Type:      gossip.TypePublish,
		Channel:   channel,
		MessageID: hex.EncodeToString(hash.Sum(nil)),
		Sequence:  seq,
		SenderID:  sender,
		Payload:   append([]byte(nil), data...),
	}
}

func (n *Node) supernodeReady(profile nat.Profile) bool {
	n.mu.RLock()
	overloaded := time.Now().Before(n.overloadedUntil)
	active := n.supernodeActive
	n.mu.RUnlock()
	if overloaded {
		return false
	}
	// Asymmetric deadband against session-count flapping. Sessions hover
	// around RelayMaxSessions in production; at the boundary one closing
	// while another opens flipped supernodeReady on every maintenance
	// heartbeat, and each flip broadcast a signed SupernodeAnnounce or
	// SupernodeRevoke to every peer — a revoke storm at exactly full
	// capacity. An active supernode demotes only at the hard cap; a demoted
	// one re-promotes only below the cap minus a margin, so traffic between
	// the two thresholds holds the current state. Margin is 10% of the cap;
	// caps of 10 or fewer (tests use 1) keep the exact old boundary.
	if max := n.config.NAT.RelayMaxSessions; max > 0 {
		sessions := n.relaySessions.Count()
		if active {
			if sessions >= max {
				return false
			}
		} else if sessions >= max-max/10 {
			return false
		}
	}
	switch profile.Type {
	case nat.TypePublic, nat.TypeFullCone:
	default:
		return false
	}
	return nat.ShouldPromote(profile, time.Since(n.startedAt), n.config.NAT.RelayMaxBandwidthKBPS, 1.0, nat.PromotionPolicy{
		MinUptime:          time.Duration(n.config.NAT.SuperNodeMinUptimeSec) * time.Second,
		MinBandwidthKBytes: n.config.NAT.RelayMaxBandwidthKBPS,
		MinScore:           1.0,
	})
}

func (n *Node) ChannelSubscribers(channel string) []string {
	if !validChannel(channel) {
		return nil
	}
	subscribers := n.pubsub.Subscribers(n.roomTopic(channel))
	sort.Strings(subscribers)
	return subscribers
}

func (n *Node) refreshSupernodeStatus() {
	profile := n.natProfile.Load().(nat.Profile)
	ready := n.supernodeReady(profile)

	n.mu.Lock()
	if n.supernodeActive == ready {
		n.mu.Unlock()
		return
	}
	n.supernodeActive = ready
	n.mu.Unlock()

	info := n.localKnownPeer()
	info.relayCapable = ready
	envType := gossip.TypeSupernodeRevoke
	eventType := int32(EventSupernodeRevoked)
	if ready {
		envType = gossip.TypeSupernodeAnnounce
		eventType = EventSupernodePromoted
	}

	signed := n.signSupernodeEnvelope(gossip.Envelope{
		Type:                   envType,
		AdvertisedPeerID:       info.id,
		AdvertisedAddr:         info.addr,
		AdvertisedNATType:      string(info.natType),
		AdvertisedReachable:    info.publicReachable,
		AdvertisedRelayCapable: ready,
	})
	n.broadcastToAll(signed, "")
	n.broadcastPeerAnnouncement(info, "")
	n.enqueueEvent(eventType, map[string]string{"nat_type": string(profile.Type)})
}

// reannounceSupernodeStatus re-broadcasts a signed SupernodeAnnounce to all peers
// while this node is an active SuperNode. refreshSupernodeStatus emits only on a
// status *change*, and the one-shot promotion broadcast reaches only the peers
// connected at that instant — a peer that joins later, or that was registered
// before this node's own probe-driven promotion, never gets a trusted, signed
// announcement (relay capability is never taken from a plain peer-announce). The
// periodic re-broadcast converges every current peer's view of our relay role.
func (n *Node) reannounceSupernodeStatus() {
	info := n.localKnownPeer()
	if !info.relayCapable {
		return
	}
	signed := n.signSupernodeEnvelope(gossip.Envelope{
		Type:                   gossip.TypeSupernodeAnnounce,
		AdvertisedPeerID:       info.id,
		AdvertisedAddr:         info.addr,
		AdvertisedNATType:      string(info.natType),
		AdvertisedReachable:    info.publicReachable,
		AdvertisedRelayCapable: true,
	})
	n.broadcastToAll(signed, "")
}

// enqueueEvent hands one event to the dispatch loop. The send is
// non-blocking: the dispatch queue is bounded (1024) and an event burst
// drops the surplus (counted, monotonic) rather than blocking the caller —
// a blocked caller here used to wedge Stop() when dispatchLoop had already
// exited on the cancelled context. Events are advisory; losing one under a
// flood is the better trade.
func (n *Node) enqueueEvent(eventType int32, detail any) {
	raw, _ := json.Marshal(detail)
	if eventType == EventTrackerFailure {
		n.forwardEventToAxiom(detail)
	}
	select {
	case n.dispatchCh <- dispatchEvent{eventType: eventType, detail: string(raw)}:
	default:
		n.countInbound("__dispatch_event_dropped__")
	}
}

func (n *Node) connectKnownPeers() {
	candidates := n.discoveredPeerTargets()
	for _, candidate := range candidates {
		go n.dialKnownPeer(candidate.peerID, candidate.addr)
	}
}

func (n *Node) connectBootstrapSeeds(ctx context.Context) {
	addrs := n.bootstrapSeedTargets()
	for _, addr := range addrs {
		go func(seed string) {
			attemptCtx, cancel := context.WithTimeout(ctx, n.config.HandshakeTimeout())
			defer cancel()
			_ = n.connectBootstrapSeed(attemptCtx, seed)
		}(addr)
	}
}

func (n *Node) bootstrapSeedTargets() []string {
	now := time.Now()
	cutoff := now.Add(-10 * time.Minute)
	cooldown := n.config.HandshakeTimeout()
	if cooldown < 2*time.Second {
		cooldown = 2 * time.Second
	}
	localAddr := n.advertisedListenAddr()

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.directPeerCountLocked() >= n.config.MaxPeers {
		return nil
	}

	targets := make([]string, 0, len(n.trackerSeeds))
	for addr, seenAt := range n.trackerSeeds {
		if addr == "" {
			continue
		}
		if seenAt.Before(cutoff) {
			delete(n.trackerSeeds, addr)
			delete(n.bootstrapDials, addr)
			continue
		}
		if addr == localAddr || hasPeerAddrLocked(n.peers, addr) {
			continue
		}
		lastDial := n.bootstrapDials[addr]
		if !lastDial.IsZero() && now.Sub(lastDial) < cooldown {
			continue
		}
		targets = append(targets, addr)
	}

	sort.Strings(targets)
	limit := n.config.GossipSub.DOut
	if limit <= 0 {
		limit = 2
	}
	if len(targets) < limit {
		limit = len(targets)
	}
	selected := append([]string(nil), targets[:limit]...)
	for _, addr := range selected {
		n.bootstrapDials[addr] = now
	}
	return selected
}

func hasPeerAddrLocked(peers map[string]*peerConn, addr string) bool {
	for _, peer := range peers {
		if peer.addr == addr {
			return true
		}
	}
	return false
}
