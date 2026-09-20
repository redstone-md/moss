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

// directedQueueDepth bounds one sender's in-flight directed payloads (relayed
// DMs and TypeDirect packets). Sized like the per-peer dispatch queue (256)
// rather than the per-channel pubsub queue (4096): a directed queue is already
// single-sender, so it absorbs callback jitter, not fan-out.
const directedQueueDepth = 256

// deliverDirected hands one directed payload (dispatchRelay or dispatchPacket)
// from the single dispatchLoop to the sender's own bounded queue instead of
// invoking the application callback inline. It is the directed twin of the
// per-channel localQueues split: the callback — a synchronous FFI call that may
// decrypt or write to disk — then runs in a per-sender worker, so one
// application parked on sender A no longer parks the only consumer and stops
// draining dispatchCh, which is what used to make UNINVOLVED senders' DMs hit
// the producers' non-blocking drop. The push here is non-blocking too: a sender
// whose own queue is full drops only its traffic, never another's. Queues are
// lazily created and bounded by MaxPeers, so a relay source spraying distinct
// keys cannot grow the map past the peer ceiling.
func (n *Node) deliverDirected(item any) {
	var key [32]byte
	var countDropped func()
	switch v := item.(type) {
	case dispatchRelay:
		// The relay path already authenticated the source: the payload opened
		// under the session's DM seal keyed to env.RelaySource, so this sender
		// is route-authenticated and safe to key on directly.
		key = v.sender
		countDropped = func() { n.countInbound("__relay_dispatch_dropped__") }
	case dispatchPacket:
		// queueKey is the authenticated session identity; the claimed sender is
		// unvalidated and must not grow the map. See dispatchPacket.
		key = v.queueKey
		countDropped = func() { n.countInbound("__packet_dispatch_dropped__") }
	default:
		return
	}
	n.directedMu.Lock()
	if n.directedQueues == nil {
		n.directedQueues = make(map[[32]byte]chan any)
	}
	queue, ok := n.directedQueues[key]
	if !ok {
		if len(n.directedQueues) >= n.config.MaxPeers {
			n.directedMu.Unlock()
			countDropped()
			return
		}
		queue = make(chan any, directedQueueDepth)
		n.directedQueues[key] = queue
		n.wg.Add(1)
		go n.directedWorker(key, queue)
	}
	select {
	case queue <- item:
		n.directedMu.Unlock()
	default:
		n.directedMu.Unlock()
		countDropped()
	}
}

// directedWorker drains one sender's directed payloads in order, invoking the
// unified packet callback when registered and the legacy relay callback
// otherwise — the same precedence the single dispatchLoop applied, now scoped
// to this sender so its slow consumer cannot touch any other's. Exits on
// rootCtx cancellation; deregisters its map entry on the way out so a
// reconnecting sender re-arms a fresh queue+worker.
func (n *Node) directedWorker(sender [32]byte, queue chan any) {
	defer n.wg.Done()
	defer func() {
		n.directedMu.Lock()
		if n.directedQueues[sender] == queue {
			delete(n.directedQueues, sender)
		}
		n.directedMu.Unlock()
	}()
	for {
		n.mu.RLock()
		root := n.rootCtx
		n.mu.RUnlock()
		var done <-chan struct{}
		if root != nil {
			done = root.Done()
		}
		select {
		case <-done:
			return
		case item := <-queue:
			n.deliverDirectedItem(item)
		}
	}
}

// deliverDirectedItem invokes the right application sink for one directed
// payload: the packet callback for both relayed and direct bytes when set, the
// legacy relay callback only for a relayed payload otherwise. Shared by the
// per-sender worker and the single dispatchLoop so the precedence stays in one
// place.
func (n *Node) deliverDirectedItem(item any) {
	switch v := item.(type) {
	case dispatchRelay:
		n.mu.RLock()
		cb := n.relayCB
		packet := n.packetCB
		n.mu.RUnlock()
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
			case dispatchRelay, dispatchPacket:
				// Shunt to the sender's worker rather than invoking the
				// callback here: the single dispatchLoop must never park on
				// one slow sender, or every other sender's DMs drop behind it
				// once dispatchCh fills. v is unused in this arm by design.
				n.deliverDirected(item)
			}
		}
	}
}

// publishSenderSignaturePayload is the byte string a publish envelope's
// Signature covers: the domain tag, the envelope type, the claimed sender,
// and the message's content — everything a spoofer would want to swap.
// SenderID is included so a signature made by one key cannot be replayed as
// another sender's proof: the verifier checks the signature against the
// sender the envelope claims, and the signer's key IS that sender.
func publishSenderSignaturePayload(env gossip.Envelope) []byte {
	payload := make([]byte, 0, 160)
	payload = append(payload, []byte("moss-publish-sender-v1")...)
	payload = append(payload, 0)
	payload = append(payload, []byte(string(env.Type))...)
	payload = append(payload, 0)
	payload = append(payload, env.SenderID...)
	payload = append(payload, 0)
	payload = append(payload, []byte(env.Channel)...)
	payload = append(payload, 0)
	payload = append(payload, []byte(env.MessageID)...)
	payload = append(payload, 0)
	payload = append(payload, env.Payload...)
	return payload
}

func (n *Node) signPublishEnvelope(env gossip.Envelope) gossip.Envelope {
	env.Signature = n.identity.Sign(publishSenderSignaturePayload(env))
	return env
}

func (n *Node) makePublishEnvelope(channel string, data []byte) gossip.Envelope {
	seq := atomic.AddUint64(&n.seq, 1)
	sender := n.identity.PublicKeyBytes()
	hash, _ := blake2s.New256(nil)
	hash.Write(sender)
	hash.Write([]byte(channel))
	hash.Write(data)
	hash.Write([]byte(strconv.FormatUint(seq, 10)))
	env := gossip.Envelope{
		Type:      gossip.TypePublish,
		Channel:   channel,
		MessageID: hex.EncodeToString(hash.Sum(nil)),
		Sequence:  seq,
		SenderID:  sender,
		Payload:   append([]byte(nil), data...),
	}
	return n.signPublishEnvelope(env)
}

func (n *Node) supernodeReady(profile nat.Profile) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.supernodeReadyLocked(profile)
}

// supernodeReadyLocked is supernodeReady for callers already holding n.mu.
// refreshSupernodeStatus needs it because its verdict and its state
// transition must commit inside one lock hold — see there.
func (n *Node) supernodeReadyLocked(profile nat.Profile) bool {
	overloaded := time.Now().Before(n.overloadedUntil)
	active := n.supernodeActive
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

// refreshSupernodeStatus re-evaluates this node's supernode role and, when the
// verdict changed, flips supernodeActive, announces the new state to every
// peer, and emits the matching event.
//
// The verdict and the transition must commit inside ONE lock hold: profile,
// session count, and supernodeActive are a single consistent read. Computing
// the verdict first and locking only to flip the state left a window where a
// refresher that had snapshotted an OLDER profile (a maintenance tick
// preempted between its snapshot and its lock) committed its stale verdict
// AFTER a fresher refresher had already flipped the state — the state flipped
// back, the next tick flipped it forward again, and peers received
// back-to-back duplicate announcements and duplicate
// EventSupernodePromoted within a heartbeat or two. Events are advisory and
// drop-tolerant, but every spurious flip also signs and broadcasts a
// SupernodeAnnounce/Revoke to every peer, so the transition itself must be
// atomic, not just the flip of the boolean.
func (n *Node) refreshSupernodeStatus() {
	n.mu.Lock()
	profile := n.natProfile.Load().(nat.Profile)
	ready := n.supernodeReadyLocked(profile)
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
			err := n.connectBootstrapSeed(attemptCtx, seed)
			// A seed that produced no session either failed its handshake or
			// was refused outright; both are a failed attempt as far as the
			// budget is concerned. A session at that addr is the only success.
			n.noteBootstrapDialOutcome(seed, err == nil && n.hasPeerAddr(seed))
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
			delete(n.bootstrapDialFailures, addr)
			continue
		}
		if addr == localAddr || hasPeerAddrLocked(n.peers, addr) {
			continue
		}
		// A seed that keeps failing gets an interval that GROWS: the flat
		// HandshakeTimeout cooldown retried the same dead port every five
		// seconds for as long as the trackers kept returning it (measured:
		// thirty attempts against one port in 140 seconds).
		if n.bootstrapAddrInBackoffLocked(addr, cooldown, now) {
			continue
		}
		// The host budget: one dead machine holding many port records must
		// not monopolise the seed dial slots either.
		if n.hostInBackoffLocked(dialHost(addr), cooldown, now) {
			continue
		}
		targets = append(targets, addr)
	}

	sort.Strings(targets)
	// One host, one seed per pass: the sorted list keeps the first addr of
	// each host, so a 41-port litter takes a single slot.
	targets = firstAddrPerHost(targets)
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
		n.hostDials[dialHost(addr)] = now
	}
	return selected
}

// firstAddrPerHost keeps the first address of each host in an ordered list,
// so one machine takes at most one dial slot per pass however many port
// records the trackers returned for it.
func firstAddrPerHost(ordered []string) []string {
	if len(ordered) <= 1 {
		return ordered
	}
	seen := make(map[string]struct{}, len(ordered))
	kept := ordered[:0]
	for _, addr := range ordered {
		host := dialHost(addr)
		if _, ok := seen[host]; ok {
			continue
		}
		seen[host] = struct{}{}
		kept = append(kept, addr)
	}
	return kept
}

// hasPeerAddr reports whether a live session sits at addr — the dial path's
// own definition of a success.
func (n *Node) hasPeerAddr(addr string) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return hasPeerAddrLocked(n.peers, addr)
}

func hasPeerAddrLocked(peers map[string]*peerConn, addr string) bool {
	for _, peer := range peers {
		if peer.addr == addr {
			return true
		}
	}
	return false
}
