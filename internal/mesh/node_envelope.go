package mesh

import (
	mcrypto "github.com/redstone-md/moss/internal/crypto"
	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/inspect"
)

// A peer may only cost us so much announcement traffic.
//
// announceRatePerSecond is far above anything a correct peer needs: a node
// re-announces itself about every 10s and forwards a peer's state at most once
// per 10s, so even a large mesh stays orders of magnitude below this. It is a
// ceiling on damage, not a schedule.
const (
	announceRatePerSecond = 20
	announceBurst         = 60
)

// isAnnounceType reports whether an envelope is announcement traffic — the kind
// that is redundant by design, so discarding a surplus one costs nothing.
func isAnnounceType(t gossip.EnvelopeType) bool {
	switch t {
	case gossip.TypePeerAnnounce, gossip.TypeSupernodeAnnounce, gossip.TypeSupernodeRevoke:
		return true
	}
	return false
}

// isChannelBearingControlType reports whether an envelope carries a channel
// claim we would record in a peer-keyed map (subscriptions, mesh membership).
// Publishing validates its own channel (it must be one we can address), and
// data-plane envelope handlers ignore unknown channels, so this is the set
// whose malformed channel would otherwise become persistent map state.
func isChannelBearingControlType(t gossip.EnvelopeType) bool {
	switch t {
	case gossip.TypeGraft, gossip.TypePrune, gossip.TypeIHave, gossip.TypeIWant, gossip.TypeIDontWant:
		return true
	}
	return false
}

func (n *Node) handleEnvelope(peer *peerConn, env gossip.Envelope) {
	if peer != nil && n.isPeerGraylisted(peer.id) {
		return
	}
	// Charge announcements against the peer's budget BEFORE doing any work on
	// them, and drop the surplus.
	//
	// Handling one costs an Ed25519 verification and the node's central lock.
	// readPeer dispatches synchronously, so at ~900 announcements a second — what
	// the fleet actually sent — the read loop cannot keep up, the 256-packet
	// stream buffer fills, and every packet behind it is discarded without a
	// trace. The pings among them are why sessions die at six misses with a
	// healthy connection.
	//
	// Not re-telling unvouched announcements stops US from feeding that flood.
	// This is the other half: a node must survive a peer that floods it whatever
	// the reason — an old build, a broken client, or malice — rather than depend
	// on every peer being well-behaved. An announcement is redundant by design, so
	// dropping a surplus one costs nothing; being unable to read is what costs.
	if peer != nil && isAnnounceType(env.Type) && peer.announceBudget != nil && !peer.announceBudget.Allow(1) {
		n.countInbound("__announce_throttled__")
		return
	}
	// Control traffic that claims a channel must actually carry one: an empty
	// or oversized channel cannot be subscribed to, grafted into, or pruned
	// from, and recording it anyway grows the peer-subscription and mesh maps
	// under keys no valid subscriber can ever produce.
	if isChannelBearingControlType(env.Type) && !validChannel(env.Channel) {
		n.countInbound("__malformed_channel__")
		return
	}
	n.emitInbound(peer, env)
	switch env.Type {
	case gossip.TypeOverlayFindNode:
		n.handleOverlayFindNode(peer, env)
	case gossip.TypeOverlayFindValue:
		n.handleOverlayFindValue(peer, env)
	case gossip.TypeOverlayStore:
		n.handleOverlayStore(peer, env)
	case gossip.TypeOverlayNodes, gossip.TypeOverlayValues:
		n.handleOverlayResponse(env)
	case gossip.TypeGraft:
		if peer == nil {
			return
		}
		// A first claim admits a late subscriber on this topic. It must not
		// clear another topic's refusal or the peer-wide health exclusion.
		n.handleTopicGraft(peer, env.Channel)
	case gossip.TypePrune:
		if peer == nil {
			return
		}
		// A recent speculative GRAFT is not evidence that the peer subscribes.
		// Retain the normal bound instead of shortening every refusal to 2s.
		n.handleTopicPrune(peer, env.Channel)
	case gossip.TypeIHave:
		n.handleIHave(peer, env)
	case gossip.TypeIWant:
		n.handleIWant(peer, env)
	case gossip.TypeIDontWant:
		if peer == nil || !n.canGossipWithPeer(peer.id) {
			return
		}
		n.rememberSuppression(peer.id, env.MessageIDs, env.MessageID)
	case gossip.TypePeerAnnounce:
		n.handlePeerAnnounce(peer, env)
	case gossip.TypeSupernodeAnnounce:
		n.handleSupernodeStatus(peer, env, true)
	case gossip.TypeSupernodeRevoke:
		n.handleSupernodeStatus(peer, env, false)
	case gossip.TypeBindingRequest:
		n.handleBindingRequest(peer, env)
	case gossip.TypeBindingResponse:
		n.handleBindingResponse(env)
	case gossip.TypeReachabilityRequest:
		n.handleReachabilityRequest(peer, env)
	case gossip.TypeReachabilityResponse:
		n.handleReachabilityResponse(env)
	case gossip.TypeHolePunchCoord:
		n.handleHolePunchCoord(peer, env)
	case gossip.TypeRelayRequest:
		n.handleRelayRequest(peer, env)
	case gossip.TypeRelayAccept:
		n.handleRelayAccept(peer, env)
	case gossip.TypeRelayData:
		n.handleRelayData(peer, env)
	case gossip.TypeRelayClose:
		n.handleRelayClose(peer, env)
	case gossip.TypePublish:
		if peer == nil {
			return
		}
		if n.isPeerBelowPublishThreshold(peer.id) {
			return
		}
		if env.Channel == "" || env.MessageID == "" {
			n.scoring.PenalizeInvalid(peer.id)
			n.emitPenalty(peer.id, "publish without channel or message id")
			return
		}
		if len(env.Payload) > n.config.Security.MaxMessageSizeBytes {
			n.scoring.PenalizeInvalid(peer.id)
			n.emitPenalty(peer.id, "publish over the message size limit")
			return
		}
		// Verify-on-present sender authentication. A publish's Signature
		// proves the envelope was authored by the key named in SenderID;
		// without it a peer can publish under anyone's SenderID. Legacy
		// senders never set Signature — that stays accepted, only counted
		// — so old clients interoperate untouched.
		if len(env.Signature) == 0 {
			n.countInbound("__sender_unsigned__")
		} else if !mcrypto.Verify(env.SenderID, publishSenderSignaturePayload(env), env.Signature) {
			n.countInbound("__sender_signature_bad__")
			n.scoring.PenalizeInvalid(peer.id)
			n.emitPenalty(peer.id, "publish sender signature invalid")
			return
		}
		n.observeMeshDelivery(env.Channel, env.MessageID, peer.id)
		// Append this node's hop before the store so the cached, replayable
		// copy carries the full path: a peer that recovers the message via
		// IWANT must see the serving node recorded, exactly as a peer that
		// receives it forwarded. Untagged publishes pay two nil-checks.
		n.appendTraceHop(&env)
		if !n.cache.StoreIfNew(env) {
			// Already seen: the message reached us by a second path. Not an
			// error, but the reason a peer looks silent when it is in fact
			// always second.
			n.emitDrop(inspect.KindDedup, peer, env, "already seen this message")
			return
		}
		n.deliverLocal(env)
		if n.waitingRequested() {
			return
		}
		if env.TraceID != "" {
			n.debugBus.Emit(func() inspect.Event {
				return inspect.Event{
					Kind:   inspect.KindTrace,
					Trace:  env.TraceID,
					Topic:  env.Channel,
					Fields: map[string]any{"hops": append([]string(nil), env.TraceHops...), "message_id": env.MessageID},
				}
			})
		}
		n.broadcastEnvelope(env, peer.id)
		n.broadcastIHave(env.Channel, []string{env.MessageID}, peer.id)
		if len(env.Payload) > 1024 {
			n.broadcastIDontWant(env.Channel, []string{env.MessageID}, peer.id)
		}
	case gossip.TypeStatDelta:
		if n.waitingRequested() {
			return
		}
		n.handleStatDelta(peer, env)
	case gossip.TypePing:
		n.sendEnvelope(peer, gossip.Envelope{Type: gossip.TypePong, RequestID: env.RequestID})
	case gossip.TypePong:
		n.handlePong(peer, env)
	case gossip.TypeDirect:
		n.handleDirectPacket(peer, env)
	}
}

// traceHopCap bounds the recorded path: a publish that already walked 16
// hops is not going anywhere an operator cares about, and an unbounded hop
// list would let the publisher's message size grow with the mesh diameter.
const traceHopCap = 16

// appendTraceHop records this node on a traced publish. A node past the cap
// stops appending and counts it — the message still forwards, the trace just
// stops growing. Untagged publishes pay two nil-checks and nothing else.
func (n *Node) appendTraceHop(env *gossip.Envelope) {
	if env.TraceID == "" {
		return
	}
	if len(env.TraceHops) >= traceHopCap {
		n.countInbound("__trace_hops_capped__")
		return
	}
	env.TraceHops = append(env.TraceHops, n.localPeerID())
}

func (n *Node) deliverLocal(env gossip.Envelope) {
	if !n.pubsub.IsLocalSubscriber(env.Channel) {
		return
	}
	// Which room this topic belongs to is not guessable from the wire — the
	// topic is an HMAC — so it comes from what this node subscribed under. A
	// topic with no subscription cannot be opened by any key we hold.
	sub, subscribed := n.subscriptionFor(env.Channel)
	if !subscribed {
		return
	}
	// Open the room seal; a payload we cannot authenticate (wrong room / PSK) is
	// dropped rather than handed up.
	plaintext, ok := n.openRoom(sub.room, env.Payload)
	if !ok {
		return
	}
	var sender [32]byte
	copy(sender[:], env.SenderID)
	n.enqueueLocal(dispatchMessage{
		// Hand the application its bare channel, not the opaque room topic.
		channel: sub.channel,
		sender:  sender,
		data:    plaintext,
	})
}

// localDeliveryQueueDepth is per channel. Deep enough to absorb a file
// transfer's chunks while the application decrypts and writes them, because the
// alternative is not "wait a moment" but "lose packets in the transport buffer
// and take the pings with them".
const localDeliveryQueueDepth = 4096

// enqueueLocal hands a message to its channel's delivery worker and never
// blocks the caller.
//
// The per-peer dispatch worker calls this from outside the socket read loop.
// Blocking here used to stop that loop, and a stopped read loop overflows the
// transport's inbound buffer, which discards whatever arrives next — including
// the pings a session dies without. A dropped message on one channel is a
// bounded, counted loss (`__local_delivery_dropped__`); a stalled reader is
// an unbounded, invisible one. The first is strictly better, so the send is
// non-blocking and the overflow is recorded. The send also sits INSIDE
// localMu: the queue's teardown (teardownLocalQueue, on Unsubscribe) closes
// it under the same mutex, so a send can never race a close.
func (n *Node) enqueueLocal(msg dispatchMessage) {
	n.localMu.Lock()
	queue, ok := n.localQueues[msg.channel]
	if !ok {
		queue = make(chan dispatchMessage, localDeliveryQueueDepth)
		if n.localQueues == nil {
			n.localQueues = make(map[string]chan dispatchMessage)
		}
		n.localQueues[msg.channel] = queue
		n.wg.Add(1)
		go n.localDeliveryWorker(msg.channel, queue)
	}
	select {
	case queue <- msg:
	default:
		n.countInbound("__local_delivery_dropped__")
	}
	n.localMu.Unlock()
}

// teardownLocalQueue closes and removes one channel's delivery queue. Called
// from UnsubscribeRoom once no live subscription to the channel remains.
// Closing lets the worker deliver what is already buffered, then exit; its
// deregistration (delete before wg.Done) no-ops on the removed entry, and
// Start()'s wholesale map reset is untouched — an Unsubscribe→Publish cycle
// simply spins a fresh lazy worker.
func (n *Node) teardownLocalQueue(channel string) {
	n.localMu.Lock()
	if queue, ok := n.localQueues[channel]; ok {
		delete(n.localQueues, channel)
		close(queue)
	}
	n.localMu.Unlock()
}

// localDeliveryWorker drains one channel's queue in order. One worker per
// channel: ordering is preserved where it is meaningful, and a slow transfer
// on one channel cannot delay control traffic on another. Exits on rootCtx's
// cancellation or when teardownLocalQueue closes the queue.
func (n *Node) localDeliveryWorker(channel string, queue chan dispatchMessage) {
	defer n.wg.Done()
	defer func() {
		n.localMu.Lock()
		if n.localQueues[channel] == queue {
			delete(n.localQueues, channel)
		}
		n.localMu.Unlock()
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
		case msg, ok := <-queue:
			if !ok {
				return
			}
			n.mu.RLock()
			cb := n.messageCB
			n.mu.RUnlock()
			if cb != nil {
				cb(msg.channel, msg.sender, msg.data)
			}
		}
	}
}

func (n *Node) broadcastEnvelope(env gossip.Envelope, excludePeerID string) bool {
	targets := n.pubsub.MeshPeers(env.Channel)
	if len(targets) == 0 {
		// An empty mesh for a topic is the single most common reason a message
		// goes nowhere, and it is invisible in a delivery counter.
		n.emitDrop(inspect.KindForward, nil, env, "no peers in the topic mesh")
		return false
	}
	eligible := filterPeerIDs(targets, func(peerID string) bool {
		return peerID != excludePeerID && n.canGossipWithPeer(peerID)
	})
	n.emitForward(env, len(eligible), len(targets))
	return n.sendToPeers(eligible, env)
}

func (n *Node) broadcastFloodPublish(env gossip.Envelope, excludePeerID string) bool {
	meshPeers := n.pubsub.MeshPeers(env.Channel)
	nonMeshSubscribers := n.pubsub.NonMeshSubscribers(env.Channel)
	targets := make([]string, 0, len(meshPeers)+len(nonMeshSubscribers))
	seen := make(map[string]struct{}, len(meshPeers)+len(nonMeshSubscribers))
	for _, peerID := range append(meshPeers, nonMeshSubscribers...) {
		if peerID == excludePeerID || !n.canGossipWithPeer(peerID) {
			continue
		}
		if _, ok := seen[peerID]; ok {
			continue
		}
		seen[peerID] = struct{}{}
		targets = append(targets, peerID)
	}
	if len(targets) == 0 {
		// Flood publish is the path Publish() itself takes, so this is where a
		// message from THIS node dies when it has nobody to give it to.
		n.emitDrop(inspect.KindForward, nil, env, "no subscribers and no mesh peers")
		return false
	}
	n.emitForward(env, len(targets), len(meshPeers)+len(nonMeshSubscribers))
	return n.sendToPeers(targets, env)
}

func filterPeerIDs(peerIDs []string, keep func(string) bool) []string {
	filtered := make([]string, 0, len(peerIDs))
	for _, peerID := range peerIDs {
		if keep(peerID) {
			filtered = append(filtered, peerID)
		}
	}
	return filtered
}
