package mesh

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
)

// outboundQueueDepth bounds how many envelopes may sit pending for one peer.
// Every send on a running node goes through a per-peer bounded queue drained
// by a dedicated worker, so a peer whose session stalled — its WritePacket
// parked in the transport's write timeout — can only cost its own queue,
// never the read loop, the maintenance pass, or the publish call that
// happened to target it.
const outboundQueueDepth = 256

// outboundEnvelope is one queued send: the envelope plus its marshaled wire
// bytes. Carrying the wire form through the queue is what makes a broadcast
// O(1) marshals instead of O(N): sendToPeers marshals once and hands every
// peer's queue the same buffer; the worker hands it to sendEnvelopeWire
// untouched. An empty wire means the enqueueing path had no pre-marshaled
// form (compat enqueue), and the worker marshals once at send time.
type outboundEnvelope struct {
	env  gossip.Envelope
	wire []byte
}

// One IHAVE must not fan out an unbounded burst of IWANTs. Gossip re-sends
// the same message list every heartbeat, so without a per-peer cooldown one
// missing id becomes an IWANT per heartbeat per peer, and a peer that
// re-announces its whole cache turns into an IWANT flood in the other
// direction. Ask at most once per cooldown, and cap what one IHAVE may
// trigger.
const (
	iwantAskCooldown        = 10 * time.Second
	maxIWantAsksPerResponse = 64
)

// Serve-side mirror of the ask-side caps above: one IWANT per id per peer per
// cooldown, and a hard ceiling on how many payloads one request may pull. The
// ask side is already capped (64 fresh asks / 10s per peer), but that assumes
// the ASKING node runs this code — an old build, a broken client, or a
// malicious peer can send IWANTs at line rate with 256 ids each, and every id
// that hits the cache replays a full envelope (up to the 64KB frame cap).
// Dedup + budget bound what one peer can extract to the same shape it may ask.
const (
	iwantServeCooldown   = 10 * time.Second
	maxIWantServesPerReq = 64
)

func (n *Node) sendRecentIHave(peer *peerConn, channel string) {
	if peer == nil || !n.canGossipWithPeer(peer.id) {
		return
	}
	ids := n.cache.RecentIDs(channel, n.config.GossipSub.DLazy)
	if len(ids) == 0 {
		return
	}
	n.sendOrEnqueue(peer, gossip.Envelope{
		Type:       gossip.TypeIHave,
		Channel:    channel,
		MessageIDs: ids,
	})
}

func (n *Node) handleIHave(peer *peerConn, env gossip.Envelope) {
	if peer == nil || !n.canGossipWithPeer(peer.id) {
		return
	}
	if env.Channel == "" || len(env.MessageIDs) == 0 || !n.pubsub.IsLocalSubscriber(env.Channel) {
		return
	}
	ids := env.MessageIDs
	if len(ids) > maxInboundControlMessageIDs {
		ids = ids[:maxInboundControlMessageIDs]
	}
	missing := make([]string, 0, len(ids))
	for _, id := range ids {
		if !n.cache.Seen(id) {
			missing = append(missing, id)
		}
	}
	if len(missing) == 0 {
		return
	}
	// Ask-side dedup: record each fresh ask under the node lock so a repeat
	// IHAVE from the same peer inside the cooldown costs a map lookup, not
	// another IWANT — and cap what a single envelope may trigger.
	now := time.Now()
	n.mu.Lock()
	n.sweepStalePeerStateLocked(now)
	if n.iwantAsks == nil {
		n.iwantAsks = make(map[string]map[string]time.Time)
	}
	asks := n.iwantAsks[peer.id]
	if asks == nil {
		asks = make(map[string]time.Time)
		n.iwantAsks[peer.id] = asks
	}
	fresh := make([]string, 0, len(missing))
	for _, id := range missing {
		if last, ok := asks[id]; ok && now.Sub(last) < iwantAskCooldown {
			continue
		}
		if _, recorded := asks[id]; !recorded && len(asks) >= maxSuppressionEntriesPerPeer {
			continue
		}
		asks[id] = now
		fresh = append(fresh, id)
		if len(fresh) >= maxIWantAsksPerResponse {
			break
		}
	}
	n.mu.Unlock()
	if len(fresh) == 0 {
		return
	}
	n.sendOrEnqueue(peer, gossip.Envelope{
		Type:       gossip.TypeIWant,
		Channel:    env.Channel,
		MessageIDs: fresh,
	})
}

func (n *Node) handleIWant(peer *peerConn, env gossip.Envelope) {
	if peer == nil || !n.canGossipWithPeer(peer.id) {
		return
	}
	ids := env.MessageIDs
	if len(ids) > maxInboundControlMessageIDs {
		ids = ids[:maxInboundControlMessageIDs]
	}
	now := time.Now()
	n.mu.Lock()
	n.sweepStalePeerStateLocked(now)
	if n.iwantServes == nil {
		n.iwantServes = make(map[string]map[string]time.Time)
	}
	served := n.iwantServes[peer.id]
	if served == nil {
		served = make(map[string]time.Time)
		n.iwantServes[peer.id] = served
	}
	fresh := make([]string, 0, len(ids))
	throttled := 0
	for _, id := range ids {
		if id == "" {
			continue
		}
		if last, ok := served[id]; ok && now.Sub(last) < iwantServeCooldown {
			throttled++
			continue
		}
		if _, recorded := served[id]; !recorded && len(served) >= maxSuppressionEntriesPerPeer {
			throttled++
			continue
		}
		served[id] = now
		fresh = append(fresh, id)
		if len(fresh) >= maxIWantServesPerReq {
			throttled += len(ids) - len(fresh)
			break
		}
	}
	n.mu.Unlock()
	if len(fresh) == 0 {
		if throttled > 0 {
			n.countInbound("__iwant_throttled__")
		}
		return
	}
	for _, id := range fresh {
		if n.isSuppressed(peer.id, id) {
			continue
		}
		cached, ok := n.cache.Get(id)
		if !ok {
			// The id expired or was never held: ordinary gossip churn, not a
			// drop — the asker's IHAVE ran ahead of the message, and asking
			// again later is the protocol working.
			continue
		}
		n.sendOrEnqueue(peer, cached)
	}
}

func (n *Node) broadcastIHave(channel string, ids []string, excludePeerID string) {
	if channel == "" || len(ids) == 0 {
		return
	}
	targets := n.selectLazyPeers(channel, excludePeerID, n.config.GossipSub.DLazy)
	n.sendToPeers(targets, gossip.Envelope{
		Type:       gossip.TypeIHave,
		Channel:    channel,
		MessageIDs: ids,
	})
}

func (n *Node) broadcastIDontWant(channel string, ids []string, excludePeerID string) {
	if channel == "" || len(ids) == 0 {
		return
	}
	n.sendToPeers(n.meshGossipPeers(channel, excludePeerID), gossip.Envelope{
		Type:       gossip.TypeIDontWant,
		Channel:    channel,
		MessageIDs: ids,
	})
}

func (n *Node) broadcastToAll(env gossip.Envelope, excludePeerID string) bool {
	n.mu.RLock()
	peerIDs := make([]string, 0, len(n.peers))
	for peerID, peer := range n.peers {
		if peer == nil || peerID == excludePeerID {
			continue
		}
		peerIDs = append(peerIDs, peerID)
	}
	n.mu.RUnlock()
	targets := filterPeerIDs(peerIDs, n.canSharePeerExchangeWithPeer)
	return n.sendToPeers(targets, env)
}

func (n *Node) sendToPeers(peerIDs []string, env gossip.Envelope) bool {
	if len(peerIDs) == 0 {
		return false
	}
	peerIDs = filterPeerIDs(peerIDs, func(peerID string) bool {
		return !n.isPeerGraylisted(peerID)
	})
	if len(peerIDs) == 0 {
		return false
	}
	n.mu.RLock()
	peers := make([]*peerConn, 0, len(peerIDs))
	for _, peerID := range peerIDs {
		if peer := n.peers[peerID]; peer != nil {
			peers = append(peers, peer)
		}
	}
	n.mu.RUnlock()
	if len(peers) == 0 {
		return false
	}
	// Marshal once for the whole fan-out: every direct peer's queue receives
	// the same wire buffer, and the worker hands it to sendEnvelopeWire
	// instead of re-marshaling per peer. Relayed peers re-seal the envelope
	// themselves, so they are unaffected by the shared form. A marshal
	// failure can only come from an unmarshalable envelope, which the old
	// per-peer path would have dropped peer by peer anyway — one drop for
	// all, same observable outcome.
	wire, err := json.Marshal(env)
	if err != nil {
		return false
	}
	sent := false
	for _, peer := range peers {
		if n.sendOrEnqueueWire(peer, env, wire) {
			sent = true
		}
	}
	return sent
}

// sendOrEnqueue is the send point for everything this node tells a peer: on a
// running node it hands the envelope to the peer's outbound queue and returns
// immediately; on a node that never started (unit tests drive sends directly)
// or one that is stopping, it falls back to the synchronous send so the
// observable behavior of an unstarted node is unchanged.
func (n *Node) sendOrEnqueue(peer *peerConn, env gossip.Envelope) bool {
	return n.sendOrEnqueueWire(peer, env, nil)
}

// sendOrEnqueueWire is sendOrEnqueue for callers that already hold the
// envelope's marshaled wire bytes; the queue carries them to the worker so
// the fan-out that produced them marshals once, not once per peer. A nil
// wire marshals at send time — the worker's fallback for the sync path and
// for compat enqueues.
func (n *Node) sendOrEnqueueWire(peer *peerConn, env gossip.Envelope, wire []byte) bool {
	if peer == nil {
		return false
	}
	// One RLock section covers the started-check AND the worker spawn. Stop
	// flips `started` and swaps the peer table under the WRITE lock before it
	// cancels and waits, so a worker registered here is n.wg-counted before
	// wg.Wait can run — wg.Add can never race Stop's Wait — and a node that
	// already stopped takes the synchronous path instead. That path releases
	// the RLock first: a relayed peer's sendEnvelope re-enters n.mu, and
	// Go's RWMutex does not admit a writer that is already holding a read.
	n.mu.RLock()
	if !n.started || n.rootCtx == nil || n.rootCtx.Err() != nil {
		n.mu.RUnlock()
		return n.sendEnvelopeWire(peer, env, wire)
	}
	queued := n.enqueueOutboundWire(n.rootCtx, peer, outboundEnvelope{env: env, wire: wire})
	n.mu.RUnlock()
	return queued
}

// enqueueOutbound hands one envelope to a peer's queue, lazily starting its
// worker on first use. Non-blocking by design: gossip traffic is redundant and
// re-announced on later heartbeats, so an overfull queue drops — silently
// costing one redundant copy — where the old code blocked the caller
// network-wide. The drop counter only grows, keeping the pressure visible.
//
// The send happens INSIDE the outboundMu critical section: the queue's
// lifecycle (teardownOutboundQueue, sweep of orphans in the maintenance
// loop) closes it under the same mutex, so a send can never race a close.
func (n *Node) enqueueOutbound(ctx context.Context, peer *peerConn, env gossip.Envelope) bool {
	return n.enqueueOutboundWire(ctx, peer, outboundEnvelope{env: env})
}

// enqueueOutboundWire is enqueueOutbound carrying the envelope's marshaled
// wire form when the caller already produced it; a nil wire defers the
// marshal to the worker's send.
func (n *Node) enqueueOutboundWire(ctx context.Context, peer *peerConn, e outboundEnvelope) bool {
	n.outboundMu.Lock()
	defer n.outboundMu.Unlock()
	if n.outboundQueues == nil {
		n.outboundQueues = make(map[string]chan outboundEnvelope)
	}
	queue, ok := n.outboundQueues[peer.id]
	if !ok {
		queue = make(chan outboundEnvelope, outboundQueueDepth)
		n.outboundQueues[peer.id] = queue
		n.wg.Add(1)
		go n.outboundWorker(ctx, peer.id, queue)
	}
	select {
	case queue <- e:
		return true
	default:
		n.outboundDropped.Add(1)
		return false
	}
}

// teardownOutboundQueue closes and removes a peer's outbound queue. Called
// from removePeer AFTER dropping n.mu — never under it — and from the
// maintenance sweep for queues whose peer left via the eviction paths that
// bypass removePeer. Closing makes the worker deliver what is already
// buffered, then exit; its own deregistration no-ops on the deleted entry.
// A peer that returns gets a fresh lazy queue, and a peer that was REPLACED
// keeps its live queue — removePeer no-ops on a session mismatch, so this
// only runs for the peer that actually left.
func (n *Node) teardownOutboundQueue(peerID string) {
	n.outboundMu.Lock()
	if queue, ok := n.outboundQueues[peerID]; ok {
		delete(n.outboundQueues, peerID)
		close(queue)
	}
	n.outboundMu.Unlock()
}

// sweepOrphanOutboundQueues reclaims queues whose peer is gone from n.peers
// — the eviction and relay-removal paths delete peers without removePeer.
// Runs on the maintenance loop's conn-tick; lock order is outboundMu →
// n.mu.RLock, which nothing nests the other way around (enqueueOutbound
// never holds n.mu).
func (n *Node) sweepOrphanOutboundQueues() {
	n.outboundMu.Lock()
	for peerID, queue := range n.outboundQueues {
		n.mu.RLock()
		_, connected := n.peers[peerID]
		n.mu.RUnlock()
		if connected {
			continue
		}
		delete(n.outboundQueues, peerID)
		close(queue)
	}
	n.outboundMu.Unlock()
}

// outboundWorker drains one peer's queue until the node stops or the queue is
// torn down. The peer is looked up at SEND time rather than captured at
// enqueue time: a peer that vanished, or was replaced by a redial between
// enqueue and send, must not receive the envelope — but its replacement may.
// Deregistration runs before wg.Done so a Stop→Start cycle can never find a
// queue whose worker is already gone.
func (n *Node) outboundWorker(ctx context.Context, peerID string, queue chan outboundEnvelope) {
	defer n.wg.Done()
	defer func() {
		n.outboundMu.Lock()
		if n.outboundQueues[peerID] == queue {
			delete(n.outboundQueues, peerID)
		}
		n.outboundMu.Unlock()
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case e, ok := <-queue:
			if !ok {
				// Queue torn down with its peer's disconnect: exit
				// promptly; Stop's wg.Wait is watching.
				return
			}
			n.mu.RLock()
			peer := n.peers[peerID]
			n.mu.RUnlock()
			if peer == nil {
				continue
			}
			// The wire form rides the queue from the fan-out that produced
			// it; a nil wire (compat enqueue) marshals once, here.
			n.sendEnvelopeWire(peer, e.env, e.wire)
		}
	}
}
