package mesh

import (
	"context"
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
	for _, id := range ids {
		if n.isSuppressed(peer.id, id) {
			continue
		}
		cached, ok := n.cache.Get(id)
		if !ok {
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
	sent := false
	for _, peer := range peers {
		if n.sendOrEnqueue(peer, env) {
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
		return n.sendEnvelope(peer, env)
	}
	queued := n.enqueueOutbound(n.rootCtx, peer, env)
	n.mu.RUnlock()
	return queued
}

// enqueueOutbound hands one envelope to a peer's queue, lazily starting its
// worker on first use. Non-blocking by design: gossip traffic is redundant and
// re-announced on later heartbeats, so an overfull queue drops — silently
// costing one redundant copy — where the old code blocked the caller
// network-wide. The drop counter only grows, keeping the pressure visible.
func (n *Node) enqueueOutbound(ctx context.Context, peer *peerConn, env gossip.Envelope) bool {
	n.outboundMu.Lock()
	if n.outboundQueues == nil {
		n.outboundQueues = make(map[string]chan gossip.Envelope)
	}
	queue, ok := n.outboundQueues[peer.id]
	if !ok {
		queue = make(chan gossip.Envelope, outboundQueueDepth)
		n.outboundQueues[peer.id] = queue
		n.wg.Add(1)
		go n.outboundWorker(ctx, peer.id, queue)
	}
	n.outboundMu.Unlock()
	select {
	case queue <- env:
		return true
	default:
		n.outboundDropped.Add(1)
		return false
	}
}

// outboundWorker drains one peer's queue until the node stops. The peer is
// looked up at SEND time rather than captured at enqueue time: a peer that
// vanished, or was replaced by a redial between enqueue and send, must not
// receive the envelope — but its replacement may. Deregistration runs before
// wg.Done so a Stop→Start cycle can never find a queue whose worker is
// already gone.
func (n *Node) outboundWorker(ctx context.Context, peerID string, queue chan gossip.Envelope) {
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
		case env := <-queue:
			n.mu.RLock()
			peer := n.peers[peerID]
			n.mu.RUnlock()
			if peer == nil {
				continue
			}
			n.sendEnvelope(peer, env)
		}
	}
}
