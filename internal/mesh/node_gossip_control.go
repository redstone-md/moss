package mesh

import (
	"sync"
	"sync/atomic"

	"github.com/redstone-md/moss/internal/gossip"
)

func (n *Node) sendRecentIHave(peer *peerConn, channel string) {
	if peer == nil || !n.canGossipWithPeer(peer.id) {
		return
	}
	ids := n.cache.RecentIDs(channel, n.config.GossipSub.DLazy)
	if len(ids) == 0 {
		return
	}
	n.sendEnvelope(peer, gossip.Envelope{
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
	n.sendEnvelope(peer, gossip.Envelope{
		Type:       gossip.TypeIWant,
		Channel:    env.Channel,
		MessageIDs: missing,
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
		n.sendEnvelope(peer, cached)
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

func (n *Node) broadcastToNonMesh(channel string, env gossip.Envelope, excludePeerID string) bool {
	targets := n.pubsub.NonMeshSubscribers(channel)
	n.mu.RLock()
	peers := make([]*peerConn, 0, len(targets))
	for _, peerID := range targets {
		if peerID == excludePeerID {
			continue
		}
		peer := n.peers[peerID]
		if peer == nil {
			continue
		}
		peers = append(peers, peer)
	}
	n.mu.RUnlock()
	sent := false
	for _, peer := range peers {
		if n.sendEnvelope(peer, env) {
			sent = true
		}
	}
	return sent
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
		peer := n.peers[peerID]
		if peer == nil {
			continue
		}
		peers = append(peers, peer)
	}
	n.mu.RUnlock()
	if len(peers) == 0 {
		return false
	}
	workerCount := min(len(peers), sendToPeersConcurrency)
	jobs := make(chan *peerConn, len(peers))
	var sent atomic.Bool
	var wg sync.WaitGroup
	wg.Add(workerCount)
	for range workerCount {
		go func() {
			defer wg.Done()
			for peer := range jobs {
				if n.sendEnvelope(peer, env) {
					sent.Store(true)
				}
			}
		}()
	}
	for _, peer := range peers {
		jobs <- peer
	}
	close(jobs)
	wg.Wait()
	return sent.Load()
}
