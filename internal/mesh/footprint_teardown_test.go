package mesh

import (
	"context"
	"testing"

	"github.com/redstone-md/moss/internal/gossip"
)

// An outbound queue lives only while its peer does: removePeer must close
// it (and let its worker exit) instead of leaking ~110KB per peer the node
// EVER connected until Stop.
func TestRemovePeerTearsDownOutboundQueue(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	node, err := NewNode("mesh-outbound-teardown", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	node.mu.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node.rootCtx = ctx
	node.started = true
	node.peers["gone"] = &peerConn{id: "gone", addr: "203.0.113.1:4000"}
	session := node.peers["gone"].session
	node.mu.Unlock()

	// Spin the queue the way traffic does.
	if !node.enqueueOutbound(ctx, node.peers["gone"], gossip.Envelope{Type: gossip.TypePublish, Channel: "alpha", MessageID: "m1"}) {
		t.Fatal("expected first enqueue to succeed")
	}
	node.outboundMu.Lock()
	_, queued := node.outboundQueues["gone"]
	node.outboundMu.Unlock()
	if !queued {
		t.Fatal("expected outbound queue to exist after enqueue")
	}

	node.removePeer("gone", session)

	node.outboundMu.Lock()
	_, stillQueued := node.outboundQueues["gone"]
	node.outboundMu.Unlock()
	if stillQueued {
		t.Fatal("outbound queue survived removePeer: leaks per-peer memory until Stop")
	}
}

// The orphan sweep reclaims queues left by eviction paths that delete a peer
// without removePeer (their removePeer no-ops on session mismatch).
func TestSweepReclaimsOrphanedOutboundQueue(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	node, err := NewNode("mesh-outbound-sweep", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	node.mu.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node.rootCtx = ctx
	node.started = true
	peer := &peerConn{id: "evicted", addr: "203.0.113.2:4000"}
	node.peers["evicted"] = peer
	node.mu.Unlock()

	if !node.enqueueOutbound(ctx, peer, gossip.Envelope{Type: gossip.TypePublish, MessageID: "m1"}) {
		t.Fatal("expected first enqueue to succeed")
	}

	// Eviction path: delete the peer WITHOUT removePeer.
	node.mu.Lock()
	delete(node.peers, "evicted")
	node.mu.Unlock()

	node.sweepOrphanOutboundQueues()

	node.outboundMu.Lock()
	_, stillQueued := node.outboundQueues["evicted"]
	node.outboundMu.Unlock()
	if stillQueued {
		t.Fatal("orphan sweep left a queue for a peer that is no longer connected")
	}
}

// A local delivery queue (~295KB at depth) must not outlive its channel's
// last subscription: Unsubscribe tears it down; a Subscribe→Publish cycle
// afterwards spins a fresh worker and delivers again.
func TestUnsubscribeTearsDownLocalQueue(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	node, err := NewNode("mesh-local-teardown", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	node.mu.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node.rootCtx = ctx
	node.started = true
	node.cancel = cancel
	node.mu.Unlock()

	if code := node.Subscribe("lobby"); code != MOSS_OK {
		t.Fatalf("Subscribe: %d", code)
	}
	// Drive the queue directly with a bare-channel dispatchMessage, the
	// shape enqueueLocal's callers (deliverLocal) hand it.
	node.enqueueLocal(dispatchMessage{channel: "lobby", data: []byte("x")})
	if !queueExists(node, "lobby") {
		t.Fatal("expected local queue after delivery")
	}

	if code := node.Unsubscribe("lobby"); code != MOSS_OK {
		t.Fatalf("Unsubscribe: %d", code)
	}
	waitFor(t, func() bool {
		return !queueExists(node, "lobby")
	}, "local queue survived Unsubscribe: leaks ~295KB per channel for the node's life")

	// Resubscribing must work — teardown left no poison behind.
	if code := node.Subscribe("lobby"); code != MOSS_OK {
		t.Fatalf("re-Subscribe: %d", code)
	}
	node.enqueueLocal(dispatchMessage{channel: "lobby", data: []byte("y")})
	if !queueExists(node, "lobby") {
		t.Fatal("expected a fresh local queue after re-Subscribe")
	}
}

// Two rooms subscribing the same channel name share one queue; the queue
// must survive the first room's Unsubscribe and go with the second's.
func TestLocalQueueSurvivesWhileAnotherRoomSubscribes(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	node, err := NewNode("mesh-local-multiroom", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	node.mu.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node.rootCtx = ctx
	node.started = true
	node.cancel = cancel
	node.mu.Unlock()

	if code := node.JoinRoom("room-b", []byte("psk-b")); code != MOSS_OK {
		t.Fatalf("JoinRoom: %d", code)
	}
	if code := node.Subscribe("lobby"); code != MOSS_OK {
		t.Fatalf("Subscribe own room: %d", code)
	}
	if code := node.SubscribeRoom("room-b", "lobby"); code != MOSS_OK {
		t.Fatalf("SubscribeRoom b: %d", code)
	}

	node.enqueueLocal(dispatchMessage{channel: "lobby", data: []byte("x")})
	if !queueExists(node, "lobby") {
		t.Fatal("expected shared local queue")
	}

	if code := node.UnsubscribeRoom("room-b", "lobby"); code != MOSS_OK {
		t.Fatalf("UnsubscribeRoom b: %d", code)
	}
	if !queueExists(node, "lobby") {
		t.Fatal("queue torn down while the node's own room still subscribes the channel")
	}

	if code := node.Unsubscribe("lobby"); code != MOSS_OK {
		t.Fatalf("Unsubscribe own room: %d", code)
	}
	waitFor(t, func() bool {
		return !queueExists(node, "lobby")
	}, "queue survived its last subscription's Unsubscribe")
}
