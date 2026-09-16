package mesh

import (
	"testing"
	"time"
)

// Re-flooding must be bounded per peer.
//
// The state-based gate that decides whether to forward an announcement assumes
// disagreements between nodes get settled. They do not: a forwarding node
// substitutes its own view of the peer's capabilities and strips the signature
// when it differs, so the next hop cannot verify it, keeps its own value, and
// disagrees straight back. Each correction floods every peer.
//
// A relay with seven peers measured 21,808 supernode announcements in two
// minutes — against 29 pings — while discarding 142,125 packets in one of them.
// Those lost pings are why healthy sessions die at six misses. A permanent
// oscillation must cost one message per peer per cooldown, not a flood.
func TestAnnounceForwardingIsCappedPerPeer(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	n, err := NewNode("mesh-announce-flood", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	if !n.shouldForwardAnnounce("peer-1") {
		t.Fatal("the first announcement for a peer must be forwarded")
	}
	for i := 0; i < 50; i++ {
		if n.shouldForwardAnnounce("peer-1") {
			t.Fatalf("announcement %d for the same peer was forwarded inside the cooldown: "+
				"an oscillation between two nodes floods the substrate again", i)
		}
	}

	// A different peer is a different story and must not be throttled by the first.
	if !n.shouldForwardAnnounce("peer-2") {
		t.Fatal("one peer's cooldown suppressed an unrelated peer's announcement")
	}
}

// The cap must lift once the cooldown passes: throttling is meant to bound a
// storm, not to stop a peer's genuine state changes from ever propagating.
func TestAnnounceForwardingResumesAfterCooldown(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	n, err := NewNode("mesh-announce-cooldown", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	if !n.shouldForwardAnnounce("peer-1") {
		t.Fatal("the first announcement must be forwarded")
	}
	n.mu.Lock()
	n.announceForwards["peer-1"] = time.Now().Add(-announceForwardCooldown - time.Second)
	n.mu.Unlock()

	if !n.shouldForwardAnnounce("peer-1") {
		t.Fatal("a peer's state change stayed suppressed after the cooldown expired")
	}
}

// An empty id is not a peer and must never be forwarded — nor claim a slot in
// the throttle table that a real peer could collide with.
func TestAnnounceForwardingRejectsAnEmptyPeer(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	n, err := NewNode("mesh-announce-empty", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	if n.shouldForwardAnnounce("") {
		t.Fatal("an announcement for no peer was forwarded")
	}
}

// The accept path must not turn a join wave into a self-announce storm.
// registerPeerFrom used to broadcast our identity to every connected peer on
// EVERY accept: 100 simultaneous joins on a 100-peer mesh was N×(N-1) ≈ 9800
// envelopes in one instant, all carrying unchanged state, each burning the
// recipients' inbound announceBudget before dying at the meaningfulChange
// gate. The joiner itself never needed the broadcast — it is excluded and
// gets our self-announce as the first envelope of its snapshot. The gate is
// the same per-advertised-peer cooldown that caps every other re-flood.
func TestAnnounceSelfToPeersIsCappedPerCooldown(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	node, err := NewNode("mesh-announce-join-storm", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	watcher := newRecordedSession(t)
	node.mu.Lock()
	// The watcher must pass the baseline filter broadcastToAll applies;
	// a fresh peerConn has score 0 which is exactly BaselineThreshold.
	node.peers["watcher"] = &peerConn{id: "watcher", session: watcher.session}
	node.mu.Unlock()

	// 100 sequential joins — the storm, without the handshakes.
	for range 100 {
		node.announceSelfToPeers("joiner")
	}

	if got := watcher.writeCount(); got != 1 {
		t.Fatalf("expected the watcher to receive exactly one self-announce across "+
			"100 joins (cooldown-capped fan-out), got %d: the accept path is an "+
			"O(N^2) storm again", got)
	}

	// The cap must lift once the cooldown passes: a peer that genuinely
	// changes state (or the next window) still gets its broadcast.
	node.mu.Lock()
	node.announceForwards[node.localPeerID()] = time.Now().Add(-announceForwardCooldown - time.Second)
	node.mu.Unlock()
	node.announceSelfToPeers("joiner")
	if got := watcher.writeCount(); got != 2 {
		t.Fatalf("expected a post-cooldown join to broadcast again, got %d packets total", got)
	}
}
