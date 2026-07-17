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
