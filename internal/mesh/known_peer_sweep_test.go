package mesh

import (
	"fmt"
	"testing"
	"time"
)

// newSweepTestNode builds an unstarted node with a small peer ceiling, the
// shape the sweep tests use. LAN discovery and trackers stay off so nothing
// races the directory behind the test's back.
func newSweepTestNode(t *testing.T, maxPeers int) *Node {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Trackers = nil
	cfg.LANDiscoveryEnabled = false
	cfg.MaxPeers = maxPeers
	node, err := NewNode("mesh-known-sweep", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	t.Cleanup(func() { _ = node.Stop() })
	return node
}

// putSweepablePeer inserts a non-LAN, non-seed, unconnected known peer with a
// controlled lastSeen — exactly what sweepKnownPeers owns.
func putSweepablePeer(node *Node, peerID string, lastSeen time.Time) {
	node.mu.Lock()
	defer node.mu.Unlock()
	node.knownPeers[peerID] = knownPeer{
		id:       peerID,
		addr:     "203.0.113.10:4001",
		lastSeen: lastSeen,
	}
}

// knownPeerCount reports the directory size under the read lock.
func knownPeerCount(node *Node) int {
	node.mu.RLock()
	defer node.mu.RUnlock()
	return len(node.knownPeers)
}

// Stale non-LAN entries must leave: without the sweep, every announcement
// ever relayed through the node stayed forever, and the dial pass walked all
// of them.
func TestSweepKnownPeersDropsStaleNonLANEntries(t *testing.T) {
	node := newSweepTestNode(t, 8)
	now := time.Now()
	putSweepablePeer(node, "stale-1", now.Add(-knownPeerStaleTTL-time.Minute))
	putSweepablePeer(node, "fresh-1", now)

	node.sweepKnownPeers(now)

	node.mu.RLock()
	defer node.mu.RUnlock()
	if _, ok := node.knownPeers["stale-1"]; ok {
		t.Fatal("expected stale entry to be dropped")
	}
	if _, ok := node.knownPeers["fresh-1"]; !ok {
		t.Fatal("expected fresh entry to survive")
	}
}

// Protected entries are never swept: LAN (owned by pruneLANPeersLocked),
// bootstrap seeds, direct peers, and connected peers.
func TestSweepKnownPeersKeepsProtectedEntries(t *testing.T) {
	node := newSweepTestNode(t, 8)
	now := time.Now()
	ancient := now.Add(-knownPeerStaleTTL - time.Hour)

	node.mu.Lock()
	node.knownPeers["lan-peer"] = knownPeer{id: "lan-peer", addr: "10.1.1.1:1", lan: true, lastSeen: ancient}
	node.knownPeers["seed-peer"] = knownPeer{id: "seed-peer", addr: "10.1.1.2:1", bootstrap: true, lastSeen: ancient}
	node.knownPeers["direct-peer"] = knownPeer{id: "direct-peer", addr: "10.1.1.3:1", direct: true, lastSeen: ancient}
	node.peers["connected-peer"] = &peerConn{id: "connected-peer"}
	node.knownPeers["connected-peer"] = knownPeer{id: "connected-peer", addr: "10.1.1.4:1", lastSeen: ancient}
	node.mu.Unlock()

	node.sweepKnownPeers(now)

	node.mu.RLock()
	defer node.mu.RUnlock()
	for _, id := range []string{"lan-peer", "seed-peer", "direct-peer", "connected-peer"} {
		if _, ok := node.knownPeers[id]; !ok {
			t.Fatalf("expected protected entry %s to survive the sweep", id)
		}
	}
}

// The directory is capped: over the cap, the OLDEST sweepable entries go,
// newest survive — and the eviction also clears the peer-keyed bookkeeping.
func TestSweepKnownPeersCapsDirectoryDroppingOldestFirst(t *testing.T) {
	node := newSweepTestNode(t, 8)
	now := time.Now()
	capLimit := knownPeerSweepCap(8)
	if capLimit != knownPeerSweepMinCap {
		t.Fatalf("test premise: expected min-cap %d for MaxPeers=8, got %d", knownPeerSweepMinCap, capLimit)
	}
	// All entries fresh (well inside the TTL) and ordered oldest → newest,
	// so ONLY the cap can evict, proving the cap path on its own.
	for i := 0; i < capLimit+10; i++ {
		putSweepablePeer(node, fmt.Sprintf("p-%02d", i), now.Add(-time.Duration(i)*time.Second))
	}
	// Pin one peer's dial bookkeeping so the eviction is proven to clear it.
	node.mu.Lock()
	node.peerDials["p-00"] = now
	node.peerDialFailures["p-00"] = 3
	node.mu.Unlock()

	node.sweepKnownPeers(now)

	if got := knownPeerCount(node); got != capLimit {
		t.Fatalf("expected directory capped at %d, got %d", capLimit, got)
	}
	// The 10 OLDEST (largest i — furthest lastSeen) went; the newest
	// capLimit stayed.
	node.mu.RLock()
	for i := capLimit; i < capLimit+10; i++ {
		if _, ok := node.knownPeers[fmt.Sprintf("p-%02d", i)]; ok {
			node.mu.RUnlock()
			t.Fatalf("expected old entry p-%02d to be evicted", i)
		}
	}
	for i := 0; i < capLimit; i++ {
		if _, ok := node.knownPeers[fmt.Sprintf("p-%02d", i)]; !ok {
			node.mu.RUnlock()
			t.Fatalf("expected newest entry p-%02d to survive", i)
		}
	}
	node.mu.RUnlock()

	// Bookkeeping: an evicted peer's dial state must go with it. Re-insert
	// a stale entry with pinned dial bookkeeping and sweep again past the
	// throttle window: the sweep's removal path must clear both.
	staleID := fmt.Sprintf("p-%02d", capLimit)
	node.mu.Lock()
	node.knownPeers[staleID] = knownPeer{
		id:       staleID,
		addr:     "203.0.113.10:4001",
		lastSeen: now.Add(-knownPeerStaleTTL - time.Minute),
	}
	node.peerDials[staleID] = now
	node.peerDialFailures[staleID] = 3
	node.mu.Unlock()
	node.sweepKnownPeers(now.Add(knownPeerSweepEvery + time.Second))
	node.mu.RLock()
	_, known := node.knownPeers[staleID]
	_, dial := node.peerDials[staleID]
	_, failures := node.peerDialFailures[staleID]
	node.mu.RUnlock()
	if known {
		t.Fatal("expected re-inserted stale entry to be dropped by the second sweep")
	}
	if dial {
		t.Fatal("expected evicted peer's dial bookkeeping to be cleared")
	}
	if failures {
		t.Fatal("expected evicted peer's dial failures to be cleared")
	}
}

// The sweep self-throttles: a second call inside the window must not walk
// again, and one after it must.
func TestSweepKnownPeersThrottlesToCadence(t *testing.T) {
	node := newSweepTestNode(t, 8)
	start := time.Now()
	putSweepablePeer(node, "stale-1", start.Add(-knownPeerStaleTTL-time.Minute))

	node.sweepKnownPeers(start)
	if got := knownPeerCount(node); got != 0 {
		t.Fatalf("expected stale entry gone on first sweep, %d left", got)
	}

	// Re-insert and sweep again inside the throttle window: no effect.
	putSweepablePeer(node, "stale-2", start.Add(-knownPeerStaleTTL-time.Minute))
	node.sweepKnownPeers(start.Add(time.Second))
	if got := knownPeerCount(node); got != 1 {
		t.Fatalf("expected throttled sweep to skip, directory now %d", got)
	}

	// After the window, it runs again.
	node.sweepKnownPeers(start.Add(knownPeerSweepEvery + time.Second))
	if got := knownPeerCount(node); got != 0 {
		t.Fatalf("expected post-window sweep to drop stale entry, %d left", got)
	}
}
