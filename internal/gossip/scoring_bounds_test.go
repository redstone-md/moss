package gossip

import (
	"testing"
	"time"
)

// Remove must evict: without it the peers map grew by one entry per peer the
// node ever scored, and Tick() walked all of them every second forever.
func TestEngineRemoveEvictsPeer(t *testing.T) {
	engine := NewEngine()
	engine.RewardFirstDelivery("peer-gone")
	if engine.Score("peer-gone") <= 0 {
		t.Fatal("expected scored peer before removal")
	}
	if !engine.Remove("peer-gone") {
		t.Fatal("expected Remove to report an existing peer")
	}
	if engine.Remove("peer-gone") {
		t.Fatal("expected second Remove of the same peer to report false")
	}
	// Score of an evicted peer reads as zero without recreating the entry.
	if got := engine.Score("peer-gone"); got != 0 {
		t.Fatalf("expected zero score after removal, got %f", got)
	}
}

func TestEngineRemoveFiresHook(t *testing.T) {
	engine := NewEngine()
	removed := make(chan string, 1)
	engine.SetOnRemove(func(peerID string) {
		removed <- peerID
	})
	engine.Ensure("peer-hooked")
	engine.Remove("peer-hooked")
	select {
	case id := <-removed:
		if id != "peer-hooked" {
			t.Fatalf("hook saw wrong peer: %s", id)
		}
	case <-time.After(time.Second):
		t.Fatal("expected eviction hook to fire")
	}
}

// Score is the hot path of every scoring sort in the mesh; it must not create
// entries for unknown peers (the old behavior charged the write lock and
// grew the map on every lookup of a stranger).
func TestScoreOfUnknownPeerDoesNotTrackIt(t *testing.T) {
	engine := NewEngine()
	engine.Score("stranger")
	engine.mu.Lock()
	tracked := len(engine.peers)
	engine.mu.Unlock()
	if tracked != 0 {
		t.Fatalf("expected no tracked peers after unknown Score, got %d", tracked)
	}
}

// TimeInMesh grows 0.03/s but must stop at the cap: an uncapped component
// made a year-old entry outrank every fresh peer on time alone.
func TestTimeInMeshCapsAtOneHour(t *testing.T) {
	now := time.Now()
	fresh := timeInMeshLocked(now.Add(-time.Minute), now)
	if fresh <= 0 || fresh > 3600*0.03 {
		t.Fatalf("expected sub-cap mesh time, got %f", fresh)
	}
	capped := timeInMeshLocked(now.Add(-30*24*time.Hour), now)
	if capped != maxTimeInMeshSeconds*0.03 {
		t.Fatalf("expected capped mesh time %f, got %f", maxTimeInMeshSeconds*0.03, capped)
	}
	engine := NewEngine()
	engine.mu.Lock()
	engine.peers["old"] = &PeerScore{ConnectedAt: now.Add(-30 * 24 * time.Hour)}
	engine.mu.Unlock()
	engine.Tick()
	engine.mu.Lock()
	got := engine.peers["old"].TimeInMesh
	engine.mu.Unlock()
	if got != maxTimeInMeshSeconds*0.03 {
		t.Fatalf("expected Tick to clamp TimeInMesh to %f, got %f", maxTimeInMeshSeconds*0.03, got)
	}
}
