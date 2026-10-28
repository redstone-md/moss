package mesh

import (
	"testing"
	"time"
)

// A replacement session admitted under the same ID while the prune scan was
// scoring must not inherit the departed session's score backoff.
func TestPruneLowScoringPeersSkipsReplacedSession(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	node, err := NewNode("mesh-low-score-replace", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	node.mu.Lock()
	node.peers["low"] = &peerConn{id: "low", connectedAt: time.Now().Add(-time.Minute)}
	node.mu.Unlock()
	node.scoring.SetApplicationScore("low", -5)
	replacement := &peerConn{id: "low", connectedAt: time.Now()}
	node.SetScoringCallback(func(peerID [32]byte, baseScore float64) float64 {
		node.mu.Lock()
		node.peers["low"] = replacement
		node.mu.Unlock()
		return baseScore
	})
	t.Cleanup(func() { node.SetScoringCallback(nil) })

	node.pruneLowScoringPeers()

	node.mu.RLock()
	blocked := replacement.meshScoreBlocked
	node.mu.RUnlock()
	if !blocked.IsZero() {
		t.Fatal("a replacement session inherited the departed session's score backoff")
	}
}
