package mesh

import (
	"context"
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/bootstrap"
)

// TestBootstrapLoopSurvivesZeroAnnounceInterval pins the guard that keeps a
// zero-value AnnounceIntervalSec (a Config literal that never went through
// applyDefaults) from reaching the bootstrap loop's timer. The loop used to
// arm a ticker with the raw interval, and time.NewTicker panics on
// non-positive input — inside the loop's goroutine, which kills the whole
// process, not just the loop. The wait must instead fall back to a sane
// floor.
func TestBootstrapLoopSurvivesZeroAnnounceInterval(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MasqConfig = MasqConfig{}
	cfg.Trackers = nil
	cfg.LANDiscoveryEnabled = false
	// The two fields a raw Config{} leaves at zero.
	cfg.AnnounceIntervalSec = 0
	cfg.AnnounceJitterSec = 0
	node, err := NewNode("mesh-bootstrap-zero-interval", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	if code := node.Start(); code != MOSS_OK {
		node.Stop()
		t.Fatalf("Start failed: %d", code)
	}
	defer node.Stop()

	// Every wait the loop can arm must be the documented floor or a backoff
	// on it — never zero, never negative.
	if w := node.announceRoundWait(0); w != time.Second {
		t.Fatalf("zero interval: wait = %v, want the 1s floor", w)
	}
	for _, empty := range []int{1, 3, 1000} {
		if w := node.announceRoundWait(empty); w <= 0 {
			t.Fatalf("zero interval with %d empty rounds: wait = %v, want positive", empty, w)
		}
	}

	// The loop's first arm happens right after Start; surviving past it —
	// plus a clean Stop — is the regression proof. Pre-fix, the goroutine
	// panicked here and took the test binary down with it.
	time.Sleep(1500 * time.Millisecond)
}

// TestAnnounceAndConnectReturnsTrackerPeerCount pins the return contract the
// bootstrap loop's empty-round backoff consumes: the count is the round's
// yield as the trackers reported it, 0 for an error round, and 0 for a
// successful round nobody answered. MaxPeers=0 keeps the kicked dials
// connectionless ("max peers reached") so no real handshake races the
// assertions.
func TestAnnounceAndConnectReturnsTrackerPeerCount(t *testing.T) {
	tracker := newCompactTracker()
	defer tracker.Close()

	cfg := DefaultConfig()
	cfg.Trackers = []string{tracker.URL()}
	cfg.LANDiscoveryEnabled = false
	cfg.MaxPeers = 0
	node, err := NewNode("mesh-announce-count", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	defer node.Stop()

	// A successful round nobody answered: the manager reports an empty merge
	// as an error, so the contract the loop consumes is the COUNT — 0
	// peers is an empty round for backoff purposes whatever the error says.
	// (AnnounceAll's own empty-swarm error text is out of scope here.)
	got, err := node.announceAndConnect(context.Background(), bootstrap.EventNone)
	if got != 0 {
		t.Fatalf("empty-tracker round = %d peers, want 0 (err = %v)", got, err)
	}

	// Successful round with peers: the count is exactly what the trackers
	// merged, regardless of how many dials survive.
	tracker.SetPeers([]string{"127.0.0.1:41031", "127.0.0.1:41032"})
	got, err = node.announceAndConnect(context.Background(), bootstrap.EventStarted)
	if err != nil {
		t.Fatalf("peer round errored: %v", err)
	}
	if got != 2 {
		t.Fatalf("peer round = %d, want 2 candidate peers", got)
	}

	// Every tracker dead: the round reports the failure, never a fake count.
	deadCfg := DefaultConfig()
	deadCfg.Trackers = []string{"http://127.0.0.1:1/announce"}
	deadCfg.LANDiscoveryEnabled = false
	deadCfg.MaxPeers = 0
	deadNode, err := NewNode("mesh-announce-count-dead", nil, deadCfg)
	if err != nil {
		t.Fatalf("NewNode deadNode failed: %v", err)
	}
	defer deadNode.Stop()
	got, err = deadNode.announceAndConnect(context.Background(), bootstrap.EventStopped)
	if err == nil {
		t.Fatal("dead-tracker round returned nil error, want the dial failure")
	}
	if got != 0 {
		t.Fatalf("dead-tracker round = %d peers, want 0", got)
	}
}
