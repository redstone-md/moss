package mesh

import (
	"context"
	"testing"
	"time"
)

// A stalled probe write must stall only its own worker: the pool dispatches
// the pass's pings concurrently, so healthy peers' probes leave while a peer
// whose WritePacket parks never releases. This is the phantom-miss fix: the
// old serial send loop chained the stalled write ahead of every later target
// in the pass while all of them were already timestamped at collect time, so
// the prune scan read the accumulated delay as expiry and healthy peers
// collected misses for a ping that was merely stuck in queue behind one
// stalled peer.
func TestStalledPingWriteDoesNotBlockHealthyProbes(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	node, err := NewNode("mesh-ping-pool", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}

	release := make(chan struct{})
	stalled := newBlockingSession(t, release)
	healthy := newRecordedSession(t)

	ctx, cancel := context.WithCancel(context.Background())
	node.mu.Lock()
	node.rootCtx = ctx
	node.started = true
	node.peers["stalled"] = &peerConn{id: "stalled", session: stalled.session}
	node.peers["healthy"] = &peerConn{id: "healthy", session: healthy.session}
	node.mu.Unlock()

	// The stalled target is FIRST on purpose: against the serial loop it
	// deterministically parks the healthy peer's write behind it, while the
	// pool must complete the healthy probe regardless of ordering.
	targets := []pingTarget{
		{peer: node.peers["stalled"], requestID: "stalled-req"},
		{peer: node.peers["healthy"], requestID: "healthy-req"},
	}
	node.mu.Lock()
	node.peers["stalled"].pingPending = "stalled-req"
	node.peers["healthy"].pingPending = "healthy-req"
	node.mu.Unlock()

	node.sendPingTargets(targets)

	// The healthy peer's ping left the node while the stalled write is still
	// parked (release is not closed), and its probe stays armed awaiting the
	// pong — a successful write is a miss only if the pong never comes.
	waitFor(t, func() bool { return healthy.writeCount() == 1 },
		"the stalled peer's write blocked the healthy peer's probe")
	node.mu.RLock()
	pending := node.peers["healthy"].pingPending
	node.mu.RUnlock()
	if pending != "healthy-req" {
		t.Fatalf("successful probe write must stay armed for the pong, got pending=%q", pending)
	}

	// Teardown proves no leak: cancel, unstick the parked write, and both
	// workers must leave the WaitGroup promptly.
	cancel()
	close(release)
	drained := make(chan struct{})
	go func() {
		node.wg.Wait()
		close(drained)
	}()
	select {
	case <-drained:
	case <-time.After(5 * time.Second):
		t.Fatal("ping send workers leaked past cancel + release")
	}
}
