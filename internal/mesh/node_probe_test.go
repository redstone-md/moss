package mesh

import (
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
)

// The probe floor (peerProbeIntervalFloor) must survive a pong: handlePong
// re-bases pingSentAt at the pong's arrival instead of zeroing it, so the
// next probe of a healthy peer waits a full interval — not the ~1s conn-tick.
// Zeroing pingSentAt on pong used to make the floor dead code: every healthy
// peer looked never-probed and was pinged once per second.
func TestProbeFloorHoldsAfterPong(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	node, err := NewNode("mesh-probe-floor", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	rec := newRecordedSession(t)
	now := time.Now()
	node.mu.Lock()
	node.peers["healthy"] = &peerConn{
		id:          "healthy",
		connectedAt: now.Add(-time.Minute),
		session:     rec.session,
	}
	node.mu.Unlock()

	// Arm the first probe exactly as the maintenance loop does.
	node.probePeerLatency(now)
	node.mu.RLock()
	peer := node.peers["healthy"]
	requestID := peer.pingPending
	sentAt := peer.pingSentAt
	node.mu.RUnlock()
	if requestID == "" || sentAt.IsZero() {
		t.Fatalf("initial probe did not arm: pending=%q sent=%s", requestID, sentAt)
	}

	// The peer answers: RTT recorded, pending cleared — and pingSentAt must
	// be re-based at the pong, not zeroed.
	node.handlePong(node.peers["healthy"], gossip.Envelope{Type: gossip.TypePong, RequestID: requestID})
	node.mu.RLock()
	peer = node.peers["healthy"]
	pongAt := peer.pingSentAt
	pending := peer.pingPending
	misses := peer.pingMisses
	node.mu.RUnlock()
	if pending != "" || misses != 0 {
		t.Fatalf("pong did not reset probe state: pending=%q misses=%d", pending, misses)
	}
	if pongAt.IsZero() {
		t.Fatal("handlePong zeroed pingSentAt: the probe floor loses its base and every conn-tick re-pings a healthy peer")
	}

	interval := node.peerProbeInterval()
	if interval != peerProbeIntervalFloor {
		t.Fatalf("expected the default probe interval to be the %s floor, got %s", peerProbeIntervalFloor, interval)
	}

	// One tick before the floor elapses: no second ping on the wire.
	before := rec.writeCount()
	node.connTickProbeAndPrune(pongAt.Add(interval - time.Second))
	node.mu.RLock()
	peer = node.peers["healthy"]
	pending = peer.pingPending
	node.mu.RUnlock()
	if pending != "" {
		t.Fatalf("peer re-probed at %s after pong, before the %s floor: pending=%q", interval-time.Second, interval, pending)
	}
	if got := rec.writeCount() - before; got != 0 {
		t.Fatalf("expected no ping inside the floor, got %d writes", got)
	}

	// At the floor exactly, the next probe fires — one ping per interval.
	node.connTickProbeAndPrune(pongAt.Add(interval))
	atFloor := pongAt.Add(interval)
	node.mu.RLock()
	peer = node.peers["healthy"]
	pending = peer.pingPending
	sentAt = peer.pingSentAt
	node.mu.RUnlock()
	if pending == "" || !sentAt.Equal(atFloor) {
		t.Fatalf("peer not re-probed at the floor: pending=%q sent=%s", pending, sentAt)
	}
	if got := rec.writeCount() - before; got != 1 {
		t.Fatalf("expected exactly one ping at the floor, got %d writes", got)
	}
}

// The prune scan consumes an expired ping by pingPending plus pingSentAt age —
// never by the timestamp's zero value. A peer that answered its probe keeps a
// non-zero (and possibly long-aged) pingSentAt with no pending ping; the prune
// pass must not turn that retained timestamp into a miss, while a silent peer
// with a genuinely expired pending ping is consumed.
func TestPruneGatesOnPendingNotTimestamp(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	node, err := NewNode("mesh-prune-gate", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	topic := "topic-prune-gate"
	node.rememberSubscription(topic, "", "alpha")
	node.pubsub.Subscribe(topic)
	node.pubsub.SetMeshPeer(topic, "answered", true)
	node.pubsub.SetMeshPeer(topic, "silent", true)

	rec := newRecordedSession(t)
	now := time.Now()
	node.mu.Lock()
	// "answered" ponged 30s ago: handlePong leaves pingSentAt non-zero, and
	// the floor has since elapsed so this same pass re-probes it.
	node.peers["answered"] = &peerConn{
		id:          "answered",
		connectedAt: now.Add(-time.Minute),
		session:     rec.session,
		pingSentAt:  now.Add(-30 * time.Second),
	}
	// "silent" was probed 6s ago and never answered: expired (timeout 5s).
	node.peers["silent"] = &peerConn{
		id:          "silent",
		connectedAt: now.Add(-time.Minute),
		pingPending: "silent-req",
		pingSentAt:  now.Add(-peerPingTimeout - time.Second),
	}
	node.mu.Unlock()

	node.connTickProbeAndPrune(now)

	node.mu.RLock()
	answered, silent := node.peers["answered"], node.peers["silent"]
	node.mu.RUnlock()
	if answered.pingMisses != 0 || answered.pingPending == "" {
		t.Fatalf("answered peer miscounted by prune: pending=%q misses=%d", answered.pingPending, answered.pingMisses)
	}
	if !answered.pingSentAt.Equal(now) {
		t.Fatalf("answered peer not re-probed at the elapsed floor: sent=%s", answered.pingSentAt)
	}
	if !node.pubsub.InMesh(topic, "answered") {
		t.Fatal("a merely re-probed peer was pruned from the mesh")
	}
	if silent.pingPending != "" || silent.pingMisses != 1 {
		t.Fatalf("expired ping not consumed: pending=%q misses=%d", silent.pingPending, silent.pingMisses)
	}
	if node.pubsub.InMesh(topic, "silent") {
		t.Fatal("a peer whose ping expired stayed in the mesh")
	}
	if got := rec.writeCount(); got != 1 {
		t.Fatalf("expected exactly the answered peer's re-probe on the wire, got %d writes", got)
	}
}

// Without a pong, every probe that expires must cost exactly one miss, and the
// maintenance cadence keeps re-arming fresh probes — so misses climb one per
// timeout cycle until the disconnect limit closes the session.
func TestMissCountGrowsWhenPongSilent(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	node, err := NewNode("mesh-miss-growth", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	rec := newRecordedSession(t)
	start := time.Now()
	node.mu.Lock()
	node.peers["silent"] = &peerConn{
		id:          "silent",
		connectedAt: start.Add(-time.Minute),
		session:     rec.session,
	}
	node.mu.Unlock()

	// One conn-tick every ping-timeout+1s: each even tick consumes the probe
	// armed by the tick before it, one miss at a time.
	wantMisses := 0
	for tick := 1; tick <= 2*peerDisconnectMissLimit; tick++ {
		node.connTickProbeAndPrune(start.Add(time.Duration(tick) * (peerPingTimeout + time.Second)))
		if tick%2 == 0 {
			wantMisses++
		}
		node.mu.RLock()
		peer := node.peers["silent"]
		gotMisses, pending := peer.pingMisses, peer.pingPending
		node.mu.RUnlock()
		if gotMisses != wantMisses {
			t.Fatalf("tick %d: miss count %d, want %d (pending=%q)", tick, gotMisses, wantMisses, pending)
		}
	}
	node.mu.RLock()
	peer := node.peers["silent"]
	finalMisses := peer.pingMisses
	node.mu.RUnlock()
	if finalMisses != peerDisconnectMissLimit {
		t.Fatalf("final miss count %d, want %d", finalMisses, peerDisconnectMissLimit)
	}
	if !rec.carrier.closed {
		t.Fatal("a silent peer reached the miss limit but its session was not closed")
	}
	// Every cycle armed exactly one ping: one write per odd tick, none on the
	// consuming even ticks.
	if got := rec.writeCount(); got != peerDisconnectMissLimit {
		t.Fatalf("expected %d pings on the wire across the cycles, got %d", peerDisconnectMissLimit, got)
	}
}
