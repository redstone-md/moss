package mesh

import (
	"sync"
	"testing"
	"time"
)

// The merged conn-tick pass must behave exactly like the standalone probe
// followed by the standalone prune: a fresh peer gets its ping, an expired
// pending ping is consumed into a miss, and only the timed-out peer leaves
// the topic mesh — all within one n.mu acquisition instead of two.
func TestConnTickProbesFreshAndConsumesExpiredPing(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	node, err := NewNode("mesh-conn-tick", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	topic := "topic-conn-tick"
	node.rememberSubscription(topic, "", "alpha")
	node.pubsub.Subscribe(topic)
	node.pubsub.SetMeshPeer(topic, "fresh", true)
	node.pubsub.SetMeshPeer(topic, "stale", true)

	rec := newRecordedSession(t)
	now := time.Now()
	node.mu.Lock()
	node.peers["fresh"] = &peerConn{
		id:          "fresh",
		connectedAt: now.Add(-10 * time.Second),
		session:     rec.session,
	}
	node.peers["stale"] = &peerConn{
		id:          "stale",
		connectedAt: now.Add(-40 * time.Second),
		pingPending: "stale-req",
		pingSentAt:  now.Add(-peerPingTimeout - time.Second),
	}
	node.mu.Unlock()

	node.connTickProbeAndPrune(now)

	node.mu.RLock()
	fresh, stale := node.peers["fresh"], node.peers["stale"]
	node.mu.RUnlock()
	if fresh.pingPending == "" || !fresh.pingSentAt.Equal(now) {
		t.Fatalf("fresh peer was not probed: pending=%q sent=%s", fresh.pingPending, fresh.pingSentAt)
	}
	if stale.pingPending != "" || !stale.pingSentAt.IsZero() || stale.pingMisses != 1 {
		t.Fatalf("expired ping not consumed by merged pass: pending=%q misses=%d", stale.pingPending, stale.pingMisses)
	}
	if !node.pubsub.InMesh(topic, "fresh") {
		t.Fatal("a merely-probed peer was pruned from the mesh")
	}
	if node.pubsub.InMesh(topic, "stale") {
		t.Fatal("a peer whose ping expired stayed in the mesh")
	}
	if got := rec.writeCount(); got != 1 {
		t.Fatalf("expected exactly the fresh peer's ping on the wire, got %d writes", got)
	}
}

// Six missed pings on a peer past the 30s retention grace must close its
// session in the merged pass; a young peer with the same miss count is pruned
// from the mesh but its session must survive the grace window.
func TestConnTickDisconnectsPeerAtMissLimit(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	node, err := NewNode("mesh-conn-tick-disc", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	topic := "topic-conn-tick-disc"
	node.rememberSubscription(topic, "", "alpha")
	node.pubsub.Subscribe(topic)

	deadRec := newRecordedSession(t)
	youngRec := newRecordedSession(t)
	now := time.Now()
	node.mu.Lock()
	node.peers["dead"] = &peerConn{
		id:          "dead",
		connectedAt: now.Add(-40 * time.Second),
		pingPending: "old-req",
		pingSentAt:  now.Add(-peerPingTimeout - time.Second),
		pingMisses:  peerDisconnectMissLimit - 1,
		session:     deadRec.session,
	}
	node.peers["young"] = &peerConn{
		id:          "young",
		connectedAt: now.Add(-10 * time.Second),
		pingPending: "young-req",
		pingSentAt:  now.Add(-peerPingTimeout - time.Second),
		pingMisses:  peerDisconnectMissLimit - 1,
		session:     youngRec.session,
	}
	node.mu.Unlock()
	node.pubsub.SetMeshPeer(topic, "dead", true)
	node.pubsub.SetMeshPeer(topic, "young", true)

	node.connTickProbeAndPrune(now)
	node.mu.RLock()
	dead := node.peers["dead"]
	node.mu.RUnlock()
	if dead.pingMisses != peerDisconnectMissLimit {
		t.Fatalf("expected %d misses after the pass, got %d", peerDisconnectMissLimit, dead.pingMisses)
	}
	if !deadRec.carrier.closed {
		t.Fatal("a peer past the grace window at the miss limit was not disconnected")
	}
	if youngRec.carrier.closed {
		t.Fatal("a peer inside the 30s grace window was disconnected despite retention")
	}
	if node.pubsub.InMesh(topic, "dead") || node.pubsub.InMesh(topic, "young") {
		t.Fatal("timed-out peers stayed in the mesh after the merged pass")
	}
}

// pruneLowScoringPeers must evaluate peerScore strictly after releasing
// n.mu: a scoring callback that takes the node's write lock would
// self-deadlock if the score were drawn under RLock. This pins the
// node_peer_discovery.go:76 invariant for the prune path.
func TestPruneLowScoringPeersScoresOutsideNodeLock(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	node, err := NewNode("mesh-low-score-lock", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	topic := "topic-low-score-lock"
	node.rememberSubscription(topic, "", "alpha")
	node.pubsub.Subscribe(topic)

	node.mu.Lock()
	node.peers["low"] = &peerConn{
		id:          "low",
		connectedAt: time.Now().Add(-time.Minute),
	}
	node.mu.Unlock()
	node.pubsub.SetMeshPeer(topic, "low", true)
	node.scoring.SetApplicationScore("low", -5)

	scored := make(chan struct{})
	var once sync.Once
	node.SetScoringCallback(func(peerID [32]byte, baseScore float64) float64 {
		node.mu.Lock() // would self-deadlock if peerScore ran under n.mu
		node.mu.Unlock()
		once.Do(func() { close(scored) })
		return baseScore
	})
	t.Cleanup(func() { node.SetScoringCallback(nil) })

	go node.pruneLowScoringPeers()

	select {
	case <-scored:
	case <-time.After(2 * time.Second):
		t.Fatal("scoring callback deadlocked on n.mu: peerScore was invoked while n.mu was held")
	}

	// The callback firing only proves the score ran outside the lock; the
	// mesh prune that follows it needs a moment to land.
	waitFor(t, func() bool { return !node.pubsub.InMesh(topic, "low") },
		"a negative-score peer stayed in the mesh after pruneLowScoringPeers")

	node.mu.RLock()
	_, stillConnected := node.peers["low"]
	node.mu.RUnlock()
	if !stillConnected {
		t.Fatal("a low-scoring but live peer was disconnected instead of only mesh-pruned")
	}
}
