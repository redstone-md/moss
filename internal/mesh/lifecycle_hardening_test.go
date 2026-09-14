package mesh

import (
	"encoding/hex"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/nat"
)

// ---- Fix 1: single-outcome capacity ----

// At capacity, one inbound must cost exactly one eviction — never an eviction
// plus a rejection (the double churn the old branch performed) and never a
// silent overshoot. A prunable victim frees its slot and the newcomer takes
// it, in the same critical section.
func TestOverflowAdmitsNewcomerByEvictingOneVictim(t *testing.T) {
	hubCfg := isolatedTestConfig("overflow-evict")
	hubCfg.GossipSub.HeartbeatMS = 50
	hubCfg.MaxPeers = 2
	hub, err := NewNode("mesh-overflow-evict", nil, hubCfg)
	if err != nil {
		t.Fatalf("NewNode hub failed: %v", err)
	}
	if code := hub.Start(); code != MOSS_OK {
		t.Fatalf("hub.Start failed: %d", code)
	}
	defer hub.Stop()

	leaf := func(name string) *Node {
		cfg := isolatedTestConfig("overflow-evict")
		cfg.GossipSub.HeartbeatMS = 50
		cfg.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(hub.ListenPort()))}
		node, nodeErr := NewNode(name, nil, cfg)
		if nodeErr != nil {
			t.Fatalf("NewNode %s failed: %v", name, nodeErr)
		}
		if code := node.Start(); code != MOSS_OK {
			t.Fatalf("%s.Start failed: %d", name, code)
		}
		return node
	}

	nodeA := leaf("mesh-overflow-evict")
	defer nodeA.Stop()
	nodeB := leaf("mesh-overflow-evict")
	defer nodeB.Stop()

	waitForPeerCount(t, hub, 2)
	waitForPeerCount(t, nodeA, 1)
	waitForPeerCount(t, nodeB, 1)

	// Make nodeA prunable AT THE HUB: a past-the-retain-window session with
	// a negative score. selectOverflowPrunePeerLocked runs on the hub and
	// reads the hub's own scoring copy, so that is where A must look bad.
	nodeAPub := nodeA.PublicKey()
	nodeAID := hex.EncodeToString(nodeAPub[:])
	hub.mu.Lock()
	if peer := hub.peers[nodeAID]; peer != nil {
		peer.connectedAt = time.Now().Add(-2 * time.Minute)
	}
	hub.mu.Unlock()
	hub.scoring.SetApplicationScore(nodeAID, -50.0)

	// A third leaf dials the hub at capacity: the hub must evict the
	// prunable victim (nodeA) and admit nodeC — one eviction, one
	// admission, count stays at MaxPeers.
	nodeC := leaf("mesh-overflow-evict")
	defer nodeC.Stop()

	waitForPeerCount(t, hub, 2)
	waitForPeerCount(t, nodeC, 1)

	hub.mu.RLock()
	_, evicted := hub.peers[nodeAID]
	count := len(hub.peers)
	hub.mu.RUnlock()
	if evicted {
		t.Fatal("overflow eviction did not remove the prune victim from n.peers")
	}
	if count != 2 {
		t.Fatalf("expected hub to hold exactly MaxPeers=2 after evict-and-accept, got %d", count)
	}
	// The newcomer really is connected — not merely tolerated then dropped.
	if hub.currentPeerCount() != 2 {
		t.Fatalf("hub peer count %d, want 2", hub.currentPeerCount())
	}
}

// When every incumbent is worth keeping, the newcomer alone is rejected: no
// victim is evicted, capacity is respected, the incumbents stay.
func TestOverflowWithoutPrunableVictimRejectsOnlyTheNewcomer(t *testing.T) {
	hubCfg := isolatedTestConfig("overflow-reject")
	hubCfg.GossipSub.HeartbeatMS = 50
	hubCfg.MaxPeers = 1
	hub, err := NewNode("mesh-overflow-reject", nil, hubCfg)
	if err != nil {
		t.Fatalf("NewNode hub failed: %v", err)
	}
	if code := hub.Start(); code != MOSS_OK {
		t.Fatalf("hub.Start failed: %d", code)
	}
	defer hub.Stop()

	leaf := func(name string) *Node {
		cfg := isolatedTestConfig("overflow-reject")
		cfg.GossipSub.HeartbeatMS = 50
		cfg.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(hub.ListenPort()))}
		node, nodeErr := NewNode(name, nil, cfg)
		if nodeErr != nil {
			t.Fatalf("NewNode %s failed: %v", name, nodeErr)
		}
		if code := node.Start(); code != MOSS_OK {
			t.Fatalf("%s.Start failed: %d", name, code)
		}
		return node
	}

	nodeA := leaf("mesh-overflow-reject")
	defer nodeA.Stop()
	waitForPeerCount(t, hub, 1)
	waitForPeerCount(t, nodeA, 1)

	nodeB := leaf("mesh-overflow-reject")
	defer nodeB.Stop()

	// Fresh incumbent (nothing prunable): nodeB alone is rejected, nodeA
	// stays, count exactly 1.
	waitForPeerCountEventually(t, nodeB, 0)
	hub.mu.RLock()
	count := len(hub.peers)
	hub.mu.RUnlock()
	if count != 1 {
		t.Fatalf("expected hub to keep exactly 1 peer, got %d", count)
	}
	waitForPeerCount(t, nodeA, 1)
}

// ---- Fix 2: relayed peer cap ----

// relayedPeerCap keeps tiny relay leaves (MaxPeers=1) a workable floor while
// scaling with the configured capacity elsewhere.
func TestRelayedPeerCapFloor(t *testing.T) {
	if got := relayedPeerCap(0); got != 2 {
		t.Fatalf("relayedPeerCap(0) = %d, want 2", got)
	}
	if got := relayedPeerCap(1); got != 2 {
		t.Fatalf("relayedPeerCap(1) = %d, want 2", got)
	}
	if got := relayedPeerCap(8); got != 8 {
		t.Fatalf("relayedPeerCap(8) = %d, want 8", got)
	}
	if got := relayedPeerCap(200); got != 200 {
		t.Fatalf("relayedPeerCap(200) = %d, want 200", got)
	}
}

// registerRelayedPeerLocked must refuse a genuinely new remote once the
// relayed fan-out is at cap — counting only relayed entries, never direct
// peers — and count the refusal. A session migration for an already-present
// remote must NOT be refused.
func TestRegisterRelayedPeerEnforcesCapCountingOnlyRelayed(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxPeers = 2
	node := &Node{
		peers: map[string]*peerConn{
			// A direct peer: must not consume relayed cap.
			"via": {id: "via", outbound: true},
			// An existing relayed remote: occupies one slot.
			"relay-old": {id: "relay-old", relayed: true, relaySessionID: "sess-old"},
		},
		knownPeers: map[string]knownPeer{
			"via":         {id: "via", direct: true, noiseStatic: make([]byte, 32)},
			"relay-old":   {id: "relay-old", noiseStatic: make([]byte, 32)},
			"relay-new":   {id: "relay-new", noiseStatic: make([]byte, 32)},
			"relay-third": {id: "relay-third", noiseStatic: make([]byte, 32)},
		},
		scoring: gossip.NewEngine(),
		config:  cfg,
	}

	node.mu.Lock()
	defer node.mu.Unlock()

	// Second relayed remote (cap 2): allowed.
	peer, capped := node.registerRelayedPeerLocked(relayLocalSession{
		sessionID: "sess-new", viaPeerID: "via", remotePeerID: "relay-new",
	})
	if capped {
		t.Fatal("second relayed remote under a cap of 2 must not be capped")
	}
	if peer == nil {
		t.Fatal("expected a registered relayed peer")
	}

	// Third relayed remote: capped and counted.
	_, capped = node.registerRelayedPeerLocked(relayLocalSession{
		sessionID: "sess-third", viaPeerID: "via", remotePeerID: "relay-third",
	})
	if !capped {
		t.Fatal("third relayed remote past a cap of 2 must be reported capped")
	}
	if got := len(node.peers); got != 3 {
		t.Fatalf("n.peers must hold via + 2 relayed, got %d", got)
	}
	if v, ok := node.inboundByType.Load("__relay_peer_capped__"); !ok {
		t.Fatal("__relay_peer_capped__ counter not recorded")
	} else if v.(*atomic.Uint64).Load() != 1 {
		t.Fatal("capped attempt was not counted exactly once")
	}

	// Migration for an existing remote replaces, never refuses.
	peer, capped = node.registerRelayedPeerLocked(relayLocalSession{
		sessionID: "sess-migrated", viaPeerID: "via", remotePeerID: "relay-old",
	})
	if capped {
		t.Fatal("session migration of an existing relayed remote must never be capped")
	}
	if peer == nil || peer.relaySessionID != "sess-migrated" {
		t.Fatal("expected the migrated session to replace the old entry")
	}
}

// ---- Fix 3: restart hygiene ----

// Stop/Start must not wedge local delivery: queues from the previous run are
// bound to workers whose rootCtx is dead, and reusing them drops every
// message after the restart. This is the regression test for the Start()
// reset.
func TestPublishIsDeliveredAfterRestart(t *testing.T) {
	cfg := isolatedTestConfig("restart-delivery")
	cfg.GossipSub.HeartbeatMS = 50
	node, err := NewNode("mesh-restart-delivery", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}

	received := make(chan string, 8)
	node.SetMessageCallback(func(channel string, _ [32]byte, data []byte) {
		received <- string(data)
	})

	startAndWait := func() {
		if code := node.Start(); code != MOSS_OK {
			t.Fatalf("Start failed: %d", code)
		}
		if code := node.Subscribe("restart"); code != MOSS_OK {
			t.Fatalf("Subscribe failed: %d", code)
		}
	}
	publish := func(payload string) {
		// MOSS_ERR_NO_PEERS is fine here: local delivery (deliverLocal) is
		// what the assertion waits on, and it happens regardless of whether
		// any peer was there to fan out to.
		code := node.Publish("restart", []byte(payload))
		if code != MOSS_OK && code != MOSS_ERR_NO_PEERS {
			t.Fatalf("Publish %s failed: %d", payload, code)
		}
	}
	waitFor := func(want string) {
		deadline := time.After(5 * time.Second)
		for {
			select {
			case got := <-received:
				if got == want {
					return
				}
			case <-deadline:
				t.Fatalf("timed out waiting for %q after restart", want)
			}
		}
	}

	startAndWait()
	publish("before")
	waitFor("before")

	if code := node.Stop(); code != MOSS_OK {
		t.Fatalf("Stop failed: %d", code)
	}
	startAndWait()
	defer node.Stop()
	publish("after")
	waitFor("after")
}

// Stop must clear relay session state: a stale relayLocal keeps
// establishedRelaySession() non-empty so dialExplicitTarget never tries a
// direct dial again, and stale Acquire'd routes pin the supernode's session
// count at capacity. This asserts the maps and the limiter are all empty
// after Stop.
func TestStopClearsRelayState(t *testing.T) {
	cfg := isolatedTestConfig("stop-relay-state")
	cfg.GossipSub.HeartbeatMS = 50
	node, err := NewNode("mesh-stop-relay-state", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	if code := node.Start(); code != MOSS_OK {
		t.Fatalf("Start failed: %d", code)
	}

	node.mu.Lock()
	node.relayLocals["sess-local"] = relayLocalSession{
		sessionID: "sess-local", viaPeerID: "via", remotePeerID: "remote", established: true,
	}
	node.relayRoutes["sess-route"] = relayRoute{initiator: "a", target: "b"}
	node.relayBuckets["bucket"] = nat.NewTokenBucket(1024, 1024)
	node.suppress["suppressed"] = map[string]time.Time{"ch": time.Now()}
	node.mu.Unlock()
	if !node.relaySessions.Acquire("sess-route") {
		t.Fatal("expected to acquire a relay session slot")
	}

	if code := node.Stop(); code != MOSS_OK {
		t.Fatalf("Stop failed: %d", code)
	}

	node.mu.RLock()
	locals := len(node.relayLocals)
	routes := len(node.relayRoutes)
	buckets := len(node.relayBuckets)
	suppress := len(node.suppress)
	node.mu.RUnlock()
	if locals != 0 || routes != 0 || buckets != 0 || suppress != 0 {
		t.Fatalf("Stop left relay state behind: locals=%d routes=%d buckets=%d suppress=%d",
			locals, routes, buckets, suppress)
	}
	if got := node.relaySessions.Count(); got != 0 {
		t.Fatalf("Stop left %d phantom sessions in the relay limiter", got)
	}
	// A second Stop must stay idempotent-safe.
	if code := node.Stop(); code != MOSS_ERR_NOT_STARTED {
		t.Fatalf("second Stop returned %d, want MOSS_ERR_NOT_STARTED", code)
	}
}

// ---- Fix 4: supernode deadband ----

// supernodeReady must demote an active supernode only at the hard cap and
// re-promote a demoted one only below cap-minus-margin, so session counts
// hovering at RelayMaxSessions cannot flap a signed broadcast to every peer
// on each heartbeat. Caps of 10 or fewer keep the exact old boundary.
func TestSupernodeReadyDeadbandHoldsStateAroundCapacity(t *testing.T) {
	newNodeAtSessions := func(maxSessions int) *Node {
		cfg := isolatedTestConfig("supernode-deadband")
		cfg.GossipSub.HeartbeatMS = 50
		cfg.NAT.RelayMaxSessions = maxSessions
		node, err := NewNode("mesh-supernode-deadband", nil, cfg)
		if err != nil {
			t.Fatalf("NewNode failed: %v", err)
		}
		node.natProfile.Store(nat.Profile{
			Type:            nat.TypePublic,
			PublicReachable: true,
			ExternalAddress: "203.0.113.1:1",
		})
		node.startedAt = time.Now().Add(-time.Hour)
		return node
	}
	acquire := func(node *Node, ids ...string) {
		for _, id := range ids {
			if !node.relaySessions.Acquire(id) {
				t.Fatalf("failed to acquire session %s", id)
			}
		}
	}
	setActive := func(node *Node, active bool) {
		node.mu.Lock()
		node.supernodeActive = active
		node.mu.Unlock()
	}
	ready := func(node *Node) bool {
		return node.supernodeReady(node.natProfile.Load().(nat.Profile))
	}
	ids := func(n int) []string {
		out := make([]string, n)
		for i := range out {
			out[i] = strconv.Itoa(i)
		}
		return out
	}

	t.Run("large cap deadband", func(t *testing.T) {
		node := newNodeAtSessions(50)
		acquire(node, ids(49)...)
		setActive(node, true)
		if !ready(node) {
			t.Fatal("active supernode at 49/50 sessions must stay ready")
		}

		acquire(node, "cap-50")
		if ready(node) {
			t.Fatal("active supernode at 50/50 sessions must demote")
		}

		// Demoted at 49 — inside the deadband (re-promote only below 45):
		// stays demoted, no flip-flop.
		setActive(node, false)
		node.relaySessions.Release("cap-50")
		if ready(node) {
			t.Fatal("demoted supernode at 49/50 must stay demoted inside the deadband")
		}

		// Below the margin: may re-promote.
		for _, id := range ids(44) {
			node.relaySessions.Release(id)
		}
		if !ready(node) {
			t.Fatal("demoted supernode below the re-promote margin must become ready")
		}
	})

	t.Run("small cap keeps exact boundary", func(t *testing.T) {
		node := newNodeAtSessions(1)
		if !ready(node) {
			t.Fatal("inactive supernode with 0/1 sessions must be ready")
		}
		acquire(node, "only")
		if ready(node) {
			t.Fatal("supernode at 1/1 sessions must not be ready")
		}
		setActive(node, true)
		if ready(node) {
			t.Fatal("active supernode at 1/1 sessions must demote (margin 0)")
		}
	})
}

// ---- GrowthHardener wiring ----

// removePeer must evict the peer from the scoring engine: Score() recreates
// an evicted entry at zero on first use, so this only releases memory — but
// without it the engine grew one entry per peer ever connected and Tick
// walked them all forever. The SetOnRemove hook observes the eviction.
func TestRemovePeerEvictsFromScoringEngine(t *testing.T) {
	cfg := isolatedTestConfig("scoring-evict")
	cfg.GossipSub.HeartbeatMS = 50
	node, err := NewNode("mesh-scoring-evict", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}

	removed := make(chan string, 4)
	node.scoring.SetOnRemove(func(peerID string) {
		// Lock-light per the engine's contract: the hook runs under the
		// engine's own mutex, so only a buffered send here.
		removed <- peerID
	})
	// The hook only fires for a peer the engine actually holds.
	node.scoring.Ensure("doomed")

	node.mu.Lock()
	node.peers["doomed"] = &peerConn{id: "doomed", addr: "203.0.113.9:4001"}
	session := node.peers["doomed"].session
	node.mu.Unlock()

	node.removePeer("doomed", session)

	select {
	case id := <-removed:
		if id != "doomed" {
			t.Fatalf("scoring removed %q, want %q", id, "doomed")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("removePeer did not evict the peer from the scoring engine")
	}
}

// maintenanceLoop's conn-tick must run the known-peers sweep. The sweep
// self-throttles via knownPeersSwept; assert the timestamp advances after a
// maintenance window with a heartbeat fine enough to guarantee conn-ticks.
func TestMaintenanceLoopRunsKnownPeerSweep(t *testing.T) {
	cfg := isolatedTestConfig("sweep-wiring")
	cfg.GossipSub.HeartbeatMS = 50
	node, err := NewNode("mesh-sweep-wiring", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	if code := node.Start(); code != MOSS_OK {
		t.Fatalf("Start failed: %d", code)
	}
	defer node.Stop()

	node.mu.RLock()
	before := node.knownPeersSwept
	node.mu.RUnlock()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
		node.mu.RLock()
		after := node.knownPeersSwept
		node.mu.RUnlock()
		if after.After(before) {
			return // conn-tick ran sweepKnownPeers
		}
	}
	t.Fatal("knownPeersSwept never advanced: maintenanceLoop is not running the sweep on its conn-ticks")
}
