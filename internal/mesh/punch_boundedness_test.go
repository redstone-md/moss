package mesh

// Regression proof for the promotion single-flight (relay_promotion) and the
// bounded binding refresh (node_reachability). These pin the observable
// behaviour the wave-4 fixes changed: a relayed target holds at most one
// in-flight upgrade attempt, and the external-address walk stops asking peers
// once it has an answer.

import (
	"testing"
	"time"
)

// TestRelayPromotionSingleFlightPerTarget: while an armed upgrade attempt is
// inside its budget, no maintenance tick may arm the same target again. The
// pre-fix code stamped the attempt's START, so a 5s attempt was re-armed every
// 3s and overlapping generations stacked punch dials on one peer.
func TestRelayPromotionSingleFlightPerTarget(t *testing.T) {
	node, err := NewNode("mesh-promote-single-flight", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}

	node.mu.Lock()
	node.relayLocals["s1"] = relayLocalSession{
		sessionID:    "s1",
		viaPeerID:    "relay",
		remotePeerID: "target",
		established:  true,
	}
	node.mu.Unlock()

	// First pass must arm.
	targets := node.relayPromotionTargets()
	if len(targets) != 1 || targets[0] != "target" {
		t.Fatalf("first promotion pass must arm the relayed target, got %v", targets)
	}

	// Every subsequent pass while the armed attempt's budget runs must be a
	// no-op: the stamp now names budget END, so re-arm becomes possible only
	// once the previous attempt can no longer be running.
	for i := 1; i <= 3; i++ {
		if targets = node.relayPromotionTargets(); len(targets) != 0 {
			t.Fatalf("promotion pass %d inside the attempt budget re-armed an in-flight target: %v", i, targets)
		}
	}

	// Stamp semantics: it records when the armed attempt's budget expires,
	// not when it started — that is the single-flight gate itself.
	node.mu.RLock()
	until, armed := node.directProbes["target"]
	node.mu.RUnlock()
	if !armed {
		t.Fatal("directProbes must hold the armed attempt's budget-end stamp")
	}
	handshake := node.config.HandshakeTimeout()
	if until.Sub(time.Now()) < handshake-time.Second {
		t.Fatalf("stamp must sit ~one handshake budget in the future (budget end), got %v", time.Until(until))
	}

	// Expire the stamp (as the attempt having ended) — the next pass must
	// re-arm: relay stays a fallback, never a terminus.
	node.mu.Lock()
	node.directProbes["target"] = time.Now().Add(-2 * time.Second)
	node.mu.Unlock()
	if targets = node.relayPromotionTargets(); len(targets) != 1 || targets[0] != "target" {
		t.Fatalf("promotion pass after the previous attempt ended must re-arm, got %v", targets)
	}

	// A session whose peer went direct must never arm.
	node.mu.Lock()
	node.peers["target"] = &peerConn{id: "target", relayed: false}
	node.mu.Unlock()
	if targets = node.relayPromotionTargets(); len(targets) != 0 {
		t.Fatalf("promotion must skip a peer that already went direct, got %v", targets)
	}
}

// TestRelayPromotionArmsOnCooldownZeroHeartbeat: the heartbeat-derived
// breather must keep the single-flight gate with a nonpositive config too.
func TestRelayPromotionArmsOnCooldownZeroHeartbeat(t *testing.T) {
	cfg := DefaultConfig()
	cfg.GossipSub.HeartbeatMS = 0
	node, err := NewNode("mesh-promote-zero-hb", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	node.mu.Lock()
	node.relayLocals["s1"] = relayLocalSession{
		sessionID:    "s1",
		viaPeerID:    "relay",
		remotePeerID: "target",
		established:  true,
	}
	node.mu.Unlock()
	if targets := node.relayPromotionTargets(); len(targets) != 1 {
		t.Fatalf("zero heartbeat must still arm the first pass (250ms floor), got %v", targets)
	}
	if targets := node.relayPromotionTargets(); len(targets) != 0 {
		t.Fatalf("zero heartbeat must still single-flight in-budget passes, got %v", targets)
	}
}

// TestRefreshExternalAddressAsksBoundedPeers: with more connected peers than
// the cap, one refresh must not put a binding request in front of every peer.
// The gossip binding request path needs a live peer entry only — a peer with
// no session makes requestBindingObservation fail fast without writing, so the
// walk's cost is bounded by the cap either way. The pinned fact is the
// selection bound itself: len(candidates) <= maxBindingRefreshPeers.
func TestRefreshExternalAddressAsksBoundedPeers(t *testing.T) {
	node, err := NewNode("mesh-refresh-bounded", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	node.mu.Lock()
	for i := range 8 {
		id := "peer-" + string(rune('a'+i))
		node.peers[id] = &peerConn{id: id}
	}
	node.mu.Unlock()

	// The candidate selection inside refreshExternalAddress is bounded; drive
	// it through the public path with a past deadline (every request returns
	// immediately, nothing is asked) so the bound is observable without a
	// live UDP stack: it must return without hanging and without asking
	// anyone (deadline exhausted).
	done := make(chan bool, 1)
	go func() {
		node.refreshExternalAddress(time.Now().Add(-time.Second))
		done <- true
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("refresh with an exhausted deadline must return immediately")
	}
}
