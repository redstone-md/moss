package mesh

import (
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/nat"
)

// backoffTestNode builds a node holding one known, dialable, disconnected peer —
// the exact shape discoveredPeerTargets is meant to select.
func backoffTestNode(t *testing.T) *Node {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Trackers = nil
	node, err := NewNode("mesh-dial-backoff", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	node.mu.Lock()
	node.knownPeers["peer-1"] = knownPeer{
		id:              "peer-1",
		addr:            "185.242.25.75:24598",
		natType:         nat.TypeRestrictedCone,
		publicReachable: false,
		verified:        true,
	}
	node.mu.Unlock()
	return node
}

func targeted(targets []discoveredPeerTarget, peerID string) bool {
	for _, target := range targets {
		if target.peerID == peerID {
			return true
		}
	}
	return false
}

// A peer that just failed must not be redialled on the very next maintenance
// tick. This is the spin that produced 723 failed attempts at ~9.8s each — 81%
// of every connect attempt the fleet made — because the cooldown entry was
// deleted the instant the attempt returned and never spaced anything out.
func TestFailedDialIsSpacedOutInsteadOfRetriedImmediately(t *testing.T) {
	node := backoffTestNode(t)

	if !targeted(node.discoveredPeerTargets(), "peer-1") {
		t.Fatal("a fresh known peer should be dialled")
	}
	node.noteDialOutcome("peer-1", false)

	if targeted(node.discoveredPeerTargets(), "peer-1") {
		t.Fatal("a peer that just failed was redialled immediately: the spin is back")
	}
}

// Backing off must never become giving up. "No path now" is not "no path ever" —
// NATs rebind, the far end restarts, a relay joins — so the interval is capped
// and the peer keeps being retried at that interval forever.
func TestDialBackoffGrowsAndIsCapped(t *testing.T) {
	base := 10 * time.Second
	if got := peerDialBackoff(base, 0); got != base {
		t.Fatalf("no failures should dial at the base interval: got %v want %v", got, base)
	}
	if got := peerDialBackoff(base, 1); got != 20*time.Second {
		t.Fatalf("one failure: got %v want 20s", got)
	}
	if got := peerDialBackoff(base, 3); got != 80*time.Second {
		t.Fatalf("three failures: got %v want 80s", got)
	}
	if got := peerDialBackoff(base, 200); got != peerDialBackoffMax {
		t.Fatalf("a peer failing forever must still be retried at the cap: got %v want %v", got, peerDialBackoffMax)
	}
	if peerDialBackoff(base, 200) <= 0 {
		t.Fatal("backoff overflowed to a non-positive duration: every peer would be dialled every tick")
	}
}

// Regression guard. A peer that connects and later drops must be redialled at
// once — its backoff history is stale the moment a path proves to exist. Getting
// this wrong is how a reconnect silently turns into a minutes-long outage, and
// it is the failure this whole change is one step away from.
func TestSuccessfulDialClearsBackoffSoAReconnectIsImmediate(t *testing.T) {
	node := backoffTestNode(t)

	for i := 0; i < 5; i++ {
		node.noteDialOutcome("peer-1", false)
	}
	node.mu.RLock()
	failures := node.peerDialFailures["peer-1"]
	node.mu.RUnlock()
	if failures != 5 {
		t.Fatalf("consecutive failures should accumulate: got %d want 5", failures)
	}

	node.noteDialOutcome("peer-1", true)

	node.mu.RLock()
	failures = node.peerDialFailures["peer-1"]
	node.mu.RUnlock()
	if failures != 0 {
		t.Fatalf("a success must reset the backoff: got %d failures", failures)
	}
	if !targeted(node.discoveredPeerTargets(), "peer-1") {
		t.Fatal("a peer that dropped after a successful dial must be redialled immediately")
	}
}

// A relayed session IS a path. Counting it as failure would back off precisely
// the peers that depend on the relay, which is the opposite of the intent.
func TestRelayedOutcomeDoesNotCountAsFailure(t *testing.T) {
	node := backoffTestNode(t)

	node.noteDialOutcome("peer-1", false)
	node.noteDialOutcome("peer-1", true) // relayed reports success

	node.mu.RLock()
	_, penalised := node.peerDialFailures["peer-1"]
	node.mu.RUnlock()
	if penalised {
		t.Fatal("reaching a peer through a relay was recorded as a failure")
	}
}
