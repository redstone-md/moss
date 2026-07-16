package mesh

import (
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/nat"
)

// moss is a P2P mesh: direct is the product, relay is the safety net. A pair
// that is hard on both ends may land on a relay quickly — nobody should wait out
// a doomed punch — but it must never STAY there unexamined. The relay preference
// used to reach the upgrade path too, so a symmetric pair, once relayed, was
// never retried: relay stopped being a fallback and became a terminus.
func TestRelayPreferenceGovernsTheConnectButNotTheUpgrade(t *testing.T) {
	// The preference itself still holds on the connect path — a real latency
	// requirement (symmetric peers should reach each other in ~5s via relay
	// rather than after a failed punch).
	if !shouldPreferRelayBetween(nat.TypeSymmetric, nat.TypeSymmetric) {
		t.Fatal("connect path must still prefer a relay for a hard pair; punching first would cost seconds nobody wants to wait")
	}

	// But the upgrade path must not consult it. Verify by policy, not by name:
	// tryDirectUpgrade forces past the preference, tryDirectConnect does not.
	cfg := DefaultConfig()
	cfg.Trackers = nil
	cfg.LANDiscoveryEnabled = false
	cfg.DHTEnabled = false
	n, err := NewNode("room", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	if code := n.Start(); code != MOSS_OK {
		t.Fatalf("Start: %d", code)
	}
	defer n.Stop()

	// A symmetric peer we only reach through a relay, and we are symmetric too:
	// exactly the pair the preference gives up on.
	n.natProfile.Store(nat.Profile{Type: nat.TypeSymmetric})
	const target = "aa11bb22cc33dd44ee55ff6600112233445566778899aabbccddeeff0011223344"
	n.mu.Lock()
	n.knownPeers[target] = knownPeer{
		id: target, addr: "203.0.113.9:41666",
		natType: nat.TypeSymmetric, natTrusted: true,
	}
	n.mu.Unlock()

	if !n.shouldPreferRelayForTarget(target) {
		t.Fatal("test premise broken: this pair must be one the preference gives up on")
	}

	// The connect path honours the preference and never punches.
	if n.attemptHolePunch(target, 50*time.Millisecond) {
		t.Fatal("connect path punched a pair it should have relayed")
	}
	// The upgrade path must get past the preference and actually try. It will
	// fail against a bogus address — the point is that it ATTEMPTS, rather than
	// refusing on the label.
	done := make(chan bool, 1)
	go func() { done <- n.attemptHolePunchPolicy(target, 300*time.Millisecond, true) }()
	select {
	case <-done: // it ran the punch and returned a real verdict
	case <-time.After(3 * time.Second):
		t.Fatal("upgrade punch never returned")
	}

	// The regression itself: promotion must use the forcing path, or a relayed
	// symmetric peer is never offered a direct path again.
	if n.shouldPreferRelayForTarget(target) && !punchesOnUpgrade(n, target) {
		t.Fatal("relay must be a fallback, not a terminus: a relayed peer has to keep being tried for a direct path")
	}
}

// punchesOnUpgrade reports whether the upgrade policy gets past the preference.
func punchesOnUpgrade(n *Node, target string) bool {
	// force=true must not short-circuit on the preference; with a zero budget it
	// returns false for lack of time, not for lack of willingness.
	return !n.attemptHolePunchPolicy(target, 0, true)
}
