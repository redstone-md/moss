package mesh

// The heartbeat IHAVE sweep is the only recovery path for a subscriber
// that is not in the topic mesh: it learns a payload id exclusively from
// an IHAVE naming it. These tests pin the two properties the sweep's
// covering rotation (selectLazyPeersCovering) exists for:
//
//   - every non-mesh subscriber is targeted within ceil(N/DLazy) ticks,
//     no matter where it sits in the sorted list — the old hash sampling
//     covered any peer only probabilistically, and a hub-fan-out topology
//     (many subscribers, DLazy-wide announcements) left each payload a
//     lottery ticket per tick;
//   - a peer below the gossip threshold is never selected, but costs the
//     cover nothing — the rotation steps over ineligible peers without
//     letting them stall the pass.

import (
	"sort"
	"testing"

	"github.com/redstone-md/moss/internal/gossip"
)

func TestSelectLazyPeersCoveringCoversAllNonMeshSubscribers(t *testing.T) {
	node, err := NewNode("mesh-lazy-cover", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	node.pubsub.Subscribe("soak")
	// Hub fan-out: DLazy=6 announce width against 12 non-mesh subscribers
	// (18 total, 6 held in the mesh) — the shape of a 24-subscriber room
	// whose hub mesh holds six.
	peerIDs := make([]string, 0, 18)
	for i := 0; i < 18; i++ {
		id := "peer-" + string(rune('a'+i))
		peerIDs = append(peerIDs, id)
		node.pubsub.SetPeerSubscription(id, "soak", true)
	}
	for _, id := range peerIDs[:6] {
		node.pubsub.SetMeshPeer("soak", id, true)
	}

	nonMesh := len(peerIDs) - 6
	seen := make(map[string]int, nonMesh)
	dLazy := node.config.GossipSub.DLazy
	ticks := (nonMesh + dLazy - 1) / dLazy // ceil(12/6) = 2
	for tick := 0; tick < ticks; tick++ {
		selected := node.selectLazyPeersCovering("soak")
		if len(selected) > dLazy {
			t.Fatalf("tick %d selected %d peers, expected at most DLazy=%d", tick, len(selected), dLazy)
		}
		for _, id := range selected {
			seen[id]++
		}
	}
	if len(seen) != nonMesh {
		t.Fatalf("after %d ticks the sweep covered %d/%d subscribers — a subscriber can still miss every announcement for ids it never learns", ticks, len(seen), nonMesh)
	}
	for id, hits := range seen {
		if hits > 1 {
			t.Fatalf("peer %s was selected %d times before the pass completed — the rotation is drawing with replacement, not covering", id, hits)
		}
	}
}

func TestSelectLazyPeersCoveringExcludesMeshPeersAndBelowThreshold(t *testing.T) {
	node, err := NewNode("mesh-lazy-cover-eligibility", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	node.pubsub.Subscribe("soak")

	node.pubsub.SetPeerSubscription("in-mesh", "soak", true)
	node.pubsub.SetMeshPeer("soak", "in-mesh", true)
	node.pubsub.SetPeerSubscription("gated", "soak", true)
	node.pubsub.SetPeerSubscription("eligible", "soak", true)
	node.scoring.SetApplicationScore("gated", -11) // below GossipThreshold

	// Enough ticks to complete a pass; the ineligible peers must never
	// appear, and the eligible one must.
	sawEligible := false
	for tick := 0; tick < 4; tick++ {
		for _, id := range node.selectLazyPeersCovering("soak") {
			if id == "in-mesh" {
				t.Fatal("a mesh peer was targeted by the lazy sweep")
			}
			if id == "gated" {
				t.Fatal("a peer below the gossip threshold was targeted by the lazy sweep")
			}
			if id == "eligible" {
				sawEligible = true
			}
		}
	}
	if !sawEligible {
		t.Fatal("the one eligible non-mesh subscriber was never targeted across a full pass")
	}
}

func TestSelectLazyPeersCoveringSurvivesShrinkingPeerSet(t *testing.T) {
	// A cursor pointing past the end of a shrunken non-mesh list must
	// re-base, not skip the whole pass: subscribers leaving (mesh grafts,
	// disconnects) change N mid-pass and the cover has to keep working
	// without waiting for the cursor to wrap.
	node, err := NewNode("mesh-lazy-cover-shrink", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	node.pubsub.Subscribe("soak")
	node.pubsub.SetPeerSubscription("a", "soak", true)
	node.pubsub.SetPeerSubscription("b", "soak", true)

	if got := len(node.selectLazyPeersCovering("soak")); got != 2 {
		t.Fatalf("expected the full 2-subscriber list selected, got %d", got)
	}

	// All subscribers leave; the sweep goes quiet rather than panicking or
	// inventing peers, and a returning set is covered again from scratch.
	node.pubsub.SetPeerSubscription("a", "soak", false)
	node.pubsub.SetPeerSubscription("b", "soak", false)
	if got := node.selectLazyPeersCovering("soak"); got != nil {
		t.Fatalf("expected no targets with no non-mesh subscribers, got %v", got)
	}
	node.pubsub.SetPeerSubscription("c", "soak", true)
	node.pubsub.SetPeerSubscription("d", "soak", true)
	node.pubsub.SetPeerSubscription("e", "soak", true)
	seen := map[string]bool{}
	for tick := 0; tick < 3; tick++ {
		for _, id := range node.selectLazyPeersCovering("soak") {
			seen[id] = true
		}
	}
	if len(seen) != 3 {
		t.Fatalf("a pass after churn covered %d/3 subscribers", len(seen))
	}
	// selectLazyPeers (publish-side) keeps its own contract untouched:
	// hash-sampled, capped, eligibility-gated.
	sampled := node.selectLazyPeers("soak", "", 2)
	if len(sampled) > 2 {
		t.Fatalf("publish-side selection grew past its cap: %v", sampled)
	}
	sort.Strings(sampled)
	for _, id := range sampled {
		if id != "c" && id != "d" && id != "e" {
			t.Fatalf("publish-side selection returned a non-subscriber: %s", id)
		}
	}
	_ = gossip.TypeIHave
}
