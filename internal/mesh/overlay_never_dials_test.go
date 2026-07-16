package mesh

import (
	"context"
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/overlay"
)

// The overlay must never dial a peer.
//
// It looked free — every contact is publicly reachable by construction, so a
// dial "should" succeed. In production it was not free: clients opened ~1.7
// sessions per second with 95% dying instantly as duplicates the dedup closed
// on arrival, and players felt that storm of handshakes as multi-second stalls
// mid-game. Lookups run on paths that repeat per peer and per tick, so whatever
// one does is multiplied by the entire known-peer set.
//
// A node's core peers are the ones it is already attached to, so the contacts
// worth asking are on hand anyway.
func TestOverlayQueryNeverDialsAnUnconnectedContact(t *testing.T) {
	n := startOverlayNode(t, "room")

	// A contact we hold no session with, at an address that would take a full
	// dial timeout to fail — if the query dials, this test takes seconds.
	contact := overlay.Contact{
		ID:   overlay.NodeID{0xAB, 0xCD},
		Addr: "203.0.113.250:41666", // TEST-NET-3: never answers
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	started := time.Now()
	_, err := n.overlayQuery(ctx, contact, gossip.Envelope{Type: gossip.TypeOverlayFindValue})
	elapsed := time.Since(started)

	if err == nil {
		t.Fatal("querying a contact we have no session with must fail, not dial it")
	}
	if elapsed > time.Second {
		t.Fatalf("overlayQuery took %v — it dialed. A lookup runs per peer per tick; "+
			"dialing there manufactures the session storm players feel as lag", elapsed)
	}

	// The one-way send must hold the same line.
	started = time.Now()
	if err := n.overlaySend(ctx, contact, gossip.Envelope{Type: gossip.TypeOverlayFindValue}); err == nil {
		t.Fatal("overlaySend must not dial an unconnected contact either")
	}
	if elapsed = time.Since(started); elapsed > time.Second {
		t.Fatalf("overlaySend took %v — it dialed", elapsed)
	}
}
