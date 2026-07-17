package mesh

import (
	"context"
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/overlay"
)

// decoyContacts returns contacts sitting right on top of key in the keyspace —
// so they sort ahead of everything else — that no session exists with.
//
// This is not a contrived shape, it is the production one: the table is filled
// from peer exchange with every publicly reachable node the substrate gossips,
// ordered purely by XOR distance, while the overlay can only speak to the two or
// three of them this node actually holds a session with. Which contacts land
// nearest a given key is effectively random, so the nearest are usually ones we
// cannot ask.
func mustOverlayID(t *testing.T, hexID string) overlay.NodeID {
	t.Helper()
	id, ok := overlay.IDFromHex(hexID)
	if !ok {
		t.Fatalf("bad overlay id: %s", hexID)
	}
	return id
}

func decoyContacts(key overlay.NodeID, count int) []overlay.Contact {
	decoys := make([]overlay.Contact, 0, count)
	for i := 0; i < count; i++ {
		id := key
		id[overlay.IDLen-1] ^= byte(i) // distance 0,1,2... — nearer than any real node
		decoys = append(decoys, overlay.Contact{
			ID:       id,
			Addr:     "203.0.113.250:41666", // TEST-NET-3: never answers
			LastSeen: time.Now(),
		})
	}
	return decoys
}

// A lookup must ask the contacts it can actually reach — not spend its whole
// budget on the nearest ones and quit.
//
// Two bugs made every rendezvous on the fleet return found=0, all 205 of them,
// while the record sat safely on a core node the whole time:
//
//   - the batch took the alpha nearest contacts without regard to whether a
//     session existed, so unreachable decoys consumed every slot; and
//   - the round ended as soon as it learned no new contacts, so after those
//     alpha failures the lookup gave up rather than asking anyone else.
//
// Together they meant a node queried three contacts out of six and reported the
// channel empty. This test holds that line: the record is reachable, and the
// nearest contacts are not.
func TestLookupReachesPastUnreachableNearestContacts(t *testing.T) {
	core := startOverlayNode(t, "room")
	makeCore(core)
	a := startOverlayNode(t, "room")
	b := startOverlayNode(t, "room")

	attachLeaf(t, a, core)
	attachLeaf(t, b, core)

	if code := a.Subscribe("sparse-channel"); code != MOSS_OK {
		t.Fatalf("a.Subscribe: %d", code)
	}
	if code := b.Subscribe("sparse-channel"); code != MOSS_OK {
		t.Fatalf("b.Subscribe: %d", code)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// A's record lands on the core it shares with B.
	if stored := a.republishOverlayRecords(ctx); stored == 0 {
		t.Fatal("A stored its record nowhere: a record nobody holds can never be found")
	}

	// Now bury the core behind nearer contacts B cannot talk to.
	topic := b.roomTopic("sparse-channel")
	key := overlay.ChannelKey(topic)
	for _, decoy := range decoyContacts(key, overlayAlpha) {
		b.overlayTable.Add(decoy)
	}

	// STORE is one-way and unacknowledged, so A's publish returning says only
	// that it was SENT — the core may not have processed it yet. Looking up
	// immediately raced it, and the test failed one run in four: my own proof was
	// part luck. Retry the way anything real does.
	started := time.Now()
	var providers []gossip.OverlayProvider
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); {
		if providers, _ = b.overlayLookup(ctx, key, true); len(providers) > 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	elapsed := time.Since(started)

	if len(providers) == 0 {
		t.Fatal("lookup found nobody: it spent its budget on the nearest contacts " +
			"it cannot reach and never asked the core actually holding the record — " +
			"this is every rendezvous the fleet ran returning found=0")
	}
	// Unreachable contacts must cost nothing: they fail without a session rather
	// than being waited on. Sequential alpha queries against dead contacts are
	// what made a lookup take 12s at p95 and 20s at worst.
	if elapsed > 5*time.Second {
		t.Fatalf("lookup took %v — unreachable contacts are still being waited on", elapsed)
	}
}

// Publishing must land somewhere reachable, and say how many nodes took it.
// stored=0 is the difference between "the room is empty" and "this layer has
// never once worked" — and both read as found=0 from a lookup.
func TestPublishReportsWhereItLanded(t *testing.T) {
	core := startOverlayNode(t, "room")
	makeCore(core)
	a := startOverlayNode(t, "room")
	attachLeaf(t, a, core)

	if code := a.Subscribe("sparse-channel"); code != MOSS_OK {
		t.Fatalf("a.Subscribe: %d", code)
	}

	// Nearest contacts A holds no session with must not swallow the publish.
	// Keep this below a bucket's capacity: a full bucket evicts by least-recently
	// seen and would drop the core itself, which tests the setup rather than the
	// code.
	topic := a.roomTopic("sparse-channel")
	for _, decoy := range decoyContacts(overlay.ChannelKey(topic), overlayAlpha) {
		a.overlayTable.Add(decoy)
	}
	// Precondition: the core must still be a contact, or this proves nothing.
	if !a.overlayCanReach(overlay.Contact{ID: mustOverlayID(t, core.localPeerID())}) {
		t.Fatal("test precondition broken: A holds no session with the core")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if stored := a.republishOverlayRecords(ctx); stored == 0 {
		t.Fatal("publish stored at zero nodes while a reachable core was attached")
	}
}
