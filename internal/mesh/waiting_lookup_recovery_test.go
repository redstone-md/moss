package mesh

import (
	"context"
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/overlay"
)

func TestWaitingIdentityLookupAsksConnectedCarrierOutsideNearestK(t *testing.T) {
	carrier := startCappedOverlayNode(t, "lookup-carrier")
	receiver := startCappedOverlayNode(t, "lookup-leaf")
	waitingRelayFixture(t, receiver, carrier)
	receiver.SetActivityMode(ActivityWaiting)
	awaitActivity(t, receiver, "waiting", "ready")
	key := mustOverlayID(t, receiver.localPeerID())
	until := time.Now().Add(time.Second)
	for len(carrier.overlayStore.Get(key, time.Now())) == 0 {
		if time.Now().After(until) {
			t.Fatal("waiting provider did not reach its carrier")
		}
		time.Sleep(time.Millisecond)
	}
	core := mustOverlayID(t, carrier.localPeerID())
	var origin *Node
	for attempt := 0; attempt < 64; attempt++ {
		candidate := startCappedOverlayNode(t, "lookup-leaf")
		self := mustOverlayID(t, candidate.localPeerID())
		if overlay.BucketIndex(self, key) != overlay.BucketIndex(self, core) {
			origin = candidate
			break
		}
		candidate.Stop()
	}
	if origin == nil {
		t.Fatal("could not isolate decoys from carrier bucket")
	}
	waitingRelayFixture(t, origin, carrier)
	for _, decoy := range decoyContacts(key, overlay.DefaultK) {
		origin.overlayTable.Add(decoy)
	}
	for _, contact := range origin.overlayTable.Closest(key, overlay.DefaultK) {
		if contact.ID == core {
			t.Fatal("fixture did not bury connected carrier outside closest K")
		}
	}
	if len(origin.overlayReachableContacts(key)) == 0 {
		t.Fatal("fixture lost connected carrier")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	providers, _ := origin.overlayLookup(ctx, key, true)
	for _, provider := range providers {
		if string(provider.Peer) == string(key[:]) {
			return
		}
	}
	t.Fatal("waiting identity was invisible although its authenticated carrier was connected and held its fresh record")
}
