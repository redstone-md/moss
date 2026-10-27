package mesh

import (
	"context"
	"testing"
	"time"
)

// A waiting node announces that it is retiring a direct carrier. Its peer must
// leave that carrier while it still drains, so no payload is written into a
// connection the waiting node is about to close.
func TestWaitingDirectRetirementMovesPeerOffCarrierBeforeClose(t *testing.T) {
	carrier := startCappedOverlayNode(t, "direct-retirement-core")
	phone, origin := startCappedOverlayNode(t, "direct-retirement-phone"), startCappedOverlayNode(t, "direct-retirement-origin")
	waitingRelayFixture(t, phone, carrier)
	waitingRelayFixture(t, origin, carrier)
	attachLeaf(t, origin, phone)
	assertWaitingBidirectionalPackets(t, origin, phone)
	if p := origin.peerByID(phone.localPeerID()); p == nil || p.relayed {
		t.Fatal("fixture origin is not directly connected to the phone")
	}
	phone.activity.requested.Store(int32(ActivityWaiting))
	sessionID, err := phone.openRelaySession(context.Background(), carrier.localPeerID(), origin.localPeerID(), phone.config.HandshakeTimeout())
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(relayMigrationGracePeriod / 2)
	for {
		if p := origin.peerByID(phone.localPeerID()); p != nil && p.relayed && p.relaySessionID == sessionID {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("peer kept writing to the direct carrier the waiting node is retiring")
		}
		time.Sleep(time.Millisecond)
	}
	assertWaitingAlternativePackets(t, origin, phone)
}
