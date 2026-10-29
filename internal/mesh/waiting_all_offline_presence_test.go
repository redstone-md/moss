package mesh

import (
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/nat"
)

func TestWaitingAllOfflineTargetsKeepOwnRendezvousCarrier(t *testing.T) {
	old, fresh := startCappedOverlayNode(t, "all-offline-old"), startCappedOverlayNode(t, "all-offline-fresh")
	phone, offline := startCappedOverlayNode(t, "all-offline-phone"), startCappedOverlayNode(t, "all-offline-old-contact")
	origin := startCappedOverlayNode(t, "all-offline-new-origin")
	waitingRelayFixture(t, phone, old)
	waitingRelayFixture(t, offline, old)
	makeCore(fresh)
	phone.SetActivityMode(ActivityWaiting)
	awaitActivity(t, phone, "waiting", "ready")
	openUpgradeFixtureRelay(t, old, offline, phone)
	assertWaitingBidirectionalPackets(t, offline, phone)
	phone.handleEnvelope(phone.peerByID(old.localPeerID()), fresh.signSupernodeEnvelope(gossip.Envelope{
		Type: gossip.TypeSupernodeAnnounce, AdvertisedPeerID: fresh.localPeerID(), AdvertisedAddr: nodeAddr(fresh),
		AdvertisedNATType: string(nat.TypePublic), AdvertisedReachable: true, AdvertisedRelayCapable: true,
	}))
	old.Stop()
	deadline := time.Now().Add(time.Second)
	for {
		phone.mu.RLock()
		retained := phone.activity.recovering[offline.localPeerID()] != nil
		phone.mu.RUnlock()
		if retained {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("carrier loss did not retain the established offline target")
		}
		time.Sleep(time.Millisecond)
	}
	offline.Stop()
	_ = ownPartialPresenceExpiry(t, fresh, phone)
	waitingRelayFixture(t, origin, fresh)
	route, err := origin.ResolveRoute(phone.localPeerID())
	if err != nil || !route.Relayed || route.ViaPeerID != fresh.localPeerID() {
		t.Fatal("all-offline prior contacts prevented a new origin reaching this installation")
	}
	assertWaitingBidirectionalPackets(t, origin, phone)
	awaitActivity(t, phone, "waiting", "ready")
	phone.mu.RLock()
	pending, physical := phone.activity.recovering[offline.localPeerID()] != nil, phone.directPeerCountLocked()
	ready := phone.activity.reachability == "ready"
	phone.mu.RUnlock()
	if !pending || !ready || physical != 1 {
		t.Fatal("rendezvous availability consumed offline intent, claimed completion or grew physical mesh")
	}
}
