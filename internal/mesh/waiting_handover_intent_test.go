package mesh

import (
	"strings"
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/nat"
)

func TestWaitingCarrierHandoverPreservesEstablishedApplicationIntent(t *testing.T) {
	old, fresh := startCappedOverlayNode(t, "handover-core"), startCappedOverlayNode(t, "handover-core")
	origin, receiver := startCappedOverlayNode(t, "handover-leaf"), startCappedOverlayNode(t, "handover-leaf")
	for _, core := range []*Node{old, fresh} {
		waitingRelayFixture(t, origin, core)
	}
	waitingRelayFixture(t, receiver, old)
	receiver.SetActivityMode(ActivityWaiting)
	awaitActivity(t, receiver, "waiting", "ready")
	openUpgradeFixtureRelay(t, old, origin, receiver)
	assertWaitingBidirectionalPackets(t, origin, receiver)
	// A saved offline target must not become an additional migration barrier.
	if receiver.ConnectToPeer(strings.Repeat("a", 64)) != MOSS_OK {
		t.Fatal("offline explicit target was not registered")
	}
	receiver.handleEnvelope(receiver.peerByID(old.localPeerID()), fresh.signSupernodeEnvelope(gossip.Envelope{
		Type: gossip.TypeSupernodeAnnounce, AdvertisedPeerID: fresh.localPeerID(), AdvertisedAddr: nodeAddr(fresh),
		AdvertisedNATType: string(nat.TypePublic), AdvertisedReachable: true, AdvertisedRelayCapable: true,
	}))
	// Keep the old core and its stale provider record alive. Only the leaf's
	// carrier dies, removing its established virtual application peer first.
	receiver.peerByID(old.localPeerID()).closeSession()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		receiver.mu.RLock()
		ready := receiver.activity.attachment == fresh.localPeerID() && receiver.activity.reachability == "ready"
		peer := receiver.peers[origin.localPeerID()]
		preserved := peer != nil && peer.relayed && peer.viaPeerID == fresh.localPeerID()
		receiver.mu.RUnlock()
		if ready {
			if !preserved {
				t.Fatal("Ready after carrier loss discarded the previously established application path")
			}
			assertWaitingBidirectionalPackets(t, origin, receiver)
			if state := readActivity(t, receiver); state.Direct != 1 || state.Attachments != 1 {
				t.Fatalf("handover expanded waiting topology: %+v", state)
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("carrier handover did not restore the established application path")
}
