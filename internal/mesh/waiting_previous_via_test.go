package mesh

import (
	"context"
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/nat"
)

func TestOrdinaryLostCarrierReusesVerifiedViaWithoutOverlayProvider(t *testing.T) {
	core, other, origin, phone := ordinaryPreviousViaFixture(t)
	lost := origin.peerByID(core.localPeerID())
	lost.closeSession()
	deadline := time.Now().Add(time.Second)
	for origin.peerByID(core.localPeerID()) == lost {
		if time.Now().After(deadline) {
			t.Fatal("origin carrier did not retire")
		}
		time.Sleep(time.Millisecond)
	}
	if p := phone.peerByID(core.localPeerID()); p == nil || p.relayed {
		t.Fatal("recipient's actual retained carrier disappeared")
	}
	key := mustOverlayID(t, phone.localPeerID())
	if len(other.overlayStore.Get(key, time.Now())) != 0 {
		t.Fatal("unrelated connected core unexpectedly has target provider")
	}
	if len(core.overlayStore.Get(key, time.Now())) == 0 {
		t.Fatal("retained recipient carrier has no current authenticated presence")
	}
	if origin.ConnectToPeer(phone.localPeerID()) != MOSS_OK {
		t.Fatal("owned explicit recovery request failed")
	}
	deadline = time.Now().Add(500 * time.Millisecond)
	for {
		a, b := origin.peerByID(phone.localPeerID()), phone.peerByID(origin.localPeerID())
		if a != nil && b != nil && a.relayed && b.relayed && a.viaPeerID == core.localPeerID() && a.relaySessionID == b.relaySessionID {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("ordinary recovery ignored the live verified previous carrier when connected cores had no providers")
		}
		time.Sleep(time.Millisecond)
	}
	assertWaitingBidirectionalPackets(t, origin, phone)
	if state := readActivity(t, phone); state.Mode != "waiting" || state.Direct != 1 || state.Attachments != 1 {
		t.Fatal("ordinary recovery expanded recipient waiting topology")
	}
}

func ordinaryPreviousViaFixture(t *testing.T) (core, other, origin, phone *Node) {
	t.Helper()
	core, other = startCappedOverlayNode(t, "previous-via-core"), startCappedOverlayNode(t, "previous-via-other")
	origin, phone = startCappedOverlayNode(t, "previous-via-origin"), startCappedOverlayNode(t, "previous-via-phone")
	other.AllowPeer(origin.localPeerID())
	waitingRelayFixture(t, origin, core)
	waitingRelayFixture(t, origin, other)
	waitingRelayFixture(t, phone, core)
	phone.SetActivityMode(ActivityWaiting)
	awaitActivity(t, phone, "waiting", "ready")
	openUpgradeFixtureRelay(t, core, origin, phone)
	assertWaitingBidirectionalPackets(t, origin, phone)
	if origin.ConnectToPeer(phone.localPeerID()) != MOSS_OK {
		t.Fatal("explicit target registration failed")
	}
	origin.mu.RLock()
	ordinary := !origin.activity.optedIn
	origin.mu.RUnlock()
	if !ordinary {
		t.Fatal("ordinary sender was opted into waiting lifecycle")
	}
	return
}

func TestLostPreviousCarrierAllowsFreshAuthenticatedReplacement(t *testing.T) {
	core, other, origin, phone := ordinaryPreviousViaFixture(t)
	other.AllowPeer(phone.localPeerID())
	phone.handleEnvelope(phone.peerByID(core.localPeerID()), other.signSupernodeEnvelope(gossip.Envelope{
		Type: gossip.TypeSupernodeAnnounce, AdvertisedPeerID: other.localPeerID(), AdvertisedAddr: nodeAddr(other),
		AdvertisedNATType: string(nat.TypePublic), AdvertisedReachable: true, AdvertisedRelayCapable: true,
	}))
	if core.Stop() != MOSS_OK {
		t.Fatal("previous core did not stop")
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		a, b := origin.peerByID(phone.localPeerID()), phone.peerByID(origin.localPeerID())
		if a != nil && b != nil && a.relayed && b.relayed && a.viaPeerID == other.localPeerID() && a.relaySessionID == b.relaySessionID {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("dead previous carrier trapped a fresh authenticated replacement")
		}
		time.Sleep(time.Millisecond)
	}
	origin.mu.RLock()
	remembered := origin.explicitTargets[phone.localPeerID()].recoveryVia
	origin.mu.RUnlock()
	if remembered != "" {
		t.Fatal("fresh actual admission did not consume old carrier hint")
	}
	assertWaitingBidirectionalPackets(t, origin, phone)
	if state := readActivity(t, phone); state.Mode != "waiting" || state.Direct != 1 || state.Attachments != 1 {
		t.Fatal("fresh replacement expanded waiting recipient topology")
	}
}

func TestDeadPreviousCarrierCannotRestoreOrConsumeOldRoute(t *testing.T) {
	core, _, origin, phone := ordinaryPreviousViaFixture(t)
	if core.Stop() != MOSS_OK {
		t.Fatal("previous core did not stop")
	}
	deadline := time.Now().Add(time.Second)
	for origin.peerByID(core.localPeerID()) != nil {
		if time.Now().After(deadline) {
			t.Fatal("previous physical carrier did not retire")
		}
		time.Sleep(time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if origin.reachPreviousRelayCarrier(ctx, phone.localPeerID()) {
		t.Fatal("dead former carrier falsely restored an application route")
	}
	origin.mu.RLock()
	hint := origin.explicitTargets[phone.localPeerID()].recoveryVia
	origin.mu.RUnlock()
	if hint != core.localPeerID() {
		t.Fatal("refusal consumed application-owned recovery before authenticated replacement")
	}
	if origin.peerByID(phone.localPeerID()) != nil {
		t.Fatal("dead former carrier resurrected an old virtual session")
	}
}
