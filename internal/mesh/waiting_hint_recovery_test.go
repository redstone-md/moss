package mesh

import (
	"context"
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/nat"
	"github.com/redstone-md/moss/internal/overlay"
)

func TestOrdinaryRecoveryUsesAuthenticatedInboundCarrierHint(t *testing.T) {
	for _, opted := range []bool{false, true} {
		name := "ordinary"
		if opted {
			name = "opted-control"
		}
		t.Run(name, func(t *testing.T) {
			carrier, origin, receiver := inboundCarrierRecoveryFixture(t)
			if opted {
				origin.SetActivityMode(ActivityActive)
			}
			if !opted && origin.peerIDByAddr(nodeAddr(carrier), receiver.localPeerID()) != "" {
				t.Fatal("ordinary healthy target unexpectedly enabled advertised aliases")
			}
			origin.ConnectToPeer(receiver.localPeerID())
			peer := origin.peerByID(receiver.localPeerID())
			origin.mu.RLock()
			session := origin.relayLocals[peer.relaySessionID]
			origin.mu.RUnlock()
			origin.closeRelaySession(session)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			route, err := origin.resolveRouteContext(ctx, receiver.localPeerID())
			if err != nil || !route.Relayed || route.ViaPeerID != carrier.localPeerID() {
				t.Fatalf("identity recovery could not reuse its authenticated inbound carrier: opted=%t err=%v", opted, err)
			}
			assertWaitingBidirectionalPackets(t, origin, receiver)
		})
	}
}

func inboundCarrierRecoveryFixture(t *testing.T) (*Node, *Node, *Node) {
	t.Helper()
	first, second := startCappedOverlayNode(t, "inbound-hint"), startCappedOverlayNode(t, "inbound-hint")
	carrier, origin := first, second
	// Both duplicate selectors keep the existing carrier→origin direction.
	if carrier.localPeerID() > origin.localPeerID() {
		carrier, origin = origin, carrier
	}
	makeCore(carrier)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := carrier.connectPeer(ctx, nodeAddr(origin)); err != nil {
		t.Fatal(err)
	}
	until := time.Now().Add(time.Second)
	for origin.peerByID(carrier.localPeerID()) == nil {
		if time.Now().After(until) {
			t.Fatal("inbound carrier did not authenticate")
		}
		time.Sleep(time.Millisecond)
	}
	origin.handleEnvelope(origin.peerByID(carrier.localPeerID()), carrier.signSupernodeEnvelope(gossip.Envelope{
		Type: gossip.TypeSupernodeAnnounce, AdvertisedPeerID: carrier.localPeerID(), AdvertisedAddr: nodeAddr(carrier),
		AdvertisedNATType: string(nat.TypePublic), AdvertisedReachable: true, AdvertisedRelayCapable: true,
	}))
	origin.overlaySeedFromKnownPeers()
	origin.mu.RLock()
	peer, info := origin.peers[carrier.localPeerID()], origin.knownPeers[carrier.localPeerID()]
	ordinary := !origin.activity.optedIn
	origin.mu.RUnlock()
	if !ordinary || peer.outbound || peer.addr == nodeAddr(carrier) || !info.verified || info.addr != nodeAddr(carrier) {
		t.Fatal("fixture needs ordinary inbound carrier with authenticated advertised/ephemeral address mismatch")
	}
	receiver := startCappedOverlayNode(t, "inbound-hint")
	waitingRelayFixture(t, receiver, carrier)
	receiver.SetActivityMode(ActivityWaiting)
	awaitActivity(t, receiver, "waiting", "ready")
	id, _ := overlay.IDFromHex(receiver.localPeerID())
	until = time.Now().Add(time.Second)
	for len(carrier.overlayStore.Get(id, time.Now())) == 0 {
		if time.Now().After(until) {
			t.Fatal("real waiting identity record did not reach carrier")
		}
		time.Sleep(time.Millisecond)
	}
	openUpgradeFixtureRelay(t, carrier, origin, receiver)
	assertWaitingBidirectionalPackets(t, origin, receiver)
	return carrier, origin, receiver
}
