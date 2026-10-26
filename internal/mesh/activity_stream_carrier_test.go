package mesh

import (
	"context"
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/nat"
)

func waitingUDPRelayFixture(t *testing.T, leaf, relay *Node) {
	t.Helper()
	makeCore(relay)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := leaf.connectPeerUDPWithHint(ctx, relay.localPeerID(), nodeAddr(relay)); err != nil {
		t.Fatal(err)
	}
	leaf.handleEnvelope(leaf.peerByID(relay.localPeerID()), relay.signSupernodeEnvelope(gossip.Envelope{
		Type: gossip.TypeSupernodeAnnounce, AdvertisedPeerID: relay.localPeerID(), AdvertisedAddr: nodeAddr(relay),
		AdvertisedNATType: string(nat.TypePublic), AdvertisedReachable: true, AdvertisedRelayCapable: true,
	}))
	peer := leaf.peerByID(relay.localPeerID())
	if peer == nil || isStreamNetwork(peer.session.RemoteAddr()) {
		t.Fatal("fixture needs an actual UDP carrier")
	}
}

func TestActivityWaitingPrefersAnExistingStreamCarrier(t *testing.T) {
	first := startCappedOverlayNode(t, "stream-carrier-first")
	second := startCappedOverlayNode(t, "stream-carrier-second")
	if nodeAddr(first) > nodeAddr(second) {
		first, second = second, first
	}
	leaf := startCappedOverlayNode(t, "stream-carrier-leaf")
	waitingUDPRelayFixture(t, leaf, first)
	waitingRelayFixture(t, leaf, second)
	leaf.SetActivityMode(ActivityWaiting)
	awaitActivity(t, leaf, "waiting", "ready")
	leaf.mu.RLock()
	carrier := leaf.activity.attachment
	leaf.mu.RUnlock()
	if carrier != second.localPeerID() {
		t.Fatalf("selected datagram carrier despite an authenticated stream alternative: %s", leaf.MeshInfoJSON())
	}
}

func TestActivityWaitingUpgradesItsUDPAttachmentToAStream(t *testing.T) {
	core := startCappedOverlayNode(t, "upgrade-carrier")
	leaf := startCappedOverlayNode(t, "upgrade-leaf")
	waitingUDPRelayFixture(t, leaf, core)
	leaf.SetActivityMode(ActivityWaiting)
	awaitActivity(t, leaf, "waiting", "ready")
	peer := leaf.peerByID(core.localPeerID())
	if peer == nil || !isStreamNetwork(peer.session.RemoteAddr()) {
		t.Fatal("waiting retained a datagram carrier without trying the available stream")
	}
}
