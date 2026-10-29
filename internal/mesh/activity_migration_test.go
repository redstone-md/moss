package mesh

import (
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/nat"
)

func TestActivityDirectLossDoesNotCancelPendingRelayMigration(t *testing.T) {
	core := startCappedOverlayNode(t, "pending-core")
	a := startCappedOverlayNode(t, "pending-leaf")
	b := startCappedOverlayNode(t, "pending-leaf")
	waitingRelayFixture(t, a, core)
	waitingRelayFixture(t, b, core)
	attachLeaf(t, a, b)
	const room, channel = "pending-room", "control"
	received := relayDeliverySink(t, room, channel, []byte("pending-room-key"), a, b)
	assertActivityTraffic(t, a, b, received, room, channel, "before")
	// Hold the real carrier's control handling while the leaf requests relay.
	// No protocol state or fake accept is installed by the test.
	core.mu.Lock()
	locked := true
	defer func() {
		if locked {
			core.mu.Unlock()
		}
	}()
	a.SetActivityMode(ActivityWaiting)
	deadline := time.Now().Add(time.Second)
	pending := false
	for time.Now().Before(deadline) && !pending {
		a.mu.RLock()
		for _, session := range a.relayLocals {
			pending = pending || (session.remotePeerID == b.localPeerID() && !session.established)
		}
		a.mu.RUnlock()
		if !pending {
			time.Sleep(time.Millisecond)
		}
	}
	if !pending {
		t.Fatal("leaf did not start a real relay request")
	}
	b.peerByID(a.localPeerID()).closeSession()
	deadline = time.Now().Add(time.Second)
	for a.directPeerConnected(b.localPeerID()) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	core.mu.Unlock()
	locked = false
	awaitActivity(t, a, "waiting", "ready")
	assertActivityTraffic(t, a, b, received, room, channel, "migrated")
}

func TestActivityWaitingMigrationDrainsOriginalDirectPackets(t *testing.T) {
	core := startCappedOverlayNode(t, "drain-core")
	a := startCappedOverlayNode(t, "drain-leaf")
	b := startCappedOverlayNode(t, "drain-leaf")
	waitingRelayFixture(t, a, core)
	waitingRelayFixture(t, b, core)
	attachLeaf(t, a, b)
	waitFor(t, func() bool { return b.directPeerConnected(a.localPeerID()) }, "remote direct admission did not finish")
	old := b.peerByID(a.localPeerID())
	if !b.sendEnvelope(old, gossip.Envelope{Type: gossip.TypePing, RequestID: "before-migration"}) {
		t.Fatal("original path was not usable before migration")
	}
	if err := a.SendToPeer(b.localPeerID(), []byte("establish-use"), time.Second); err != nil {
		t.Fatal(err)
	}
	received := make(chan string, 1)
	a.SetPacketCallback(func(_ [32]byte, data []byte) { received <- string(data) })
	if code := a.SetActivityMode(ActivityWaiting); code != MOSS_OK {
		t.Fatal(code)
	}
	waitFor(t, func() bool {
		peer := a.peerByID(b.localPeerID())
		return peer != nil && peer.relayed
	}, "waiting did not admit the relay replacement")
	// This is the packet the remote had already queued on the old encrypted
	// path when our relay accept arrived. It must still reach the application.
	if !b.sendEnvelope(old, gossip.Envelope{
		Type: gossip.TypeDirect, SenderID: b.identity.PublicKeyBytes(), Payload: []byte("in-flight"),
	}) {
		t.Fatal("relay admission immediately closed the original direct path")
	}
	select {
	case value := <-received:
		if value != "in-flight" {
			t.Fatal(value)
		}
	case <-time.After(time.Second):
		t.Fatal("migration dropped a packet already assigned to the original path")
	}
	awaitActivity(t, a, "waiting", "ready")
}

func TestActivityPendingMigrationKeepsDirectedPathForLaterHandover(t *testing.T) {
	first := startCappedOverlayNode(t, "direct-first")
	second := startCappedOverlayNode(t, "direct-second")
	a := startCappedOverlayNode(t, "direct-leaf")
	b := startCappedOverlayNode(t, "direct-leaf")
	waitingRelayFixture(t, a, first)
	waitingRelayFixture(t, b, first)
	waitingRelayFixture(t, b, second)
	attachLeaf(t, a, b)
	advertisement := second.signSupernodeEnvelope(gossip.Envelope{
		Type: gossip.TypeSupernodeAnnounce, AdvertisedPeerID: second.localPeerID(), AdvertisedAddr: nodeAddr(second),
		AdvertisedNATType: string(nat.TypePublic), AdvertisedReachable: true, AdvertisedRelayCapable: true,
	})
	a.handleEnvelope(a.peerByID(first.localPeerID()), advertisement)
	received := make(chan string, 4)
	a.SetPacketCallback(func(_ [32]byte, data []byte) { received <- string(data) })
	if err := b.SendToPeer(a.localPeerID(), []byte("before"), time.Second); err != nil {
		t.Fatal(err)
	}
	select {
	case <-received:
	case <-time.After(time.Second):
		t.Fatal("initial directed payload not received")
	}
	first.mu.Lock()
	locked := true
	defer func() {
		if locked {
			first.mu.Unlock()
		}
	}()
	a.SetActivityMode(ActivityWaiting)
	deadline := time.Now().Add(time.Second)
	pending := false
	for time.Now().Before(deadline) {
		a.mu.RLock()
		for _, session := range a.relayLocals {
			pending = pending || session.remotePeerID == b.localPeerID() && !session.established
		}
		a.mu.RUnlock()
		if pending {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !pending {
		t.Fatal("leaf did not start a pending relay migration")
	}
	b.peerByID(a.localPeerID()).closeSession()
	for a.directPeerConnected(b.localPeerID()) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	first.mu.Unlock()
	locked = false
	awaitActivity(t, a, "waiting", "ready")
	a.handleEnvelope(a.peerByID(first.localPeerID()), first.signSupernodeEnvelope(gossip.Envelope{
		Type: gossip.TypeSupernodeRevoke, AdvertisedPeerID: first.localPeerID(), AdvertisedAddr: nodeAddr(first),
		AdvertisedNATType: string(nat.TypePublic), AdvertisedReachable: true, AdvertisedRelayCapable: false,
	}))
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if ids := connectedDirectIDs(t, a); len(ids) == 1 && ids[0] == second.localPeerID() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	peer := b.peerByID(a.localPeerID())
	if peer == nil || !peer.relayed || peer.viaPeerID != second.localPeerID() {
		t.Fatalf("handover lost existing directed recipient path: %s", b.MeshInfoJSON())
	}
	if err := b.SendToPeer(a.localPeerID(), []byte("after"), time.Second); err != nil {
		t.Fatal(err)
	}
	select {
	case value := <-received:
		if value != "after" {
			t.Fatal(value)
		}
	case <-time.After(time.Second):
		t.Fatal("handover lost passive directed delivery")
	}
}
