package mesh

import (
	"context"
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/overlay"
)

// Seed an independent directory with B's genuine provider record, without
// supplying B's address/key to A or letting a shared relay connect them.
func identityOnlyRoom(t *testing.T) (*Node, *Node) {
	t.Helper()
	directory, attachment := startCappedOverlayNode(t, "index"), startCappedOverlayNode(t, "index")
	makeCore(directory)
	makeCore(attachment)
	attachment.mu.Lock()
	attachment.startedAt = time.Now().Add(-10 * time.Minute)
	attachment.mu.Unlock()
	a, b := startCappedOverlayNode(t, "private"), startCappedOverlayNode(t, "private")
	attachLeaf(t, a, directory)
	attachLeaf(t, b, attachment)
	for _, pair := range [][2]*Node{{a, b}, {b, a}} {
		if code := pair[0].SetRoomPeerDiscovery("", RoomDiscoveryKnownPeers, []string{pair[1].localPeerID()}); code != MOSS_OK {
			t.Fatal(code)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	b.republishOverlayRecords(ctx, overlaySeedTally{})
	id, _ := overlay.IDFromHex(b.localPeerID())
	for len(attachment.overlayStore.Get(id, time.Now())) == 0 {
		if ctx.Err() != nil {
			t.Fatal("fixture did not store the real target's identity record")
		}
		time.Sleep(10 * time.Millisecond)
	}
	for _, record := range attachment.overlayStore.Get(id, time.Now()) {
		directory.overlayStore.Put(id, record.Peer, record.Payload, time.Now())
	}
	a.mu.RLock()
	_, known := a.knownPeers[b.localPeerID()]
	a.mu.RUnlock()
	if known || a.peerByID(b.localPeerID()) != nil || a.peerByID(attachment.localPeerID()) != nil {
		t.Fatal("fixture leaked the target or its relay through peer exchange")
	}
	return a, b
}

func TestKnownRoomResolvesIdentityWithoutPeerCatalog(t *testing.T) {
	a, b := identityOnlyRoom(t)

	// Unconfigured consumers retain their original direct/relay-only attempt.
	if code := a.SetRoomPeerDiscovery("", RoomDiscoveryAutomatic, nil); code != MOSS_OK {
		t.Fatal(code)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	a.dialExplicitTarget(ctx, b.localPeerID())
	if a.peerByID(b.localPeerID()) != nil {
		t.Fatal("Automatic unexpectedly opted into identity resolution")
	}
	a.SetRoomPeerDiscovery("", RoomDiscoveryKnownPeers, []string{b.localPeerID()})
	stopped, stop := context.WithCancel(ctx)
	stop()
	a.dialExplicitTarget(stopped, b.localPeerID())
	if a.peerByID(b.localPeerID()) != nil {
		t.Fatal("cancelled attempt established a route")
	}
	received := roomReceiver(b)
	a.Subscribe("moves")
	b.Subscribe("moves")
	if code := a.ConnectToPeer(b.localPeerID()); code != MOSS_OK {
		t.Fatal(code)
	}
	awaitRoomDelivery(t, a, received, "identity-only")
}
