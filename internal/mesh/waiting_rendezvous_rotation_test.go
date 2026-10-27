package mesh

import (
	"context"
	"testing"
	"time"
)

func TestRendezvousRetainsUnusedOwnerUntilAuthenticatedApplicationProgress(t *testing.T) {
	old, fresh := startCappedOverlayNode(t, "rendezvous-rotation-old"), startCappedOverlayNode(t, "rendezvous-rotation-fresh")
	n, app := startCappedOverlayNode(t, "rendezvous-rotation-phone"), startCappedOverlayNode(t, "rendezvous-rotation-app")
	waitingRelayFixture(t, n, old)
	waitingRelayFixture(t, n, fresh)
	waitingRelayFixture(t, app, fresh)
	attachLeaf(t, app, n)
	waitFor(t, func() bool { return n.peerByID(app.localPeerID()) != nil && app.peerByID(n.localPeerID()) != nil }, "initial direct application admission did not complete")
	assertWaitingAlternativePackets(t, app, n)
	n.mu.Lock()
	n.activity.requested.Store(int32(ActivityWaiting))
	n.activity.candidate, n.activity.attempt = old.localPeerID(), 1
	n.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	n.retainWaitingRendezvous(ctx, old.localPeerID(), 1)
	owner := n.peerByID(old.localPeerID())
	n.mu.Lock()
	n.activity.candidate, n.activity.attempt = fresh.localPeerID(), 2
	n.mu.Unlock()
	n.retainWaitingRendezvous(ctx, fresh.localPeerID(), 2)
	n.mu.RLock()
	stable := n.activity.rendezvous == owner && !n.waitingCarrierCanSettleLocked(old.localPeerID()) && !n.waitingCarrierCanSettleLocked("")
	n.mu.RUnlock()
	if !stable {
		t.Fatal("unused alternative replaced owner or hid a useful direct path")
	}
	if !n.migrateWaitingPeers(ctx, fresh.localPeerID()) {
		t.Fatal("actual application path could not migrate")
	}
	n.retainWaitingRendezvous(ctx, fresh.localPeerID(), 2)
	n.awaitWaitingDrains()
	n.mu.RLock()
	rotated := n.activity.rendezvous == n.peers[fresh.localPeerID()] && n.peers[old.localPeerID()] == nil && n.directPeerCountLocked() == 1
	n.mu.RUnlock()
	if !rotated {
		t.Fatal("verified progress failed to replace and retire the unused old holder")
	}
	awaitRendezvousRelayPair(t, n, app)
	assertWaitingAlternativePackets(t, app, n)
}

func awaitRendezvousRelayPair(t *testing.T, a, b *Node) {
	t.Helper()
	deadline := time.Now().Add(a.config.HandshakeTimeout() + relayMigrationGracePeriod + 250*time.Millisecond)
	for {
		local, remote := a.peerByID(b.localPeerID()), b.peerByID(a.localPeerID())
		if local != nil && remote != nil && local.relayed && remote.relayed && local.relaySessionID == remote.relaySessionID {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("authenticated replacement did not finish direct drain on both endpoints")
		}
		time.Sleep(time.Millisecond)
	}
}
