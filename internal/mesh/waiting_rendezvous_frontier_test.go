package mesh

import (
	"context"
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/nat"
)

func TestReadyUsefulRendezvousAdvancesRecoveryToAnotherKnownCarrier(t *testing.T) {
	f := waitingPartialPresenceFixture(t)
	awaitActivity(t, f.phone, "waiting", "ready")
	next := startCappedOverlayNode(t, "ready-pending-new-carrier")
	waitingRelayFixture(t, f.live, next)
	// The retained holder still serves A but cannot admit Q. Only the known
	// alternative can perform a fresh authenticated relay exchange with Q.
	f.fresh.AllowPeer(f.phone.localPeerID())
	f.fresh.AllowPeer(f.live.localPeerID())
	f.phone.handleEnvelope(f.phone.peerByID(f.fresh.localPeerID()), next.signSupernodeEnvelope(gossip.Envelope{
		Type: gossip.TypeSupernodeAnnounce, AdvertisedPeerID: next.localPeerID(), AdvertisedAddr: nodeAddr(next),
		AdvertisedNATType: string(nat.TypePublic), AdvertisedReachable: true, AdvertisedRelayCapable: true,
	}))
	if f.offline.Start() != MOSS_OK {
		t.Fatal("offline peer could not restart")
	}
	waitingRelayFixture(t, f.offline, next)
	f.offline.SetActivityMode(ActivityWaiting)
	awaitActivity(t, f.offline, "waiting", "ready")
	deadline := time.Now().Add(10 * time.Second)
	for {
		f.phone.mu.RLock()
		recovered := f.phone.activity.recovering[f.offline.localPeerID()] == nil
		settled := f.phone.activity.attachment == next.localPeerID() && f.phone.activity.reachability == "ready" && f.phone.directPeerCountLocked() == 1
		bounded := f.phone.directPeerCountLocked() <= 2
		f.phone.mu.RUnlock()
		if !bounded {
			t.Fatal("pending recovery exceeded the two-carrier bound")
		}
		if recovered && settled {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("useful settled holder exhausted every retry before the known alternative")
		}
		time.Sleep(time.Millisecond)
	}
	awaitRendezvousRelayPair(t, f.phone, f.live)
	awaitRendezvousRelayPair(t, f.phone, f.offline)
	assertWaitingAlternativePackets(t, f.live, f.phone)
	assertWaitingAlternativePackets(t, f.offline, f.phone)
}

func TestIncompleteRendezvousCannotBeRetiredToAdmitThirdCarrier(t *testing.T) {
	owner, next := startCappedOverlayNode(t, "incomplete-owner"), startCappedOverlayNode(t, "incomplete-next")
	n, app := startCappedOverlayNode(t, "incomplete-phone"), startCappedOverlayNode(t, "incomplete-application")
	waitingRelayFixture(t, n, owner)
	attachLeaf(t, app, n)
	waitFor(t, func() bool { return n.peerByID(app.localPeerID()) != nil && app.peerByID(n.localPeerID()) != nil }, "initial direct application admission did not complete")
	assertWaitingAlternativePackets(t, app, n)
	n.mu.Lock()
	n.activity.requested.Store(int32(ActivityWaiting))
	n.activity.candidate, n.activity.attempt = owner.localPeerID(), 1
	n.mu.Unlock()
	n.retainWaitingRendezvous(context.Background(), owner.localPeerID(), 1)
	current := n.peerByID(owner.localPeerID())
	n.mu.Lock()
	n.activity.candidate, n.activity.attempt = next.localPeerID(), 2
	n.mu.Unlock()
	if n.retireUnusedWaitingCarriers(next.localPeerID(), 2) || n.peerByID(owner.localPeerID()) != current || n.peerByID(app.localPeerID()) == nil {
		t.Fatal("unused cleanup sacrificed the current verified rendezvous before replacement admission")
	}
	n.mu.RLock()
	bounded := n.directPeerCountLocked() == 2 && n.activity.rendezvous == current
	n.mu.RUnlock()
	if !bounded {
		t.Fatal("incomplete transition lost exact holder ownership or two-peer bound")
	}
	assertWaitingAlternativePackets(t, app, n)
	// The same exact owner loses protection if its current signed capability
	// is revoked; this guard must not retain an unusable slot indefinitely.
	n.mu.Lock()
	info := n.knownPeers[owner.localPeerID()]
	info.relayCapable = false
	n.knownPeers[owner.localPeerID()] = info
	n.mu.Unlock()
	if !n.retireUnusedWaitingCarriers(next.localPeerID(), 2) || n.peerByID(owner.localPeerID()) != nil {
		t.Fatal("revoked owner retained an unused admission slot")
	}
	assertWaitingAlternativePackets(t, app, n)
}
