package mesh

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/nat"
	"github.com/redstone-md/moss/internal/overlay"
)

func TestOrdinaryRecoveryFindsFreshCarrierPastStaleProvider(t *testing.T) {
	staleWaitingProviderRecovery(t, false)
}

func TestOrdinaryRecoveryKeepsRouteBudgetWithSilentOverlayCore(t *testing.T) {
	staleWaitingProviderRecovery(t, true)
}

func staleWaitingProviderRecovery(t *testing.T, silent bool) {
	origin, receiver, old, fresh, cores := staleWaitingProviderFixture(t)
	if silent {
		// This authenticated core retains its socket but never answers our queries.
		cores[1].SetScoringCallback(func(_ [32]byte, _ float64) float64 { return gossip.GraylistThreshold - 1 })
	}
	key := mustOverlayID(t, receiver.localPeerID())
	if len(old.overlayStore.Get(key, time.Now())) == 0 || len(fresh.overlayStore.Get(key, time.Now())) == 0 {
		t.Fatal("fixture needs both old and fresh authenticated provider records")
	}
	oldPeer := origin.peerByID(receiver.localPeerID())
	if oldPeer != nil {
		origin.applyPruneLists(expiredVirtualProbe(t, origin, oldPeer))
	}
	origin.mu.RLock()
	ordinary := !origin.activity.optedIn
	retry := origin.explicitTargets[receiver.localPeerID()].identityRetry
	origin.mu.RUnlock()
	if !ordinary || !retry {
		t.Fatal("fixture needs ordinary lost-relay identity recovery")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	route, err := origin.resolveRouteContext(ctx, receiver.localPeerID())
	if err != nil || !route.Relayed || route.ViaPeerID != fresh.localPeerID() {
		t.Fatalf("stale closest provider hid a fresh waiting path: route=%+v err=%v", route, err)
	}
	assertWaitingBidirectionalPackets(t, origin, receiver)
	if state := readActivity(t, receiver); state.Mode != "waiting" || state.Direct != 1 || state.Attachments != 1 {
		t.Fatalf("identity recovery expanded waiting topology: %+v", state)
	}
}

func staleWaitingProviderFixture(t *testing.T) (origin, receiver, old, fresh *Node, cores []*Node) {
	t.Helper()
	receiver = startCappedOverlayNode(t, "stale-provider-leaf")
	key := mustOverlayID(t, receiver.localPeerID())
	cores = orderedWaitingCores(t, key)
	old, fresh = cores[0], cores[3]
	origin = startCappedOverlayNode(t, "stale-provider-leaf")
	for _, core := range cores {
		waitingRelayFixture(t, origin, core)
	}
	for _, node := range []*Node{origin, old, fresh} {
		receiver.AllowPeer(node.localPeerID())
	}
	waitingRelayFixture(t, receiver, old)
	receiver.SetActivityMode(ActivityWaiting)
	awaitActivity(t, receiver, "waiting", "ready")
	openUpgradeFixtureRelay(t, old, origin, receiver)
	assertWaitingBidirectionalPackets(t, origin, receiver)
	origin.ConnectToPeer(receiver.localPeerID())
	receiver.handleEnvelope(receiver.peerByID(old.localPeerID()), fresh.signSupernodeEnvelope(gossip.Envelope{
		Type: gossip.TypeSupernodeAnnounce, AdvertisedPeerID: fresh.localPeerID(), AdvertisedAddr: nodeAddr(fresh),
		AdvertisedNATType: string(nat.TypePublic), AdvertisedReachable: true, AdvertisedRelayCapable: true,
	}))
	// Only the leaf's old attachment is lost; the old core and its TTL record live.
	peer := receiver.peerByID(old.localPeerID())
	peer.closeSession()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		receiver.mu.RLock()
		ready := receiver.activity.attachment == fresh.localPeerID() && receiver.activity.reachability == "ready"
		receiver.mu.RUnlock()
		if ready && len(fresh.overlayStore.Get(key, time.Now())) > 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("receiver did not hand over to fresh carrier with one physical attachment")
	return
}

func orderedWaitingCores(t *testing.T, key overlay.NodeID) []*Node {
	t.Helper()
	cores := make([]*Node, 4)
	for i := range cores {
		cores[i] = startCappedOverlayNode(t, "stale-provider-core")
		makeCore(cores[i])
	}
	sort.Slice(cores, func(i, j int) bool {
		return overlay.Closer(key, mustOverlayID(t, cores[i].localPeerID()), mustOverlayID(t, cores[j].localPeerID()))
	})
	return cores
}
