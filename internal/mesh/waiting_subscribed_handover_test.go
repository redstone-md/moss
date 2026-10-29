package mesh

import (
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/nat"
)

func TestWaitingSubscribedPathSurvivesTwoCarrierLossesBeforeNewClaim(t *testing.T) {
	old, fresh := startCappedOverlayNode(t, "sub-handover-core"), startCappedOverlayNode(t, "sub-handover-core")
	origin, receiver := startCappedOverlayNode(t, "sub-handover-leaf"), startCappedOverlayNode(t, "sub-handover-leaf")
	for _, core := range []*Node{old, fresh} {
		waitingRelayFixture(t, origin, core)
	}
	waitingRelayFixture(t, receiver, old)
	receiver.SetActivityMode(ActivityWaiting)
	awaitActivity(t, receiver, "waiting", "ready")
	openUpgradeFixtureRelay(t, old, origin, receiver)
	if receiver.peerByID(origin.localPeerID()).applicationUsed.Load() {
		t.Fatal("fixture needs a subscribed path before any directed application packet")
	}
	// Subscription interest is independent of application traffic. A fresh
	// remote claim may be delayed after teardown removes the old incarnation.
	receiver.pubsub.Subscribe("subscribed-before-payload")
	receiver.pubsub.SetPeerSubscription(origin.localPeerID(), "subscribed-before-payload", true)
	receiver.handleEnvelope(receiver.peerByID(old.localPeerID()), fresh.signSupernodeEnvelope(gossip.Envelope{
		Type: gossip.TypeSupernodeAnnounce, AdvertisedPeerID: fresh.localPeerID(), AdvertisedAddr: nodeAddr(fresh),
		AdvertisedNATType: string(nat.TypePublic), AdvertisedReachable: true, AdvertisedRelayCapable: true,
	}))
	for _, pair := range [][2]*Node{{old, fresh}, {fresh, old}} {
		receiver.peerByID(pair[0].localPeerID()).closeSession()
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			receiver.mu.RLock()
			ready := receiver.activity.attachment == pair[1].localPeerID() && receiver.activity.reachability == "ready"
			peer := receiver.peers[origin.localPeerID()]
			preserved := peer != nil && peer.relayed && peer.viaPeerID == pair[1].localPeerID()
			receiver.mu.RUnlock()
			if ready {
				if !preserved {
					t.Fatal("second carrier handover discarded restored subscription interest before a new claim")
				}
				break
			}
			time.Sleep(time.Millisecond)
		}
		if receiver.peerByID(origin.localPeerID()) == nil {
			t.Fatal("subscribed application route did not survive handover")
		}
	}
	assertWaitingBidirectionalPackets(t, origin, receiver)
}

func TestWaitingLossOfPartiallyMigratedCarrierRetainsEstablishedApplication(t *testing.T) {
	n, err := NewNode("partial-carrier-loss", nil, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	n.started = true
	n.activity.requested.Store(int32(ActivityWaiting))
	n.activity.attachment = "old-lost-carrier"
	carrier := &peerConn{id: "partial-carrier", connectedAt: time.Now()}
	app := &peerConn{id: "application", relayed: true, viaPeerID: carrier.id, relaySessionID: "partial-session"}
	app.applicationUsed.Store(true)
	n.peers[carrier.id], n.peers[app.id] = carrier, app
	n.relayLocals[app.relaySessionID] = relayLocalSession{sessionID: app.relaySessionID, viaPeerID: carrier.id, remotePeerID: app.id, established: true}
	// A failed later target leaves this useful candidate alive while the
	// committed attachment still names the previously lost carrier.
	n.removePeer(carrier.id, nil)
	if n.peers[app.id] != nil {
		t.Fatal("fixture did not remove virtual peer with its physical carrier")
	}
	if n.waitingMigrationTargets("next-carrier")[app.id] != app {
		t.Fatal("loss of a retained partial carrier erased its established application intent")
	}
}
