package mesh

import (
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/nat"
	"github.com/redstone-md/moss/internal/overlay"
)

type partialPresenceFixture struct {
	phone, live, origin, fresh, offline *Node
	room, channel                       string
}

func TestWaitingPartialCarrierAdvertisesOwnPresenceDespiteOfflineTarget(t *testing.T) {
	f := waitingPartialPresenceFixture(t)
	phone, live, origin, fresh, offline := f.phone, f.live, f.origin, f.fresh, f.offline
	room, channel := f.room, f.channel
	identity := mustOverlayID(t, phone.localPeerID())
	topic := overlay.ChannelKey(phone.roomTopicIn(room, channel))
	deadline := time.Now().Add(3 * time.Second)
	for len(fresh.overlayStore.Get(identity, time.Now())) == 0 || len(fresh.overlayStore.Get(topic, time.Now())) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("offline retained target blocked own identity/room presence on the useful authenticated carrier")
		}
		time.Sleep(time.Millisecond)
	}
	for _, key := range []overlay.NodeID{identity, topic} {
		for _, entry := range fresh.overlayStore.Get(key, time.Now()) {
			if entry.Peer != identity {
				t.Fatal("partial publication claimed another provider identity")
			}
			hint, valid := decodeHint(entry.Payload)
			if !valid || len(hint.Attachments) != 1 || hint.Attachments[0] != nodeAddr(fresh) {
				t.Fatal("partial publication advertised an unretained physical carrier")
			}
		}
	}
	waitingRelayFixture(t, origin, fresh)
	if origin.peerByID(phone.localPeerID()) != nil {
		t.Fatal("new origin unexpectedly had a prior application route")
	}
	route, err := origin.ResolveRoute(phone.localPeerID())
	if err != nil || !route.Relayed || route.ViaPeerID != fresh.localPeerID() {
		t.Fatal("new origin could not discover the useful partial carrier through own presence")
	}
	assertWaitingBidirectionalPackets(t, origin, phone)
	assertWaitingBidirectionalPackets(t, live, phone)
	awaitActivity(t, phone, "waiting", "ready")
	phone.mu.RLock()
	pending, physical := phone.activity.recovering[offline.localPeerID()] != nil, phone.directPeerCountLocked()
	ready := phone.activity.reachability == "ready"
	phone.mu.RUnlock()
	if !pending || !ready || physical != 1 {
		t.Fatal("presence publication changed recovery ownership, Ready or physical bounds")
	}
}

func waitingPartialPresenceFixture(t *testing.T) partialPresenceFixture {
	return waitingPartialPresenceFixtureOrder(t, false)
}

func waitingPartialPresenceFixtureOrder(t *testing.T, offlineFirst bool) partialPresenceFixture {
	t.Helper()
	old, fresh := startCappedOverlayNode(t, "partial-presence-old"), startCappedOverlayNode(t, "partial-presence-fresh")
	phone := startCappedOverlayNode(t, "partial-presence-phone")
	origin := startCappedOverlayNode(t, "partial-presence-new-origin")
	live, offline := startCappedOverlayNode(t, "partial-presence-app"), startCappedOverlayNode(t, "partial-presence-app")
	if (live.localPeerID() > offline.localPeerID()) != offlineFirst {
		live, offline = offline, live
	}
	for _, app := range []*Node{live, offline} {
		waitingRelayFixture(t, app, old)
	}
	waitingRelayFixture(t, live, fresh)
	waitingRelayFixture(t, phone, old)
	const room, channel = "partial-presence-room", "messages"
	if phone.JoinRoom(room, []byte("partial-presence-key")) != MOSS_OK || phone.SubscribeRoom(room, channel) != MOSS_OK {
		t.Fatal("room subscription failed")
	}
	phone.SetActivityMode(ActivityWaiting)
	awaitActivity(t, phone, "waiting", "ready")
	for _, app := range []*Node{live, offline} {
		openUpgradeFixtureRelay(t, old, app, phone)
		assertWaitingBidirectionalPackets(t, app, phone)
	}
	phone.handleEnvelope(phone.peerByID(old.localPeerID()), fresh.signSupernodeEnvelope(gossip.Envelope{
		Type: gossip.TypeSupernodeAnnounce, AdvertisedPeerID: fresh.localPeerID(), AdvertisedAddr: nodeAddr(fresh),
		AdvertisedNATType: string(nat.TypePublic), AdvertisedReachable: true, AdvertisedRelayCapable: true,
	}))
	if old.Stop() != MOSS_OK {
		t.Fatal("old carrier did not stop")
	}
	deadline := time.Now().Add(time.Second)
	for {
		phone.mu.RLock()
		retained := phone.activity.recovering[offline.localPeerID()] != nil
		phone.mu.RUnlock()
		if retained {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("real carrier loss did not retain the established target")
		}
		time.Sleep(time.Millisecond)
	}
	offline.Stop()
	deadline = time.Now().Add(4 * time.Second)
	for {
		phone.mu.RLock()
		app := phone.peers[live.localPeerID()]
		partial := app != nil && app.relayed && app.viaPeerID == fresh.localPeerID()
		phone.mu.RUnlock()
		if partial {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("live application path did not partially migrate")
		}
		time.Sleep(time.Millisecond)
	}
	assertWaitingBidirectionalPackets(t, live, phone)
	return partialPresenceFixture{phone: phone, live: live, origin: origin, fresh: fresh, offline: offline, room: room, channel: channel}
}

func TestWaitingOfflineFirstTargetDoesNotBlockLiveMigration(t *testing.T) {
	f := waitingPartialPresenceFixtureOrder(t, true)
	assertWaitingBidirectionalPackets(t, f.live, f.phone)
	_ = ownPartialPresenceExpiry(t, f.fresh, f.phone)
	awaitActivity(t, f.phone, "waiting", "ready")
}
