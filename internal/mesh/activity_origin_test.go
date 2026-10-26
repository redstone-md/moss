package mesh

import (
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/nat"
	"github.com/redstone-md/moss/internal/overlay"
)

const waitingOriginRoom = "waiting-origin-room"
const waitingOriginChannel = "control"

func waitingRecipient(t *testing.T, maxPeers ...int) (first, second, receiver *Node) {
	t.Helper()
	first = startCappedOverlayNode(t, "origin-first")
	second = startCappedOverlayNode(t, "origin-second")
	waitingRelayFixture(t, first, second)
	waitingRelayFixture(t, second, first)
	limit := 1
	if len(maxPeers) > 0 {
		limit = maxPeers[0]
	}
	receiver = startCappedOverlayNode(t, "origin-leaf", limit)
	waitingRelayFixture(t, receiver, second)
	if receiver.JoinRoom(waitingOriginRoom, []byte("waiting-origin-key")) != MOSS_OK ||
		receiver.SubscribeRoom(waitingOriginRoom, waitingOriginChannel) != MOSS_OK {
		t.Fatal("receiver room subscription failed")
	}
	if code := receiver.SetActivityMode(ActivityWaiting); code != MOSS_OK {
		t.Fatal(code)
	}
	awaitActivity(t, receiver, "waiting", "ready")
	return
}

func activeOrigin(t *testing.T, first, receiver *Node, known bool) (*Node, <-chan string) {
	t.Helper()
	origin := startCappedOverlayNode(t, "origin-leaf")
	waitingRelayFixture(t, origin, first)
	if code := origin.SetActivityMode(ActivityActive); code != MOSS_OK {
		t.Fatal(code)
	}
	received := relayDeliverySink(t, waitingOriginRoom, waitingOriginChannel, []byte("waiting-origin-key"), origin, receiver)
	if known {
		if code := origin.SetRoomPeerDiscovery(waitingOriginRoom, RoomDiscoveryKnownPeers, []string{receiver.localPeerID()}); code != MOSS_OK {
			t.Fatal(code)
		}
		if code := origin.ConnectToPeer(receiver.localPeerID()); code != MOSS_OK {
			t.Fatal(code)
		}
	}
	return origin, received
}

func awaitWaitingRecipientRoute(t *testing.T, origin, receiver *Node, cores ...*Node) {
	t.Helper()
	started := time.Now()
	for time.Since(started) < 25*time.Second {
		if peer := origin.peerByID(receiver.localPeerID()); peer != nil && peer.relayed {
			t.Logf("natural foreground route established in%s", time.Since(started))
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	origin.mu.RLock()
	if target := origin.explicitTargets[receiver.localPeerID()]; target != nil {
		t.Logf("explicit retry running=%t identity=%t failures=%d last=%s", target.running, target.identityRetry, target.failures, target.lastAttempt)
	}
	origin.mu.RUnlock()
	key, _ := overlay.IDFromHex(receiver.localPeerID())
	for _, core := range cores {
		t.Logf("carrier info=%s providers=%v", core.MeshInfoJSON(), core.overlayStore.Get(key, time.Now()))
	}
	t.Fatalf("recipient ID/room did not establish route; origin=%s receiver=%s", origin.MeshInfoJSON(), receiver.MeshInfoJSON())
}

func TestActivityActiveOriginFindsWaitingRecipientFromIdentityOnly(t *testing.T) {
	first, second, receiver := waitingRecipient(t)
	origin, received := activeOrigin(t, first, receiver, true)
	awaitWaitingRecipientRoute(t, origin, receiver, first, second)
	assertActivityTraffic(t, origin, receiver, received, waitingOriginRoom, waitingOriginChannel, "identity")
	if state := readActivity(t, receiver); state.Mode != "waiting" || state.Direct != 1 || state.Attachments != 1 {
		t.Fatalf("waiting receiver expanded topology: %+v", state)
	}
}

func TestActivityWaitingPresenceSurvivesTTLForFreshIdentityAndRoomOrigins(t *testing.T) {
	// Two distinct origins require two existing per-sender dispatch slots;
	// the waiting policy still keeps exactly one physical carrier.
	first, _, receiver := waitingRecipient(t, 2)
	deadline := time.NewTimer(overlay.DefaultRecordTTL + 5*time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
wait:
	for {
		select {
		case <-deadline.C:
			break wait
		case <-ticker.C:
			if state := readActivity(t, receiver); state.Direct != 1 || state.Attachments != 1 {
				t.Fatalf("waiting topology grew while idle: %+v", state)
			}
		}
	}
	// New origins receive only the ordinary bootstrap carrier address. There
	// is no test-created remote-carrier connection, record, route or refresh.
	for _, known := range []bool{true, false} {
		origin, received := activeOrigin(t, first, receiver, known)
		if known {
			awaitWaitingRecipientRoute(t, origin, receiver)
			assertActivityTraffic(t, origin, receiver, received, waitingOriginRoom, waitingOriginChannel, "recipient-id")
		} else {
			// Room gossip may form through another authenticated subscriber;
			// a direct virtual edge to this specific peer is not required.
			assertActivityRoomTraffic(t, origin, receiver, received, waitingOriginRoom, waitingOriginChannel, "room-only", 25*time.Second)
		}
	}
	if state := readActivity(t, receiver); state.Mode != "waiting" || state.Direct != 1 || state.Attachments != 1 {
		t.Fatalf("cold-origin reply expanded waiting topology: %+v", state)
	}
}

func TestActivityFreshRoomOriginAfterIdentityOrigin(t *testing.T) {
	first, second, receiver := waitingRecipient(t, 2)
	origin, received := activeOrigin(t, first, receiver, true)
	awaitWaitingRecipientRoute(t, origin, receiver, first, second)
	assertActivityTraffic(t, origin, receiver, received, waitingOriginRoom, waitingOriginChannel, "first-origin")
	roomOrigin, roomReceived := activeOrigin(t, first, receiver, false)
	t.Cleanup(func() {
		if t.Failed() {
			for label, node := range map[string]*Node{"first-origin": origin, "room-origin": roomOrigin, "receiver": receiver} {
				topic := node.roomTopicIn(waitingOriginRoom, waitingOriginChannel)
				node.mu.RLock()
				peers := len(node.peers)
				node.mu.RUnlock()
				t.Logf("%s peers=%d subscribed=%d mesh=%d", label, peers, len(node.pubsub.Subscribers(topic)), len(node.pubsub.MeshPeers(topic)))
			}
		}
	})
	assertActivityRoomTraffic(t, roomOrigin, receiver, roomReceived, waitingOriginRoom, waitingOriginChannel, "room-origin", 25*time.Second)
}

func TestActivityOptInKeepsLiveDirectStartupPath(t *testing.T) {
	for _, opted := range []bool{false, true} {
		core := startCappedOverlayNode(t, "startup-core")
		receiver := startCappedOverlayNode(t, "startup-leaf")
		waitingRelayFixture(t, receiver, core)
		origin := startCappedOverlayNode(t, "startup-leaf")
		waitingRelayFixture(t, origin, core)
		if opted {
			origin.SetActivityMode(ActivityActive)
		}
		started := time.Now()
		if code := origin.ConnectToPeer(receiver.localPeerID()); code != MOSS_OK {
			t.Fatal(code)
		}
		for !origin.directPeerConnected(receiver.localPeerID()) && time.Since(started) < 3*time.Second {
			time.Sleep(time.Millisecond)
		}
		if !origin.directPeerConnected(receiver.localPeerID()) {
			t.Fatalf("live direct path was delayed; optedIn=%t elapsed=%s", opted, time.Since(started))
		}
		t.Logf("live direct startup optedIn=%t elapsed=%s", opted, time.Since(started))
	}
}

func TestActivityForegroundReusesInboundCarrierByAuthenticatedListenAddress(t *testing.T) {
	_, carrier, receiver := waitingRecipient(t)
	origin := startCappedOverlayNode(t, "inbound-origin")
	origin.SetActivityMode(ActivityActive)
	// The network initiates this physical connection toward the origin. Its
	// authenticated peer.addr is an ephemeral source port, not the listen hint.
	attachLeaf(t, carrier, origin)
	carrier.sendEnvelope(carrier.peerByID(origin.localPeerID()), carrier.signSupernodeEnvelope(gossip.Envelope{
		Type: gossip.TypeSupernodeAnnounce, AdvertisedPeerID: carrier.localPeerID(), AdvertisedAddr: nodeAddr(carrier),
		AdvertisedNATType: string(nat.TypePublic), AdvertisedReachable: true, AdvertisedRelayCapable: true,
	}))
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		origin.mu.RLock()
		info := origin.knownPeers[carrier.localPeerID()]
		origin.mu.RUnlock()
		if info.addr == nodeAddr(carrier) && info.natTrusted {
			break
		}
		time.Sleep(time.Millisecond)
	}
	peer := origin.peerByID(carrier.localPeerID())
	if peer == nil || peer.addr == nodeAddr(carrier) {
		t.Fatal("fixture did not establish a distinct inbound source port")
	}
	route, err := origin.ResolveRoute(receiver.localPeerID())
	if err != nil || !route.Relayed || route.ViaPeerID != carrier.localPeerID() {
		origin.mu.RLock()
		t.Logf("known carrier=%+v", origin.knownPeers[carrier.localPeerID()])
		origin.mu.RUnlock()
		key, _ := overlay.IDFromHex(receiver.localPeerID())
		t.Logf("origin=%s carrier=%s providers=%v", origin.MeshInfoJSON(), carrier.MeshInfoJSON(), carrier.overlayStore.Get(key, time.Now()))
		t.Fatalf("authenticated inbound carrier was not reusable: route=%+v error=%v", route, err)
	}
}
