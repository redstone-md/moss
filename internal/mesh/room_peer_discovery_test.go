package mesh

import (
	"strings"
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
)

// Observe actual encrypted session writes through SubscribeRoom, using arbitrary
// application names: the policy must follow the room's opaque topics.
func TestKnownRoomDoesNotProbeUnrelatedPeers(t *testing.T) {
	node, _, _ := topicPrunePeer(t)
	knownWire, strangerWire := newRecordedSession(t), newRecordedSession(t)
	known, stranger := strings.Repeat("ab", 32), strings.Repeat("cd", 32)
	node.peers = map[string]*peerConn{
		known:    {id: known, session: knownWire.session},
		stranger: {id: stranger, session: strangerWire.session},
	}
	if code := node.JoinRoom("private-game", nil); code != MOSS_OK {
		t.Fatal(code)
	}
	if code := node.SetRoomPeerDiscovery("private-game", RoomDiscoveryKnownPeers, []string{known}); code != MOSS_OK {
		t.Fatal(code)
	}
	if code := node.SubscribeRoom("private-game", "moves"); code != MOSS_OK {
		t.Fatal(code)
	}
	if strangerWire.writeCount() != 0 {
		t.Fatal("known-recipient room speculatively probed an unrelated peer")
	}
	if knownWire.writeCount() == 0 {
		t.Fatal("known recipient did not receive a subscription introduction")
	}
	if code := node.SubscribeRoom("", "public-chat"); code != MOSS_OK {
		t.Fatal(code)
	}
	if strangerWire.writeCount() == 0 {
		t.Fatal("unconfigured room lost automatic discovery")
	}
}

func TestRoomDiscoveryValidationAndLeave(t *testing.T) {
	node, _, _ := topicPrunePeer(t)
	known := strings.Repeat("ab", 32)
	node.JoinRoom("game", nil)
	for _, test := range []struct {
		room  string
		mode  RoomPeerDiscovery
		peers []string
		code  int32
	}{
		{"game", RoomDiscoveryKnownPeers, nil, MOSS_ERR_CONFIG_INVALID},
		{"game", RoomDiscoveryAutomatic, []string{known}, MOSS_ERR_CONFIG_INVALID},
		{"game", 9, nil, MOSS_ERR_CONFIG_INVALID},
		{"game", RoomDiscoveryKnownPeers, []string{"bad"}, MOSS_ERR_CONFIG_INVALID},
		{"game", RoomDiscoveryKnownPeers, []string{strings.Repeat("zz", 32)}, MOSS_ERR_CONFIG_INVALID},
		{"game", RoomDiscoveryKnownPeers, []string{node.localPeerID()}, MOSS_ERR_CONFIG_INVALID},
		{"missing", RoomDiscoveryKnownPeers, []string{known}, MOSS_ERR_NOT_IN_ROOM},
	} {
		if code := node.SetRoomPeerDiscovery(test.room, test.mode, test.peers); code != test.code {
			t.Fatalf("%+v returned %d", test, code)
		}
	}
	node.SetRoomPeerDiscovery("game", RoomDiscoveryKnownPeers, []string{known})
	node.LeaveRoom("game")
	node.JoinRoom("game", nil)
	stranger := newRecordedSession(t)
	node.peers["stranger"] = &peerConn{id: "stranger", session: stranger.session}
	node.SubscribeRoom("game", "moves")
	if stranger.writeCount() == 0 {
		t.Fatal("rejoined room retained an obsolete policy")
	}
}

func TestKnownRoomReplacesHintsAndAcceptsNewSubscription(t *testing.T) {
	node, _, _ := topicPrunePeer(t)
	first, next := strings.Repeat("ab", 32), strings.Repeat("cd", 32)
	firstWire, nextWire := newRecordedSession(t), newRecordedSession(t)
	node.peers = map[string]*peerConn{
		first: {id: first, session: firstWire.session},
		next:  {id: next, session: nextWire.session},
	}
	node.SetRoomPeerDiscovery("", RoomDiscoveryKnownPeers, []string{first})
	node.SetRoomPeerDiscovery("", RoomDiscoveryKnownPeers, []string{next})
	node.Subscribe("moves")
	if firstWire.writeCount() != 0 || nextWire.writeCount() == 0 {
		t.Fatal("replacement retained old discovery recipients")
	}
	// Hints are not an ACL: a new device can still introduce its subscription.
	node.handleEnvelope(node.peers[first], gossip.Envelope{Type: gossip.TypeGraft, Channel: node.roomTopic("moves")})
	if !node.pubsub.InMesh(node.roomTopic("moves"), first) {
		t.Fatal("a fresh subscription was rejected by discovery hints")
	}
}

func TestKnownRoomReturnsItsFirstSubscriptionClaimWithoutPeriodicRefresh(t *testing.T) {
	node, peer, sent := topicPrunePeer(t)
	known := strings.Repeat("ab", 32)
	if code := node.SetRoomPeerDiscovery("", RoomDiscoveryKnownPeers, []string{known}); code != MOSS_OK {
		t.Fatal(code)
	}
	if code := node.Subscribe("late-subscriber"); code != MOSS_OK {
		t.Fatal(code)
	}
	if sent.writeCount() != 0 {
		t.Fatal("known-room discovery probed an unrelated peer")
	}
	topic := node.roomTopic("late-subscriber")
	node.handleEnvelope(peer, gossip.Envelope{Type: gossip.TypeGraft, Channel: topic})
	if sent.writeCount() != 1 {
		t.Fatal("accepted late subscriber did not receive our positive subscription claim")
	}
	for range 16 {
		node.handleEnvelope(peer, gossip.Envelope{Type: gossip.TypeGraft, Channel: topic})
	}
	if sent.writeCount() != 1 {
		t.Fatal("repeated claims created a GRAFT response loop")
	}
}

func TestKnownRoomLateSubscriberExchangesTrafficBeforePeriodicMaintenance(t *testing.T) {
	cfg := overlayTestConfig()
	cfg.GossipSub.HeartbeatMS = 60000
	first := startOverlayNodeWithConfig(t, "reciprocal-room", cfg)
	fresh := startOverlayNodeWithConfig(t, "reciprocal-room", cfg)
	const room, channel = "late-room", "messages"
	key := []byte("late-room-key")
	first.JoinRoom(room, key)
	first.SetRoomPeerDiscovery(room, RoomDiscoveryKnownPeers, []string{strings.Repeat("ab", 32)})
	first.SubscribeRoom(room, channel)
	attachLeaf(t, fresh, first)
	received := relayDeliverySink(t, room, channel, key, first, fresh)
	assertActivityRoomTraffic(t, first, fresh, received, room, channel, "late", time.Second)
}
