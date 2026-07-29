package mesh

import (
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
)

// One node holding several rooms must be indistinguishable, on the wire, from
// several nodes each holding one. This is the property the whole change rests
// on: without it a client that consolidates its conversations onto one node
// stops being able to talk to every already-released client, which computes its
// topics from a node whose own room is the pair's.
func TestJoinedRoomIsWireIdenticalToOwningIt(t *testing.T) {
	shared, err := NewNode("room-a", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	if code := shared.JoinRoom("room-b", nil); code != MOSS_OK {
		t.Fatalf("JoinRoom: %d", code)
	}
	// What an already-released client looks like: one node, one room.
	legacy, err := NewNode("room-b", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	got := shared.roomTopicIn("room-b", "mls-data/session-1")
	want := legacy.roomTopic("mls-data/session-1")
	if got == "" || got != want {
		t.Fatalf("topic in a joined room is %q, the same room owned outright is %q — "+
			"a consolidated client would publish where nobody is listening", got, want)
	}

	sealed, err := shared.sealRoomIn("room-b", []byte("hello"))
	if err != nil {
		t.Fatalf("sealRoomIn: %v", err)
	}
	plaintext, ok := legacy.openRoom("", sealed)
	if !ok || string(plaintext) != "hello" {
		t.Fatalf("a payload sealed for a joined room did not open on a node that owns it")
	}
}

// Rooms on one node stay isolated from each other: same channel name, unrelated
// topics, and a seal from one is not openable by another.
func TestRoomsOnOneNodeStayIsolated(t *testing.T) {
	node, err := NewNode("room-a", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	if code := node.JoinRoom("room-b", nil); code != MOSS_OK {
		t.Fatalf("JoinRoom: %d", code)
	}

	own := node.roomTopicIn("", "chat")
	joined := node.roomTopicIn("room-b", "chat")
	if own == joined {
		t.Fatalf("both rooms mapped %q to the same topic %q — one conversation would "+
			"receive another's traffic", "chat", own)
	}

	sealed, err := node.sealRoomIn("room-b", []byte("for b only"))
	if err != nil {
		t.Fatalf("sealRoomIn: %v", err)
	}
	if _, ok := node.openRoom("", sealed); ok {
		t.Fatal("room-a's key opened a payload sealed for room-b")
	}
}

// A room that was never joined has no key, so there is nothing to publish
// under. Falling back to the node's own room would put the payload on a topic
// the intended peers do not read — a silent misdelivery rather than an error.
func TestUnjoinedRoomIsRefusedRatherThanSubstituted(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	cfg.LANDiscoveryEnabled = false
	node, err := NewNode("room-a", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	// Started, so the publish reaches the room check rather than stopping at
	// MOSS_ERR_NOT_STARTED.
	if code := node.Start(); code != MOSS_OK {
		t.Fatalf("Start: %d", code)
	}
	defer node.Stop()
	if code := node.SubscribeRoom("room-unknown", "chat"); code != MOSS_ERR_NOT_IN_ROOM {
		t.Fatalf("SubscribeRoom into an unjoined room returned %d, want MOSS_ERR_NOT_IN_ROOM", code)
	}
	if code := node.PublishRoom("room-unknown", "chat", []byte("x")); code != MOSS_ERR_NOT_IN_ROOM {
		t.Fatalf("PublishRoom into an unjoined room returned %d, want MOSS_ERR_NOT_IN_ROOM", code)
	}
	if topic := node.roomTopicIn("room-unknown", "chat"); topic != "" {
		t.Fatalf("an unjoined room produced topic %q; it must produce none", topic)
	}
}

// Delivery picks the key from what the topic was subscribed under, so a message
// arriving for a joined room is opened with that room's key rather than the
// node's own.
func TestDeliveryOpensWithTheSubscribedRoomsKey(t *testing.T) {
	receiver, err := NewNode("room-a", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	if code := receiver.JoinRoom("room-b", nil); code != MOSS_OK {
		t.Fatalf("JoinRoom: %d", code)
	}
	if code := receiver.SubscribeRoom("room-b", "chat"); code != MOSS_OK {
		t.Fatalf("SubscribeRoom: %d", code)
	}

	delivered := make(chan string, 1)
	receiver.SetMessageCallback(func(channel string, _ [32]byte, data []byte) {
		delivered <- channel + "|" + string(data)
	})

	sender, err := NewNode("room-b", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	sealed, err := sender.sealRoom([]byte("payload"))
	if err != nil {
		t.Fatalf("sealRoom: %v", err)
	}
	receiver.deliverLocal(gossip.Envelope{
		Type:    gossip.TypePublish,
		Channel: sender.roomTopic("chat"),
		Payload: sealed,
	})

	select {
	case got := <-delivered:
		if got != "chat|payload" {
			t.Fatalf("delivered %q, want the bare channel and the opened payload", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("nothing delivered — the joined room's key was not used to open it")
	}
}
