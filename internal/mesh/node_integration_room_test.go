package mesh

import (
	"net"
	"strconv"
	"testing"
	"time"
)

// TestPrivateRoomPSKIsolation proves room privacy: two nodes that name the same
// room but hold different PSKs derive different room keys, so they neither share
// a (hashed) topic nor can open each other's sealed payloads. A publish by one
// is never delivered to the other, even though both subscribe the same channel
// name and sit on the same substrate.
func TestPrivateRoomPSKIsolation(t *testing.T) {
	newNode := func(psk []byte, static string) *Node {
		cfg := DefaultConfig()
		cfg.Trackers = nil
		cfg.LANDiscoveryEnabled = false
		cfg.GossipSub.HeartbeatMS = 50
		if static != "" {
			cfg.StaticPeers = []string{static}
		}
		node, err := NewNode("secret-room", psk, cfg)
		if err != nil {
			t.Fatalf("NewNode failed: %v", err)
		}
		if code := node.Start(); code != MOSS_OK {
			t.Fatalf("Start failed: %d", code)
		}
		return node
	}

	pskA := []byte("room-secret-alpha")
	pskB := []byte("room-secret-bravo")

	publisher := newNode(pskA, "")
	defer publisher.Stop()
	insider := newNode(pskA, net.JoinHostPort("127.0.0.1", strconv.Itoa(publisher.ListenPort())))
	defer insider.Stop()
	eavesdropper := newNode(pskB, net.JoinHostPort("127.0.0.1", strconv.Itoa(publisher.ListenPort())))
	defer eavesdropper.Stop()

	// All three connect on the shared substrate regardless of PSK.
	waitForPeerCount(t, publisher, 2)

	got := make(chan []byte, 1)
	insider.SetMessageCallback(func(channel string, _ [32]byte, data []byte) {
		if channel == "chat" {
			got <- append([]byte(nil), data...)
		}
	})
	leaked := make(chan []byte, 1)
	eavesdropper.SetMessageCallback(func(channel string, _ [32]byte, data []byte) {
		if channel == "chat" {
			leaked <- append([]byte(nil), data...)
		}
	})
	insider.Subscribe("chat")
	eavesdropper.Subscribe("chat")
	publisher.Subscribe("chat")
	time.Sleep(200 * time.Millisecond)

	publisher.Publish("chat", []byte("for-insiders-only"))

	select {
	case msg := <-got:
		if string(msg) != "for-insiders-only" {
			t.Fatalf("insider got wrong payload: %q", msg)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("same-PSK insider did not receive the message")
	}

	select {
	case msg := <-leaked:
		t.Fatalf("PRIVACY BREACH: different-PSK eavesdropper read %q", msg)
	case <-time.After(500 * time.Millisecond):
		// Expected: the wrong-PSK node never gets it.
	}
}

// TestCrossRoomSubstrateConnects proves the two-layer model: two nodes in
// DIFFERENT rooms (mesh ids) but on the same substrate (default NetworkID)
// still discover, handshake and peer with each other. This is the property a
// spore/gateway relies on to serve every room.
func TestCrossRoomSubstrateConnects(t *testing.T) {
	cfgA := DefaultConfig()
	cfgA.Trackers = nil
	cfgA.LANDiscoveryEnabled = false
	cfgA.GossipSub.HeartbeatMS = 50
	nodeA, err := NewNode("room-alpha", nil, cfgA)
	if err != nil {
		t.Fatalf("NewNode nodeA failed: %v", err)
	}
	if code := nodeA.Start(); code != MOSS_OK {
		t.Fatalf("nodeA.Start failed: %d", code)
	}
	defer nodeA.Stop()

	cfgB := DefaultConfig()
	cfgB.Trackers = nil
	cfgB.LANDiscoveryEnabled = false
	cfgB.GossipSub.HeartbeatMS = 50
	cfgB.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(nodeA.ListenPort()))}
	nodeB, err := NewNode("room-beta", nil, cfgB)
	if err != nil {
		t.Fatalf("NewNode nodeB failed: %v", err)
	}
	if code := nodeB.Start(); code != MOSS_OK {
		t.Fatalf("nodeB.Start failed: %d", code)
	}
	defer nodeB.Stop()

	// Despite different rooms, the shared substrate lets them peer.
	waitForPeerCount(t, nodeA, 1)
	waitForPeerCount(t, nodeB, 1)
}

// TestCrossRoomPubSubIsolated proves the flip side: a shared substrate does not
// merge rooms. Two connected nodes in different rooms both subscribe the same
// channel name, but a publish in one room is never delivered to the other.
func TestCrossRoomPubSubIsolated(t *testing.T) {
	cfgA := DefaultConfig()
	cfgA.Trackers = nil
	cfgA.LANDiscoveryEnabled = false
	cfgA.GossipSub.HeartbeatMS = 50
	nodeA, err := NewNode("room-alpha", nil, cfgA)
	if err != nil {
		t.Fatalf("NewNode nodeA failed: %v", err)
	}
	if code := nodeA.Start(); code != MOSS_OK {
		t.Fatalf("nodeA.Start failed: %d", code)
	}
	defer nodeA.Stop()

	cfgB := DefaultConfig()
	cfgB.Trackers = nil
	cfgB.LANDiscoveryEnabled = false
	cfgB.GossipSub.HeartbeatMS = 50
	cfgB.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(nodeA.ListenPort()))}
	nodeB, err := NewNode("room-beta", nil, cfgB)
	if err != nil {
		t.Fatalf("NewNode nodeB failed: %v", err)
	}
	if code := nodeB.Start(); code != MOSS_OK {
		t.Fatalf("nodeB.Start failed: %d", code)
	}
	defer nodeB.Stop()

	waitForPeerCount(t, nodeA, 1)
	waitForPeerCount(t, nodeB, 1)

	leaked := make(chan []byte, 1)
	nodeA.SetMessageCallback(func(channel string, senderID [32]byte, data []byte) {
		if channel == "chat" {
			leaked <- append([]byte(nil), data...)
		}
	})

	// Same channel NAME in both rooms; they must not cross.
	if code := nodeA.Subscribe("chat"); code != MOSS_OK {
		t.Fatalf("nodeA.Subscribe failed: %d", code)
	}
	if code := nodeB.Subscribe("chat"); code != MOSS_OK {
		t.Fatalf("nodeB.Subscribe failed: %d", code)
	}
	time.Sleep(150 * time.Millisecond)

	if code := nodeB.Publish("chat", []byte("beta-only")); code != MOSS_OK && code != MOSS_ERR_NO_PEERS {
		t.Fatalf("nodeB.Publish failed: %d", code)
	}

	select {
	case payload := <-leaked:
		t.Fatalf("room isolation breached: nodeA (room-alpha) received %q published in room-beta", string(payload))
	case <-time.After(1 * time.Second):
		// Expected: no cross-room delivery.
	}
}
