package mesh

import (
	"net"
	"strconv"
	"testing"
	"time"
)

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
