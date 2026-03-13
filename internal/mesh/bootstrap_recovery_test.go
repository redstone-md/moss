package mesh

import (
	"net"
	"strconv"
	"testing"
	"time"
)

func TestTrackerBootstrapRecoversAfterClientRestart(t *testing.T) {
	tracker := newCompactTracker()
	defer tracker.Close()

	cfgServer := DefaultConfig()
	cfgServer.Trackers = nil
	cfgServer.LANDiscoveryEnabled = false
	cfgServer.GossipSub.HeartbeatMS = 50
	server, err := NewNode("mesh-bootstrap-restart", nil, cfgServer)
	if err != nil {
		t.Fatalf("NewNode server failed: %v", err)
	}
	if code := server.Start(); code != MOSS_OK {
		t.Fatalf("server.Start failed: %d", code)
	}
	defer server.Stop()

	serverAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(server.ListenPort()))
	tracker.SetPeers([]string{serverAddr})

	newClient := func() *Node {
		cfg := DefaultConfig()
		cfg.Trackers = []string{tracker.URL()}
		cfg.LANDiscoveryEnabled = false
		cfg.GossipSub.HeartbeatMS = 50
		cfg.AnnounceIntervalSec = 1
		cfg.BootstrapTimeoutSec = 1
		cfg.MaxPeers = 1
		node, nodeErr := NewNode("mesh-bootstrap-restart", nil, cfg)
		if nodeErr != nil {
			t.Fatalf("NewNode client failed: %v", nodeErr)
		}
		if code := node.Start(); code != MOSS_OK {
			t.Fatalf("client Start failed: %d", code)
		}
		return node
	}

	nodeA := newClient()
	defer nodeA.Stop()
	nodeB := newClient()
	defer nodeB.Stop()

	if !waitForPeerCountWithin(nodeA, 1, 10*time.Second) || !waitForPeerCountWithin(nodeB, 1, 10*time.Second) {
		t.Fatalf("bootstrap peers did not connect in time; server=%s nodeA=%s nodeB=%s", server.MeshInfoJSON(), nodeA.MeshInfoJSON(), nodeB.MeshInfoJSON())
	}

	if code := nodeA.Subscribe("lobby"); code != MOSS_OK {
		t.Fatalf("nodeA.Subscribe failed: %d", code)
	}
	if code := nodeB.Subscribe("lobby"); code != MOSS_OK {
		t.Fatalf("nodeB.Subscribe failed: %d", code)
	}
	waitForSubscriberCount(t, server, "lobby", 2)

	received := make(chan string, 4)
	nodeB.SetMessageCallback(func(channel string, senderID [32]byte, data []byte) {
		if channel == "lobby" {
			received <- string(data)
		}
	})

	if code := nodeA.Publish("lobby", []byte("before-restart")); code != MOSS_OK && code != MOSS_ERR_NO_PEERS {
		t.Fatalf("nodeA.Publish before-restart failed: %d", code)
	}
	select {
	case payload := <-received:
		if payload != "before-restart" {
			t.Fatalf("unexpected payload before restart: %q", payload)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("bootstrap publish did not converge before restart; server=%s nodeA=%s nodeB=%s", server.MeshInfoJSON(), nodeA.MeshInfoJSON(), nodeB.MeshInfoJSON())
	}

	nodeB.Stop()

	nodeB = newClient()
	defer nodeB.Stop()
	if code := nodeB.Subscribe("lobby"); code != MOSS_OK {
		t.Fatalf("restarted nodeB.Subscribe failed: %d", code)
	}
	nodeB.SetMessageCallback(func(channel string, senderID [32]byte, data []byte) {
		if channel == "lobby" {
			received <- string(data)
		}
	})

	if !waitForPeerCountWithin(nodeB, 1, 10*time.Second) || !waitForPeerCountWithin(nodeA, 1, 10*time.Second) {
		t.Fatalf("bootstrap peers did not reconnect after restart; server=%s nodeA=%s nodeB=%s", server.MeshInfoJSON(), nodeA.MeshInfoJSON(), nodeB.MeshInfoJSON())
	}
	waitForSubscriberCount(t, server, "lobby", 2)

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if code := nodeA.Publish("lobby", []byte("after-restart")); code != MOSS_OK && code != MOSS_ERR_NO_PEERS {
			t.Fatalf("nodeA.Publish after-restart failed: %d", code)
		}
		select {
		case payload := <-received:
			if payload == "after-restart" {
				return
			}
		case <-time.After(200 * time.Millisecond):
		}
	}

	t.Fatalf("bootstrap transit did not recover after client restart in time; server=%s nodeA=%s nodeB=%s", server.MeshInfoJSON(), nodeA.MeshInfoJSON(), nodeB.MeshInfoJSON())
}
