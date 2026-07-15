package mesh

import (
	"context"
	"encoding/hex"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/nat"
)

func TestTwoNodesExchangePubSubMessages(t *testing.T) {
	cfg1 := DefaultConfig()
	cfg1.Trackers = nil
	cfg1.AnnounceIntervalSec = 1
	cfg1.GossipSub.HeartbeatMS = 50
	node1, err := NewNode("mesh-int", nil, cfg1)
	if err != nil {
		t.Fatalf("NewNode node1 failed: %v", err)
	}
	if code := node1.Start(); code != MOSS_OK {
		t.Fatalf("node1.Start failed: %d", code)
	}
	defer node1.Stop()

	cfg2 := DefaultConfig()
	cfg2.Trackers = nil
	cfg2.AnnounceIntervalSec = 1
	cfg2.GossipSub.HeartbeatMS = 50
	cfg2.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(node1.ListenPort()))}
	node2, err := NewNode("mesh-int", nil, cfg2)
	if err != nil {
		t.Fatalf("NewNode node2 failed: %v", err)
	}
	if code := node2.Start(); code != MOSS_OK {
		t.Fatalf("node2.Start failed: %d", code)
	}
	defer node2.Stop()

	waitForPeerCount(t, node1, 1)
	waitForPeerCount(t, node2, 1)

	received := make(chan []byte, 1)
	node1.SetMessageCallback(func(channel string, senderID [32]byte, data []byte) {
		if channel == "alpha" {
			received <- append([]byte(nil), data...)
		}
	})

	if code := node1.Subscribe("alpha"); code != MOSS_OK {
		t.Fatalf("node1.Subscribe failed: %d", code)
	}
	if code := node2.Subscribe("alpha"); code != MOSS_OK {
		t.Fatalf("node2.Subscribe failed: %d", code)
	}
	time.Sleep(150 * time.Millisecond)

	if code := node2.Publish("alpha", []byte("payload-1")); code != MOSS_OK {
		t.Fatalf("node2.Publish failed: %d", code)
	}

	select {
	case payload := <-received:
		if string(payload) != "payload-1" {
			t.Fatalf("unexpected payload: %q", string(payload))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for pubsub delivery")
	}
}

func TestFreshPeerIsNotPrunedByNegativeScore(t *testing.T) {
	cfgA := DefaultConfig()
	cfgA.Trackers = nil
	cfgA.LANDiscoveryEnabled = false
	cfgA.GossipSub.HeartbeatMS = 50
	nodeA, err := NewNode("mesh-retain-fresh", nil, cfgA)
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
	nodeB, err := NewNode("mesh-retain-fresh", nil, cfgB)
	if err != nil {
		t.Fatalf("NewNode nodeB failed: %v", err)
	}
	if code := nodeB.Start(); code != MOSS_OK {
		t.Fatalf("nodeB.Start failed: %d", code)
	}
	defer nodeB.Stop()

	waitForPeerCount(t, nodeA, 1)
	waitForPeerCount(t, nodeB, 1)

	nodeA.mu.RLock()
	var peerID string
	for id := range nodeA.peers {
		peerID = id
		break
	}
	nodeA.mu.RUnlock()
	if peerID == "" {
		t.Fatal("expected connected peer")
	}
	nodeA.scoring.SetApplicationScore(peerID, -5)
	nodeA.pruneLowScoringPeers()
	waitForPeerCountAtLeast(t, nodeA, 1, 500*time.Millisecond)
}

func TestLateSubscriberRequestsCachedMessageViaIHaveIWant(t *testing.T) {
	cfg1 := DefaultConfig()
	cfg1.Trackers = nil
	cfg1.GossipSub.HeartbeatMS = 50
	node1, err := NewNode("mesh-catchup", nil, cfg1)
	if err != nil {
		t.Fatalf("NewNode node1 failed: %v", err)
	}
	if code := node1.Start(); code != MOSS_OK {
		t.Fatalf("node1.Start failed: %d", code)
	}
	defer node1.Stop()

	if code := node1.Subscribe("alpha"); code != MOSS_OK {
		t.Fatalf("node1.Subscribe failed: %d", code)
	}
	node1.Publish("alpha", []byte("cached-payload"))

	cfg2 := DefaultConfig()
	cfg2.Trackers = nil
	cfg2.GossipSub.HeartbeatMS = 50
	cfg2.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(node1.ListenPort()))}
	node2, err := NewNode("mesh-catchup", nil, cfg2)
	if err != nil {
		t.Fatalf("NewNode node2 failed: %v", err)
	}
	if code := node2.Start(); code != MOSS_OK {
		t.Fatalf("node2.Start failed: %d", code)
	}
	defer node2.Stop()

	waitForPeerCount(t, node1, 1)
	waitForPeerCount(t, node2, 1)

	received := make(chan []byte, 1)
	node2.SetMessageCallback(func(channel string, senderID [32]byte, data []byte) {
		if channel == "alpha" {
			received <- append([]byte(nil), data...)
		}
	})
	if code := node2.Subscribe("alpha"); code != MOSS_OK {
		t.Fatalf("node2.Subscribe failed: %d", code)
	}

	select {
	case payload := <-received:
		if string(payload) != "cached-payload" {
			t.Fatalf("unexpected payload: %q", string(payload))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for cached message replay")
	}
}

func TestTrackerBootstrapPeersRelayPubSubThroughTransitServer(t *testing.T) {
	tracker := newCompactTracker()
	defer tracker.Close()

	cfgServer := DefaultConfig()
	cfgServer.Trackers = nil
	cfgServer.LANDiscoveryEnabled = false
	cfgServer.GossipSub.HeartbeatMS = 50
	server, err := NewNode("mesh-tracker-transit", nil, cfgServer)
	if err != nil {
		t.Fatalf("NewNode server failed: %v", err)
	}
	if code := server.Start(); code != MOSS_OK {
		t.Fatalf("server.Start failed: %d", code)
	}
	defer server.Stop()
	if code := server.Subscribe("lobby"); code != MOSS_OK {
		t.Fatalf("server.Subscribe failed: %d", code)
	}

	serverAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(server.ListenPort()))
	tracker.SetPeers([]string{serverAddr})

	cfgA := DefaultConfig()
	cfgA.Trackers = []string{tracker.URL()}
	cfgA.LANDiscoveryEnabled = false
	cfgA.GossipSub.HeartbeatMS = 50
	cfgA.AnnounceIntervalSec = 1
	cfgA.MaxPeers = 1
	nodeA, err := NewNode("mesh-tracker-transit", nil, cfgA)
	if err != nil {
		t.Fatalf("NewNode nodeA failed: %v", err)
	}
	if code := nodeA.Start(); code != MOSS_OK {
		t.Fatalf("nodeA.Start failed: %d", code)
	}
	defer nodeA.Stop()

	cfgB := DefaultConfig()
	cfgB.Trackers = []string{tracker.URL()}
	cfgB.LANDiscoveryEnabled = false
	cfgB.GossipSub.HeartbeatMS = 50
	cfgB.AnnounceIntervalSec = 1
	cfgB.MaxPeers = 1
	nodeB, err := NewNode("mesh-tracker-transit", nil, cfgB)
	if err != nil {
		t.Fatalf("NewNode nodeB failed: %v", err)
	}
	if code := nodeB.Start(); code != MOSS_OK {
		t.Fatalf("nodeB.Start failed: %d", code)
	}
	defer nodeB.Stop()

	if !waitForPeerCountWithin(nodeA, 1, 10*time.Second) || !waitForPeerCountWithin(nodeB, 1, 10*time.Second) {
		t.Fatalf("bootstrap peers did not connect in time; server=%s nodeA=%s nodeB=%s", server.MeshInfoJSON(), nodeA.MeshInfoJSON(), nodeB.MeshInfoJSON())
	}

	received := make(chan []byte, 1)
	nodeB.SetMessageCallback(func(channel string, senderID [32]byte, data []byte) {
		if channel == "lobby" {
			received <- append([]byte(nil), data...)
		}
	})

	if code := nodeA.Subscribe("lobby"); code != MOSS_OK {
		t.Fatalf("nodeA.Subscribe failed: %d", code)
	}
	if code := nodeB.Subscribe("lobby"); code != MOSS_OK {
		t.Fatalf("nodeB.Subscribe failed: %d", code)
	}
	waitForSubscriberCount(t, server, "lobby", 2)

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if code := nodeA.Publish("lobby", []byte("tracker-transit")); code != MOSS_OK && code != MOSS_ERR_NO_PEERS {
			t.Fatalf("nodeA.Publish failed: %d", code)
		}
		select {
		case payload := <-received:
			if string(payload) != "tracker-transit" {
				t.Fatalf("unexpected payload: %q", string(payload))
			}
			return
		case <-time.After(250 * time.Millisecond):
		}
	}
	t.Fatalf("bootstrap transit topology did not converge in time; server=%s nodeA=%s nodeB=%s", server.MeshInfoJSON(), nodeA.MeshInfoJSON(), nodeB.MeshInfoJSON())
}

func TestTrackerBootstrapPeerIsRetainedAfterNegativeScore(t *testing.T) {
	tracker := newCompactTracker()
	defer tracker.Close()

	cfgServer := DefaultConfig()
	cfgServer.Trackers = nil
	cfgServer.LANDiscoveryEnabled = false
	server, err := NewNode("mesh-bootstrap-retain", nil, cfgServer)
	if err != nil {
		t.Fatalf("NewNode server failed: %v", err)
	}
	if code := server.Start(); code != MOSS_OK {
		t.Fatalf("server.Start failed: %d", code)
	}
	defer server.Stop()

	serverAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(server.ListenPort()))
	tracker.SetPeers([]string{serverAddr})

	cfgClient := DefaultConfig()
	cfgClient.Trackers = []string{tracker.URL()}
	cfgClient.LANDiscoveryEnabled = false
	cfgClient.AnnounceIntervalSec = 1
	cfgClient.MaxPeers = 1
	client, err := NewNode("mesh-bootstrap-retain", nil, cfgClient)
	if err != nil {
		t.Fatalf("NewNode client failed: %v", err)
	}
	if code := client.Start(); code != MOSS_OK {
		t.Fatalf("client.Start failed: %d", code)
	}
	defer client.Stop()

	if !waitForPeerCountWithin(server, 1, 8*time.Second) || !waitForPeerCountWithin(client, 1, 8*time.Second) {
		t.Fatalf("bootstrap retain topology did not converge in time; server=%s client=%s", server.MeshInfoJSON(), client.MeshInfoJSON())
	}

	serverPub := server.PublicKey()
	serverID := hex.EncodeToString(serverPub[:])
	client.scoring.SetApplicationScore(serverID, -5)
	client.mu.Lock()
	if peer := client.peers[serverID]; peer != nil {
		peer.connectedAt = time.Now().Add(-time.Minute)
	}
	client.mu.Unlock()
	client.pruneLowScoringPeers()
	waitForPeerCountAtLeast(t, client, 1, 500*time.Millisecond)
}

func TestDirectPeerAnnouncementDoesNotOverwriteSessionAddress(t *testing.T) {
	cfgA := DefaultConfig()
	cfgA.Trackers = nil
	cfgA.LANDiscoveryEnabled = false
	nodeA, err := NewNode("mesh-direct-announce", nil, cfgA)
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
	cfgB.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(nodeA.ListenPort()))}
	nodeB, err := NewNode("mesh-direct-announce", nil, cfgB)
	if err != nil {
		t.Fatalf("NewNode nodeB failed: %v", err)
	}
	if code := nodeB.Start(); code != MOSS_OK {
		t.Fatalf("nodeB.Start failed: %d", code)
	}
	defer nodeB.Stop()

	waitForPeerCount(t, nodeA, 1)
	waitForPeerCount(t, nodeB, 1)

	peerPub := nodeA.PublicKey()
	peerID := hex.EncodeToString(peerPub[:])

	nodeB.mu.RLock()
	peer := nodeB.peers[peerID]
	actualAddr := ""
	if peer != nil {
		actualAddr = peer.addr
	}
	nodeB.mu.RUnlock()
	if peer == nil || actualAddr == "" {
		t.Fatal("expected direct peer session")
	}

	nodeB.handleKnownPeerEnvelope(peer, gossip.Envelope{
		Type:              gossip.TypePeerAnnounce,
		AdvertisedPeerID:  peerID,
		AdvertisedAddr:    "10.123.45.67:41030",
		AdvertisedNATType: string(nat.TypePublic),
	}, gossip.TypePeerAnnounce, false)

	nodeB.mu.RLock()
	got := nodeB.knownPeers[peerID].addr
	nodeB.mu.RUnlock()
	if got != actualAddr {
		t.Fatalf("expected direct peer to keep session addr %s, got %s", actualAddr, got)
	}
}

func TestNodeRejectsSelfPeerConnection(t *testing.T) {
	host, ok := bestLocalAdvertiseHost()
	if !ok {
		t.Skip("no non-loopback local address available")
	}

	cfg := DefaultConfig()
	cfg.Trackers = nil
	cfg.LANDiscoveryEnabled = false
	node, err := NewNode("mesh-self-peer", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	if code := node.Start(); code != MOSS_OK {
		t.Fatalf("node.Start failed: %d", code)
	}
	defer node.Stop()

	addr := net.JoinHostPort(host, strconv.Itoa(node.ListenPort()))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = node.connectPeer(ctx, addr)
	waitForPeerCountAtMost(t, node, 0, 500*time.Millisecond)
}

func TestRelaySessionDeliversThroughIntermediatePeer(t *testing.T) {
	cfgRelay := DefaultConfig()
	cfgRelay.Trackers = nil
	cfgRelay.GossipSub.HeartbeatMS = 50
	relayNode, err := NewNode("mesh-relay", nil, cfgRelay)
	if err != nil {
		t.Fatalf("NewNode relay failed: %v", err)
	}
	if code := relayNode.Start(); code != MOSS_OK {
		t.Fatalf("relayNode.Start failed: %d", code)
	}
	defer relayNode.Stop()

	cfgA := DefaultConfig()
	cfgA.Trackers = nil
	cfgA.LANDiscoveryEnabled = false
	cfgA.GossipSub.HeartbeatMS = 50
	cfgA.MaxPeers = 1
	cfgA.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(relayNode.ListenPort()))}
	nodeA, err := NewNode("mesh-relay", nil, cfgA)
	if err != nil {
		t.Fatalf("NewNode nodeA failed: %v", err)
	}
	if code := nodeA.Start(); code != MOSS_OK {
		t.Fatalf("nodeA.Start failed: %d", code)
	}
	defer nodeA.Stop()

	cfgB := DefaultConfig()
	cfgB.Trackers = nil
	cfgB.GossipSub.HeartbeatMS = 50
	cfgB.MaxPeers = 1
	cfgB.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(relayNode.ListenPort()))}
	nodeB, err := NewNode("mesh-relay", nil, cfgB)
	if err != nil {
		t.Fatalf("NewNode nodeB failed: %v", err)
	}
	if code := nodeB.Start(); code != MOSS_OK {
		t.Fatalf("nodeB.Start failed: %d", code)
	}
	defer nodeB.Stop()

	waitForPeerCount(t, relayNode, 2)
	waitForPeerCount(t, nodeA, 1)
	waitForPeerCount(t, nodeB, 1)

	receivedByA := make(chan []byte, 1)
	nodeA.SetRelayCallback(func(senderID [32]byte, data []byte) {
		_ = senderID
		receivedByA <- append([]byte(nil), data...)
	})
	receivedByB := make(chan []byte, 1)
	nodeB.SetRelayCallback(func(senderID [32]byte, data []byte) {
		_ = senderID
		receivedByB <- append([]byte(nil), data...)
	})

	relayPub := relayNode.PublicKey()
	targetPub := nodeB.PublicKey()
	sessionID, err := nodeA.OpenRelaySession(hex.EncodeToString(relayPub[:]), hex.EncodeToString(targetPub[:]), 2*time.Second)
	if err != nil {
		t.Fatalf("OpenRelaySession failed: %v", err)
	}
	waitForRelaySession(t, nodeA, sessionID)
	waitForRelaySession(t, nodeB, sessionID)
	if err := nodeA.RelaySend(sessionID, []byte("through-relay")); err != nil {
		t.Fatalf("RelaySend failed: %v", err)
	}

	select {
	case payload := <-receivedByB:
		if string(payload) != "through-relay" {
			t.Fatalf("unexpected relay payload: %q", string(payload))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for relayed payload")
	}

	if err := nodeB.RelaySend(sessionID, []byte("relay-response")); err != nil {
		t.Fatalf("reverse RelaySend failed: %v", err)
	}

	select {
	case payload := <-receivedByA:
		if string(payload) != "relay-response" {
			t.Fatalf("unexpected reverse relay payload: %q", string(payload))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for reverse relayed payload")
	}
}
