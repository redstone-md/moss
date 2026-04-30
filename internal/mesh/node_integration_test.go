package mesh

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	mcrypto "moss/internal/crypto"
	"moss/internal/gossip"
	"moss/internal/nat"
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
	}, gossip.TypePeerAnnounce)

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

	received := make(chan []byte, 1)
	nodeB.SetRelayCallback(func(senderID [32]byte, data []byte) {
		_ = senderID
		received <- append([]byte(nil), data...)
	})

	relayPub := relayNode.PublicKey()
	targetPub := nodeB.PublicKey()
	sessionID, err := nodeA.OpenRelaySession(hex.EncodeToString(relayPub[:]), hex.EncodeToString(targetPub[:]), 2*time.Second)
	if err != nil {
		t.Fatalf("OpenRelaySession failed: %v", err)
	}
	if err := nodeA.RelaySend(sessionID, []byte("through-relay")); err != nil {
		t.Fatalf("RelaySend failed: %v", err)
	}

	select {
	case payload := <-received:
		if string(payload) != "through-relay" {
			t.Fatalf("unexpected relay payload: %q", string(payload))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for relayed payload")
	}
}

func TestRelaySendToAutoOpensRelaySession(t *testing.T) {
	cfgRelay := DefaultConfig()
	cfgRelay.Trackers = nil
	cfgRelay.GossipSub.HeartbeatMS = 50
	relayNode, err := NewNode("mesh-relay-auto", nil, cfgRelay)
	if err != nil {
		t.Fatalf("NewNode relay failed: %v", err)
	}
	if code := relayNode.Start(); code != MOSS_OK {
		t.Fatalf("relayNode.Start failed: %d", code)
	}
	defer relayNode.Stop()

	cfgA := DefaultConfig()
	cfgA.Trackers = nil
	cfgA.GossipSub.HeartbeatMS = 50
	cfgA.MaxPeers = 1
	cfgA.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(relayNode.ListenPort()))}
	nodeA, err := NewNode("mesh-relay-auto", nil, cfgA)
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
	nodeB, err := NewNode("mesh-relay-auto", nil, cfgB)
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

	received := make(chan []byte, 1)
	nodeB.SetRelayCallback(func(senderID [32]byte, data []byte) {
		_ = senderID
		received <- append([]byte(nil), data...)
	})

	targetPub := nodeB.PublicKey()
	if err := nodeA.RelaySendTo(hex.EncodeToString(targetPub[:]), []byte("auto-relay"), 2*time.Second); err != nil {
		if err.Error() == "target peer became directly connected; use direct transport" || err.Error() == "target peer is directly connected" {
			return
		}
		t.Fatalf("RelaySendTo failed: %v", err)
	}

	select {
	case payload := <-received:
		if string(payload) != "auto-relay" {
			t.Fatalf("unexpected relay payload: %q", string(payload))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for auto-relayed payload")
	}
}

func TestRelayNodeEnforcesSessionLimit(t *testing.T) {
	cfgRelay := DefaultConfig()
	cfgRelay.Trackers = nil
	cfgRelay.GossipSub.HeartbeatMS = 5000
	cfgRelay.NAT.RelayMaxSessions = 1
	relayNode, err := NewNode("mesh-relay-limit", nil, cfgRelay)
	if err != nil {
		t.Fatalf("NewNode relay failed: %v", err)
	}
	if code := relayNode.Start(); code != MOSS_OK {
		t.Fatalf("relayNode.Start failed: %d", code)
	}
	defer relayNode.Stop()

	makeLeaf := func() *Node {
		cfg := DefaultConfig()
		cfg.Trackers = nil
		cfg.GossipSub.HeartbeatMS = 5000
		cfg.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(relayNode.ListenPort()))}
		node, err := NewNode("mesh-relay-limit", nil, cfg)
		if err != nil {
			t.Fatalf("NewNode leaf failed: %v", err)
		}
		if code := node.Start(); code != MOSS_OK {
			t.Fatalf("leaf.Start failed: %d", code)
		}
		return node
	}

	nodeA := makeLeaf()
	defer nodeA.Stop()
	nodeB := makeLeaf()
	defer nodeB.Stop()
	nodeC := makeLeaf()
	defer nodeC.Stop()
	nodeD := makeLeaf()
	defer nodeD.Stop()

	waitForPeerCount(t, relayNode, 4)

	relayPub := relayNode.PublicKey()
	nodeBPub := nodeB.PublicKey()
	sessionID, err := nodeA.OpenRelaySession(hex.EncodeToString(relayPub[:]), hex.EncodeToString(nodeBPub[:]), 2*time.Second)
	if err != nil {
		t.Fatalf("OpenRelaySession A->B failed: %v", err)
	}
	waitForRelaySession(t, nodeA, sessionID)
	waitForRelaySession(t, nodeB, sessionID)
	waitForRelayRoute(t, relayNode, sessionID)

	nodeDPub := nodeD.PublicKey()
	if _, err := nodeC.OpenRelaySession(hex.EncodeToString(relayPub[:]), hex.EncodeToString(nodeDPub[:]), 400*time.Millisecond); err == nil {
		t.Fatal("expected second relay session to be rejected by relay session limit")
	}
}

func TestRelayNodeEnforcesConfiguredBandwidth(t *testing.T) {
	cfgRelay := DefaultConfig()
	cfgRelay.Trackers = nil
	cfgRelay.GossipSub.HeartbeatMS = 5000
	cfgRelay.NAT.RelayMaxBandwidthKBPS = 1
	cfgRelay.Security.RateLimitBurst = 1 << 20
	cfgRelay.Security.RateLimitSustained = 1 << 20
	relayNode, err := NewNode("mesh-relay-bandwidth", nil, cfgRelay)
	if err != nil {
		t.Fatalf("NewNode relay failed: %v", err)
	}
	if code := relayNode.Start(); code != MOSS_OK {
		t.Fatalf("relayNode.Start failed: %d", code)
	}
	defer relayNode.Stop()

	cfgA := DefaultConfig()
	cfgA.Trackers = nil
	cfgA.GossipSub.HeartbeatMS = 5000
	cfgA.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(relayNode.ListenPort()))}
	nodeA, err := NewNode("mesh-relay-bandwidth", nil, cfgA)
	if err != nil {
		t.Fatalf("NewNode nodeA failed: %v", err)
	}
	if code := nodeA.Start(); code != MOSS_OK {
		t.Fatalf("nodeA.Start failed: %d", code)
	}
	defer nodeA.Stop()

	cfgB := DefaultConfig()
	cfgB.Trackers = nil
	cfgB.GossipSub.HeartbeatMS = 5000
	cfgB.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(relayNode.ListenPort()))}
	nodeB, err := NewNode("mesh-relay-bandwidth", nil, cfgB)
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

	received := make(chan []byte, 2)
	nodeB.SetRelayCallback(func(senderID [32]byte, data []byte) {
		_ = senderID
		received <- append([]byte(nil), data...)
	})

	relayPub := relayNode.PublicKey()
	targetPub := nodeB.PublicKey()
	sessionID, err := nodeA.OpenRelaySession(hex.EncodeToString(relayPub[:]), hex.EncodeToString(targetPub[:]), 2*time.Second)
	if err != nil {
		t.Fatalf("OpenRelaySession failed: %v", err)
	}

	firstPayload := bytes.Repeat([]byte("a"), 1024)
	secondPayload := bytes.Repeat([]byte("b"), 1024)
	if err := nodeA.RelaySend(sessionID, firstPayload); err != nil {
		t.Fatalf("first RelaySend failed: %v", err)
	}
	if err := nodeA.RelaySend(sessionID, secondPayload); err != nil {
		t.Fatalf("second RelaySend failed: %v", err)
	}

	select {
	case payload := <-received:
		if !bytes.Equal(payload, firstPayload) {
			t.Fatalf("unexpected first relay payload: %q", string(payload))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for first relayed payload")
	}

	select {
	case payload := <-received:
		t.Fatalf("expected second relay payload to be throttled, got %d bytes", len(payload))
	case <-time.After(400 * time.Millisecond):
	}
}

func TestRelayBandwidthOverloadDemotesAndRecoversSupernode(t *testing.T) {
	cfgRelay := DefaultConfig()
	cfgRelay.Trackers = nil
	cfgRelay.GossipSub.HeartbeatMS = 50
	cfgRelay.NAT.SuperNodeMinUptimeSec = 0
	cfgRelay.NAT.RelayMaxBandwidthKBPS = 1
	cfgRelay.Security.RateLimitBurst = 1 << 20
	cfgRelay.Security.RateLimitSustained = 1 << 20
	relayNode, err := NewNode("mesh-relay-overload", nil, cfgRelay)
	if err != nil {
		t.Fatalf("NewNode relay failed: %v", err)
	}
	if code := relayNode.Start(); code != MOSS_OK {
		t.Fatalf("relayNode.Start failed: %d", code)
	}
	defer relayNode.Stop()

	cfgA := DefaultConfig()
	cfgA.Trackers = nil
	cfgA.GossipSub.HeartbeatMS = 50
	cfgA.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(relayNode.ListenPort()))}
	nodeA, err := NewNode("mesh-relay-overload", nil, cfgA)
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
	cfgB.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(relayNode.ListenPort()))}
	nodeB, err := NewNode("mesh-relay-overload", nil, cfgB)
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

	relayPub := relayNode.PublicKey()
	relayID := hex.EncodeToString(relayPub[:])
	waitForKnownPeer(t, nodeA, relayID)
	relayNode.natProfile.Store(nat.Profile{
		Type:            nat.TypePublic,
		PublicReachable: true,
		ExternalAddress: net.JoinHostPort("203.0.113.30", strconv.Itoa(relayNode.ListenPort())),
	})
	relayNode.refreshSupernodeStatus()
	waitForKnownPeerRelayCapable(t, nodeA, relayID, true)

	targetPub := nodeB.PublicKey()
	sessionID, err := nodeA.OpenRelaySession(relayID, hex.EncodeToString(targetPub[:]), 2*time.Second)
	if err != nil {
		t.Fatalf("OpenRelaySession failed: %v", err)
	}

	firstPayload := bytes.Repeat([]byte("a"), 1024)
	secondPayload := bytes.Repeat([]byte("b"), 1024)
	if err := nodeA.RelaySend(sessionID, firstPayload); err != nil {
		t.Fatalf("first RelaySend failed: %v", err)
	}
	if err := nodeA.RelaySend(sessionID, secondPayload); err != nil {
		t.Fatalf("second RelaySend failed: %v", err)
	}

	waitForKnownPeerRelayCapable(t, nodeA, relayID, false)
	time.Sleep(relayNode.relayOverloadCooldown() + 150*time.Millisecond)
	waitForKnownPeerRelayCapable(t, nodeA, relayID, true)
}

func TestSupernodeStatusAnnounceAndRevokePropagatesOnce(t *testing.T) {
	cfgA := DefaultConfig()
	cfgA.Trackers = nil
	cfgA.GossipSub.HeartbeatMS = 50
	cfgA.NAT.SuperNodeMinUptimeSec = 0
	nodeA, err := NewNode("mesh-supernode-status", nil, cfgA)
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
	cfgB.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(nodeA.ListenPort()))}
	nodeB, err := NewNode("mesh-supernode-status", nil, cfgB)
	if err != nil {
		t.Fatalf("NewNode nodeB failed: %v", err)
	}
	if code := nodeB.Start(); code != MOSS_OK {
		t.Fatalf("nodeB.Start failed: %d", code)
	}
	defer nodeB.Stop()

	waitForPeerCount(t, nodeA, 1)
	waitForPeerCount(t, nodeB, 1)

	promoted := make(chan struct{}, 4)
	revoked := make(chan struct{}, 4)
	nodeA.SetEventCallback(func(eventType int32, detailJSON string) {
		switch eventType {
		case EventSupernodePromoted:
			promoted <- struct{}{}
		case EventSupernodeRevoked:
			revoked <- struct{}{}
		}
	})

	nodeAPub := nodeA.PublicKey()
	nodeAID := hex.EncodeToString(nodeAPub[:])
	waitForKnownPeer(t, nodeB, nodeAID)
	nodeA.natProfile.Store(nat.Profile{
		Type:            nat.TypePublic,
		PublicReachable: true,
		ExternalAddress: net.JoinHostPort("203.0.113.10", strconv.Itoa(nodeA.ListenPort())),
	})
	nodeA.refreshSupernodeStatus()

	waitForKnownPeerRelayCapable(t, nodeB, nodeAID, true)
	select {
	case <-promoted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for supernode promotion event")
	}

	select {
	case <-promoted:
		t.Fatal("unexpected duplicate supernode promotion event")
	case <-time.After(200 * time.Millisecond):
	}

	nodeA.natProfile.Store(nat.Profile{
		Type:            nat.TypeSymmetric,
		PublicReachable: false,
		ExternalAddress: net.JoinHostPort("203.0.113.10", strconv.Itoa(nodeA.ListenPort())),
	})
	nodeA.refreshSupernodeStatus()

	waitForKnownPeerRelayCapable(t, nodeB, nodeAID, false)
	select {
	case <-revoked:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for supernode revoke event")
	}

	select {
	case <-revoked:
		t.Fatal("unexpected duplicate supernode revoke event")
	case <-time.After(200 * time.Millisecond):
	}
}

func TestPeerAnnouncementsPopulateKnownPeerDirectory(t *testing.T) {
	cfgRelay := DefaultConfig()
	cfgRelay.Trackers = nil
	cfgRelay.GossipSub.HeartbeatMS = 50
	relayNode, err := NewNode("mesh-directory", nil, cfgRelay)
	if err != nil {
		t.Fatalf("NewNode relay failed: %v", err)
	}
	if code := relayNode.Start(); code != MOSS_OK {
		t.Fatalf("relayNode.Start failed: %d", code)
	}
	defer relayNode.Stop()

	cfgA := DefaultConfig()
	cfgA.Trackers = nil
	cfgA.GossipSub.HeartbeatMS = 50
	cfgA.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(relayNode.ListenPort()))}
	nodeA, err := NewNode("mesh-directory", nil, cfgA)
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
	cfgB.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(relayNode.ListenPort()))}
	nodeB, err := NewNode("mesh-directory", nil, cfgB)
	if err != nil {
		t.Fatalf("NewNode nodeB failed: %v", err)
	}
	if code := nodeB.Start(); code != MOSS_OK {
		t.Fatalf("nodeB.Start failed: %d", code)
	}
	defer nodeB.Stop()

	targetPub := nodeB.PublicKey()
	waitForKnownPeer(t, nodeA, hex.EncodeToString(targetPub[:]))
}

func TestDiscoveredPeersAutoConnectIntoOverlay(t *testing.T) {
	cfgB := DefaultConfig()
	cfgB.Trackers = nil
	cfgB.GossipSub.HeartbeatMS = 50
	nodeB, err := NewNode("mesh-overlay", nil, cfgB)
	if err != nil {
		t.Fatalf("NewNode nodeB failed: %v", err)
	}
	if code := nodeB.Start(); code != MOSS_OK {
		t.Fatalf("nodeB.Start failed: %d", code)
	}
	defer nodeB.Stop()

	makeLeaf := func(port int) *Node {
		cfg := DefaultConfig()
		cfg.Trackers = nil
		cfg.GossipSub.HeartbeatMS = 50
		cfg.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(nodeB.ListenPort()))}
		node, err := NewNode("mesh-overlay", nil, cfg)
		if err != nil {
			t.Fatalf("NewNode leaf failed: %v", err)
		}
		if code := node.Start(); code != MOSS_OK {
			t.Fatalf("leaf.Start failed: %d", code)
		}
		return node
	}

	nodeA := makeLeaf(0)
	defer nodeA.Stop()
	nodeC := makeLeaf(0)
	defer nodeC.Stop()

	waitForPeerCount(t, nodeB, 2)
	waitForPeerCount(t, nodeA, 1)
	waitForPeerCount(t, nodeC, 1)

	targetPub := nodeC.PublicKey()
	targetID := hex.EncodeToString(targetPub[:])
	waitForKnownPeer(t, nodeA, targetID)
	waitForDirectPeer(t, nodeA, targetID)

	sourcePub := nodeA.PublicKey()
	waitForDirectPeer(t, nodeC, hex.EncodeToString(sourcePub[:]))
}

func TestDiscoveredPeerReconnectsAfterRestartWithNewPort(t *testing.T) {
	cfgHub := DefaultConfig()
	cfgHub.Trackers = nil
	cfgHub.GossipSub.HeartbeatMS = 50
	hub, err := NewNode("mesh-overlay-restart", nil, cfgHub)
	if err != nil {
		t.Fatalf("NewNode hub failed: %v", err)
	}
	if code := hub.Start(); code != MOSS_OK {
		t.Fatalf("hub.Start failed: %d", code)
	}
	defer hub.Stop()

	newLeaf := func(identity *mcrypto.Identity) *Node {
		cfg := DefaultConfig()
		cfg.Trackers = nil
		cfg.GossipSub.HeartbeatMS = 50
		cfg.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(hub.ListenPort()))}
		node, err := NewNodeWithIdentity("mesh-overlay-restart", nil, cfg, identity)
		if err != nil {
			t.Fatalf("NewNode leaf failed: %v", err)
		}
		if code := node.Start(); code != MOSS_OK {
			t.Fatalf("leaf.Start failed: %d", code)
		}
		return node
	}

	nodeA := newLeaf(nil)
	defer nodeA.Stop()

	restartIdentity, err := mcrypto.NewIdentity()
	if err != nil {
		t.Fatalf("NewIdentity failed: %v", err)
	}
	nodeB := newLeaf(restartIdentity)
	defer nodeB.Stop()

	waitForPeerCount(t, hub, 2)
	waitForPeerCount(t, nodeA, 1)
	waitForPeerCount(t, nodeB, 1)

	nodeBPub := nodeB.PublicKey()
	nodeBID := hex.EncodeToString(nodeBPub[:])
	nodeAPub := nodeA.PublicKey()
	nodeAID := hex.EncodeToString(nodeAPub[:])
	waitForKnownPeer(t, nodeA, nodeBID)
	waitForDirectPeer(t, nodeA, nodeBID)
	waitForDirectPeer(t, nodeB, nodeAID)
	waitForPeerCount(t, nodeA, 2)

	nodeB.Stop()

	nodeB = newLeaf(restartIdentity)
	defer nodeB.Stop()
	waitForPeerCount(t, nodeB, 1)
	if nodeB.ListenPort() == 0 {
		t.Fatal("expected restarted nodeB to have a listen port")
	}
	restartedAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(nodeB.ListenPort()))
	waitForKnownPeer(t, nodeA, nodeBID)
	waitForDirectPeer(t, nodeA, nodeBID)
	waitForDirectPeer(t, nodeB, nodeAID)
	waitForKnownPeerAddr(t, nodeA, nodeBID, restartedAddr)
	waitForPeerCount(t, nodeA, 2)
}

func TestRelaySendToFallsBackAfterDirectDialFailure(t *testing.T) {
	cfgRelay := DefaultConfig()
	cfgRelay.Trackers = nil
	cfgRelay.GossipSub.HeartbeatMS = 50
	relayNode, err := NewNode("mesh-relay-known", nil, cfgRelay)
	if err != nil {
		t.Fatalf("NewNode relay failed: %v", err)
	}
	if code := relayNode.Start(); code != MOSS_OK {
		t.Fatalf("relayNode.Start failed: %d", code)
	}
	defer relayNode.Stop()

	cfgA := DefaultConfig()
	cfgA.Trackers = nil
	cfgA.GossipSub.HeartbeatMS = 50
	cfgA.MaxPeers = 1
	cfgA.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(relayNode.ListenPort()))}
	nodeA, err := NewNode("mesh-relay-known", nil, cfgA)
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
	nodeB, err := NewNode("mesh-relay-known", nil, cfgB)
	if err != nil {
		t.Fatalf("NewNode nodeB failed: %v", err)
	}
	if code := nodeB.Start(); code != MOSS_OK {
		t.Fatalf("nodeB.Start failed: %d", code)
	}
	defer nodeB.Stop()

	waitForPeerCount(t, relayNode, 2)
	targetPub := nodeB.PublicKey()
	targetID := hex.EncodeToString(targetPub[:])
	waitForKnownPeer(t, nodeA, targetID)

	nodeA.mu.Lock()
	info := nodeA.knownPeers[targetID]
	info.addr = "127.0.0.1:1"
	nodeA.knownPeers[targetID] = info
	nodeA.mu.Unlock()

	received := make(chan []byte, 1)
	nodeB.SetRelayCallback(func(senderID [32]byte, data []byte) {
		_ = senderID
		received <- append([]byte(nil), data...)
	})

	if err := nodeA.RelaySendTo(targetID, []byte("fallback-relay"), 500*time.Millisecond); err != nil {
		t.Fatalf("RelaySendTo failed: %v", err)
	}

	select {
	case payload := <-received:
		if string(payload) != "fallback-relay" {
			t.Fatalf("unexpected relay payload: %q", string(payload))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for fallback relay payload")
	}
}

func TestRelaySendToFallsBackToSecondaryRelayPeer(t *testing.T) {
	cfgRelay1 := DefaultConfig()
	cfgRelay1.Trackers = nil
	cfgRelay1.GossipSub.HeartbeatMS = 50
	cfgRelay1.NAT.RelayMaxSessions = 1
	relay1, err := NewNode("mesh-relay-secondary", nil, cfgRelay1)
	if err != nil {
		t.Fatalf("NewNode relay1 failed: %v", err)
	}
	if code := relay1.Start(); code != MOSS_OK {
		t.Fatalf("relay1.Start failed: %d", code)
	}
	defer relay1.Stop()

	cfgRelay2 := DefaultConfig()
	cfgRelay2.Trackers = nil
	cfgRelay2.GossipSub.HeartbeatMS = 50
	relay2, err := NewNode("mesh-relay-secondary", nil, cfgRelay2)
	if err != nil {
		t.Fatalf("NewNode relay2 failed: %v", err)
	}
	if code := relay2.Start(); code != MOSS_OK {
		t.Fatalf("relay2.Start failed: %d", code)
	}
	defer relay2.Stop()

	cfgA := DefaultConfig()
	cfgA.Trackers = nil
	cfgA.GossipSub.HeartbeatMS = 50
	cfgA.MaxPeers = 2
	cfgA.StaticPeers = []string{
		net.JoinHostPort("127.0.0.1", strconv.Itoa(relay1.ListenPort())),
		net.JoinHostPort("127.0.0.1", strconv.Itoa(relay2.ListenPort())),
	}
	nodeA, err := NewNode("mesh-relay-secondary", nil, cfgA)
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
	cfgB.MaxPeers = 2
	cfgB.StaticPeers = []string{
		net.JoinHostPort("127.0.0.1", strconv.Itoa(relay1.ListenPort())),
		net.JoinHostPort("127.0.0.1", strconv.Itoa(relay2.ListenPort())),
	}
	nodeB, err := NewNode("mesh-relay-secondary", nil, cfgB)
	if err != nil {
		t.Fatalf("NewNode nodeB failed: %v", err)
	}
	if code := nodeB.Start(); code != MOSS_OK {
		t.Fatalf("nodeB.Start failed: %d", code)
	}
	defer nodeB.Stop()

	waitForPeerCount(t, relay1, 2)
	waitForPeerCount(t, relay2, 2)
	waitForPeerCount(t, nodeA, 2)
	waitForPeerCount(t, nodeB, 2)

	targetPub := nodeB.PublicKey()
	targetID := hex.EncodeToString(targetPub[:])
	relay1Pub := relay1.PublicKey()
	relay1ID := hex.EncodeToString(relay1Pub[:])
	relay2Pub := relay2.PublicKey()
	relay2ID := hex.EncodeToString(relay2Pub[:])

	waitForKnownPeer(t, nodeA, targetID)
	nodeA.mu.Lock()
	info1 := nodeA.knownPeers[relay1ID]
	info1.relayCapable = true
	info1.publicReachable = true
	info1.natType = nat.TypePublic
	nodeA.knownPeers[relay1ID] = info1
	info2 := nodeA.knownPeers[relay2ID]
	info2.relayCapable = true
	info2.publicReachable = true
	info2.natType = nat.TypePublic
	nodeA.knownPeers[relay2ID] = info2
	nodeA.mu.Unlock()
	nodeA.scoring.SetApplicationScore(relay1ID, 10)
	nodeA.scoring.SetApplicationScore(relay2ID, 1)

	if !relay1.relaySessions.Acquire("saturated") {
		t.Fatal("expected relay1 capacity acquisition to succeed")
	}
	defer relay1.relaySessions.Release("saturated")

	received := make(chan []byte, 1)
	nodeB.SetRelayCallback(func(senderID [32]byte, data []byte) {
		_ = senderID
		received <- append([]byte(nil), data...)
	})

	if err := nodeA.RelaySendTo(targetID, []byte("secondary-relay"), 2*time.Second); err != nil {
		t.Fatalf("RelaySendTo failed: %v", err)
	}

	select {
	case payload := <-received:
		if string(payload) != "secondary-relay" {
			t.Fatalf("unexpected relay payload: %q", string(payload))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for fallback secondary relay payload")
	}
}

func TestRelaySessionAnyPrefersLessLoadedRelayPeer(t *testing.T) {
	cfgRelay1 := DefaultConfig()
	cfgRelay1.Trackers = nil
	cfgRelay1.GossipSub.HeartbeatMS = 50
	relay1, err := NewNode("mesh-relay-balance", nil, cfgRelay1)
	if err != nil {
		t.Fatalf("NewNode relay1 failed: %v", err)
	}
	if code := relay1.Start(); code != MOSS_OK {
		t.Fatalf("relay1.Start failed: %d", code)
	}
	defer relay1.Stop()

	cfgRelay2 := DefaultConfig()
	cfgRelay2.Trackers = nil
	cfgRelay2.GossipSub.HeartbeatMS = 50
	relay2, err := NewNode("mesh-relay-balance", nil, cfgRelay2)
	if err != nil {
		t.Fatalf("NewNode relay2 failed: %v", err)
	}
	if code := relay2.Start(); code != MOSS_OK {
		t.Fatalf("relay2.Start failed: %d", code)
	}
	defer relay2.Stop()

	cfgA := DefaultConfig()
	cfgA.Trackers = nil
	cfgA.GossipSub.HeartbeatMS = 50
	cfgA.MaxPeers = 2
	cfgA.StaticPeers = []string{
		net.JoinHostPort("127.0.0.1", strconv.Itoa(relay1.ListenPort())),
		net.JoinHostPort("127.0.0.1", strconv.Itoa(relay2.ListenPort())),
	}
	nodeA, err := NewNode("mesh-relay-balance", nil, cfgA)
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
	cfgB.MaxPeers = 2
	cfgB.StaticPeers = []string{
		net.JoinHostPort("127.0.0.1", strconv.Itoa(relay1.ListenPort())),
		net.JoinHostPort("127.0.0.1", strconv.Itoa(relay2.ListenPort())),
	}
	nodeB, err := NewNode("mesh-relay-balance", nil, cfgB)
	if err != nil {
		t.Fatalf("NewNode nodeB failed: %v", err)
	}
	if code := nodeB.Start(); code != MOSS_OK {
		t.Fatalf("nodeB.Start failed: %d", code)
	}
	defer nodeB.Stop()

	waitForPeerCount(t, relay1, 2)
	waitForPeerCount(t, relay2, 2)
	waitForPeerCount(t, nodeA, 2)
	waitForPeerCount(t, nodeB, 2)

	targetPub := nodeB.PublicKey()
	targetID := hex.EncodeToString(targetPub[:])
	relay1Pub := relay1.PublicKey()
	relay1ID := hex.EncodeToString(relay1Pub[:])
	relay2Pub := relay2.PublicKey()
	relay2ID := hex.EncodeToString(relay2Pub[:])

	waitForKnownPeer(t, nodeA, targetID)
	nodeA.mu.Lock()
	info1 := nodeA.knownPeers[relay1ID]
	info1.relayCapable = true
	info1.publicReachable = true
	info1.natType = nat.TypePublic
	nodeA.knownPeers[relay1ID] = info1
	info2 := nodeA.knownPeers[relay2ID]
	info2.relayCapable = true
	info2.publicReachable = true
	info2.natType = nat.TypePublic
	nodeA.knownPeers[relay2ID] = info2
	nodeA.relayLocals["existing-1"] = relayLocalSession{sessionID: "existing-1", viaPeerID: relay1ID, remotePeerID: "other-1", established: true}
	nodeA.relayLocals["existing-2"] = relayLocalSession{sessionID: "existing-2", viaPeerID: relay1ID, remotePeerID: "other-2", established: true}
	nodeA.mu.Unlock()
	nodeA.scoring.SetApplicationScore(relay1ID, 10)
	nodeA.scoring.SetApplicationScore(relay2ID, 1)

	sessionID, err := nodeA.OpenRelaySessionAny(targetID, 2*time.Second)
	if err != nil {
		t.Fatalf("OpenRelaySessionAny failed: %v", err)
	}

	nodeA.mu.RLock()
	session := nodeA.relayLocals[sessionID]
	nodeA.mu.RUnlock()
	if session.viaPeerID != relay2ID {
		t.Fatalf("expected less-loaded relay %s, got %s", relay2ID, session.viaPeerID)
	}
}

func TestSupernodeDemotesWhenRelayCapacityIsSaturated(t *testing.T) {
	cfgA := DefaultConfig()
	cfgA.Trackers = nil
	cfgA.GossipSub.HeartbeatMS = 50
	cfgA.NAT.SuperNodeMinUptimeSec = 0
	cfgA.NAT.RelayMaxSessions = 1
	nodeA, err := NewNode("mesh-supernode-overload", nil, cfgA)
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
	cfgB.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(nodeA.ListenPort()))}
	nodeB, err := NewNode("mesh-supernode-overload", nil, cfgB)
	if err != nil {
		t.Fatalf("NewNode nodeB failed: %v", err)
	}
	if code := nodeB.Start(); code != MOSS_OK {
		t.Fatalf("nodeB.Start failed: %d", code)
	}
	defer nodeB.Stop()

	waitForPeerCount(t, nodeA, 1)
	waitForPeerCount(t, nodeB, 1)

	promoted := make(chan struct{}, 4)
	revoked := make(chan struct{}, 4)
	nodeA.SetEventCallback(func(eventType int32, detailJSON string) {
		switch eventType {
		case EventSupernodePromoted:
			promoted <- struct{}{}
		case EventSupernodeRevoked:
			revoked <- struct{}{}
		}
	})

	nodeAPub := nodeA.PublicKey()
	nodeAID := hex.EncodeToString(nodeAPub[:])
	waitForKnownPeer(t, nodeB, nodeAID)
	nodeA.natProfile.Store(nat.Profile{
		Type:            nat.TypePublic,
		PublicReachable: true,
		ExternalAddress: net.JoinHostPort("203.0.113.10", strconv.Itoa(nodeA.ListenPort())),
	})
	nodeA.refreshSupernodeStatus()
	waitForKnownPeerRelayCapable(t, nodeB, nodeAID, true)

	select {
	case <-promoted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for initial supernode promotion")
	}

	if !nodeA.relaySessions.Acquire("overload-session") {
		t.Fatal("expected to acquire relay session capacity")
	}
	nodeA.refreshSupernodeStatus()
	waitForKnownPeerRelayCapable(t, nodeB, nodeAID, false)

	select {
	case <-revoked:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for overload revoke")
	}

	nodeA.relaySessions.Release("overload-session")
	nodeA.refreshSupernodeStatus()
	waitForKnownPeerRelayCapable(t, nodeB, nodeAID, true)

	select {
	case <-promoted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for re-promotion after capacity recovery")
	}
}

func TestRefreshExternalAddressPreservesListenPort(t *testing.T) {
	cfgRelay := DefaultConfig()
	cfgRelay.Trackers = nil
	cfgRelay.GossipSub.HeartbeatMS = 50
	relayNode, err := NewNode("mesh-binding", nil, cfgRelay)
	if err != nil {
		t.Fatalf("NewNode relay failed: %v", err)
	}
	if code := relayNode.Start(); code != MOSS_OK {
		t.Fatalf("relayNode.Start failed: %d", code)
	}
	defer relayNode.Stop()

	cfgA := DefaultConfig()
	cfgA.Trackers = nil
	cfgA.GossipSub.HeartbeatMS = 50
	cfgA.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(relayNode.ListenPort()))}
	nodeA, err := NewNode("mesh-binding", nil, cfgA)
	if err != nil {
		t.Fatalf("NewNode nodeA failed: %v", err)
	}
	if code := nodeA.Start(); code != MOSS_OK {
		t.Fatalf("nodeA.Start failed: %d", code)
	}
	defer nodeA.Stop()

	waitForPeerCount(t, relayNode, 1)
	waitForPeerCount(t, nodeA, 1)

	if !nodeA.refreshExternalAddress(time.Now().Add(time.Second)) {
		t.Fatal("expected binding refresh to succeed through connected relay peer")
	}

	host, port, err := net.SplitHostPort(nodeA.advertisedListenAddr())
	if err != nil {
		t.Fatalf("advertisedListenAddr is invalid: %v", err)
	}
	if host == "" {
		t.Fatal("expected advertised host to be populated")
	}
	if port != strconv.Itoa(nodeA.ListenPort()) {
		t.Fatalf("expected binding refresh to preserve listen port %d, got %s", nodeA.ListenPort(), port)
	}
}

func TestReachabilityProbeReportsReachableAddress(t *testing.T) {
	cfgA := DefaultConfig()
	cfgA.Trackers = nil
	nodeA, err := NewNode("mesh-reachability", nil, cfgA)
	if err != nil {
		t.Fatalf("NewNode nodeA failed: %v", err)
	}
	if code := nodeA.Start(); code != MOSS_OK {
		t.Fatalf("nodeA.Start failed: %d", code)
	}
	defer nodeA.Stop()

	cfgB := DefaultConfig()
	cfgB.Trackers = nil
	cfgB.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(nodeA.ListenPort()))}
	nodeB, err := NewNode("mesh-reachability", nil, cfgB)
	if err != nil {
		t.Fatalf("NewNode nodeB failed: %v", err)
	}
	if code := nodeB.Start(); code != MOSS_OK {
		t.Fatalf("nodeB.Start failed: %d", code)
	}
	defer nodeB.Stop()

	waitForPeerCount(t, nodeA, 1)
	waitForPeerCount(t, nodeB, 1)

	proberPub := nodeB.PublicKey()
	proberID := hex.EncodeToString(proberPub[:])
	if !nodeA.requestReachabilityProbe(proberID, net.JoinHostPort("127.0.0.1", strconv.Itoa(nodeA.ListenPort())), time.Second) {
		t.Fatal("expected reachability probe to confirm reachable address")
	}
}

func TestReachabilityProbeReportsUnreachableAddress(t *testing.T) {
	cfgA := DefaultConfig()
	cfgA.Trackers = nil
	nodeA, err := NewNode("mesh-reachability-fail", nil, cfgA)
	if err != nil {
		t.Fatalf("NewNode nodeA failed: %v", err)
	}
	if code := nodeA.Start(); code != MOSS_OK {
		t.Fatalf("nodeA.Start failed: %d", code)
	}
	defer nodeA.Stop()

	cfgB := DefaultConfig()
	cfgB.Trackers = nil
	cfgB.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(nodeA.ListenPort()))}
	nodeB, err := NewNode("mesh-reachability-fail", nil, cfgB)
	if err != nil {
		t.Fatalf("NewNode nodeB failed: %v", err)
	}
	if code := nodeB.Start(); code != MOSS_OK {
		t.Fatalf("nodeB.Start failed: %d", code)
	}
	defer nodeB.Stop()

	waitForPeerCount(t, nodeA, 1)
	waitForPeerCount(t, nodeB, 1)

	proberPub := nodeB.PublicKey()
	proberID := hex.EncodeToString(proberPub[:])
	if nodeA.requestReachabilityProbe(proberID, "127.0.0.1:1", 500*time.Millisecond) {
		t.Fatal("expected reachability probe to fail for closed address")
	}
}

func TestDirectUDPConnectRegistersPeer(t *testing.T) {
	cfgA := DefaultConfig()
	cfgA.Trackers = nil
	nodeA, err := NewNode("mesh-udp-direct", nil, cfgA)
	if err != nil {
		t.Fatalf("NewNode nodeA failed: %v", err)
	}
	if code := nodeA.Start(); code != MOSS_OK {
		t.Fatalf("nodeA.Start failed: %d", code)
	}
	defer nodeA.Stop()

	cfgB := DefaultConfig()
	cfgB.Trackers = nil
	nodeB, err := NewNode("mesh-udp-direct", nil, cfgB)
	if err != nil {
		t.Fatalf("NewNode nodeB failed: %v", err)
	}
	if code := nodeB.Start(); code != MOSS_OK {
		t.Fatalf("nodeB.Start failed: %d", code)
	}
	defer nodeB.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	targetPub := nodeB.PublicKey()
	nodeA.connectPeerUDP(ctx, hex.EncodeToString(targetPub[:]), net.JoinHostPort("127.0.0.1", strconv.Itoa(nodeB.ListenPort())))
	cancel()

	sourcePub := nodeA.PublicKey()
	waitForDirectPeerWithin(t, nodeA, hex.EncodeToString(targetPub[:]), 10*time.Second)
	waitForDirectPeerWithin(t, nodeB, hex.EncodeToString(sourcePub[:]), 10*time.Second)
}

func TestTryHolePunchDialEstablishesDirectPeer(t *testing.T) {
	cfgRelay := DefaultConfig()
	cfgRelay.Trackers = nil
	cfgRelay.GossipSub.HeartbeatMS = 50
	relayNode, err := NewNode("mesh-holepunch", nil, cfgRelay)
	if err != nil {
		t.Fatalf("NewNode relay failed: %v", err)
	}
	if code := relayNode.Start(); code != MOSS_OK {
		t.Fatalf("relayNode.Start failed: %d", code)
	}
	defer relayNode.Stop()

	cfgA := DefaultConfig()
	cfgA.Trackers = nil
	cfgA.GossipSub.HeartbeatMS = 50
	cfgA.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(relayNode.ListenPort()))}
	nodeA, err := NewNode("mesh-holepunch", nil, cfgA)
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
	cfgB.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(relayNode.ListenPort()))}
	nodeB, err := NewNode("mesh-holepunch", nil, cfgB)
	if err != nil {
		t.Fatalf("NewNode nodeB failed: %v", err)
	}
	if code := nodeB.Start(); code != MOSS_OK {
		t.Fatalf("nodeB.Start failed: %d", code)
	}
	defer nodeB.Stop()

	waitForPeerCount(t, relayNode, 2)
	targetPub := nodeB.PublicKey()
	targetID := hex.EncodeToString(targetPub[:])
	waitForKnownPeer(t, nodeA, targetID)
	waitForKnownPeerPort(t, nodeA, targetID, strconv.Itoa(nodeB.ListenPort()))
	sourcePub := nodeA.PublicKey()
	waitForKnownPeerPort(t, nodeB, hex.EncodeToString(sourcePub[:]), strconv.Itoa(nodeA.ListenPort()))

	nodeA.mu.RLock()
	targetAddr := nodeA.knownPeers[targetID].addr
	nodeA.mu.RUnlock()
	for attempt := 0; attempt < 5 && !nodeA.directPeerConnected(targetID); attempt++ {
		nodeA.tryHolePunchDial(targetID, targetAddr)
		time.Sleep(150 * time.Millisecond)
	}

	waitForDirectPeer(t, nodeA, targetID)
	waitForDirectPeer(t, nodeB, hex.EncodeToString(sourcePub[:]))
}

func TestHandleHolePunchCoordIgnoresUnsolicitedRequest(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	node, err := NewNode("mesh-holepunch-unsolicited", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}

	localID := node.localPeerID()
	peer := &peerConn{id: "relay-peer"}

	node.mu.RLock()
	before, hadBefore := node.knownPeers["attacker-source"]
	node.mu.RUnlock()

	node.handleHolePunchCoord(peer, gossip.Envelope{
		Type:           gossip.TypeHolePunchCoord,
		RequestID:      "attacker-request",
		CoordStage:     "reply",
		RelaySource:    "attacker-source",
		RelayTarget:    localID,
		AdvertisedAddr: "127.0.0.1:6553",
	})

	node.mu.RLock()
	after, hadAfter := node.knownPeers["attacker-source"]
	_, pending := node.holePunchWait["attacker-request"]
	node.mu.RUnlock()

	if hadAfter != hadBefore || after.addr != before.addr {
		t.Fatalf("unexpected known peer update for unsolicited request")
	}
	if pending {
		t.Fatalf("unexpected pending hole punch request for unsolicited request")
	}
}

func TestHandleHolePunchCoordKeepsPendingRequestAfterMismatchedReply(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	node, err := NewNode("mesh-holepunch-mismatch", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}

	localID := node.localPeerID()
	node.mu.Lock()
	node.holePunchWait["req-1"] = holePunchRequest{targetPeerID: "target-peer", relayPeerID: "relay-peer"}
	node.mu.Unlock()

	node.handleHolePunchCoord(&peerConn{id: "attacker-relay"}, gossip.Envelope{
		Type:           gossip.TypeHolePunchCoord,
		RequestID:      "req-1",
		CoordStage:     "reply",
		RelaySource:    "attacker-source",
		RelayTarget:    localID,
		AdvertisedAddr: "127.0.0.1:6553",
	})

	node.mu.RLock()
	_, stillPending := node.holePunchWait["req-1"]
	_, attackerKnown := node.knownPeers["attacker-source"]
	node.mu.RUnlock()
	if !stillPending {
		t.Fatalf("mismatched reply consumed pending request")
	}
	if attackerKnown {
		t.Fatalf("mismatched reply updated attacker peer")
	}

	node.handleHolePunchCoord(&peerConn{id: "relay-peer"}, gossip.Envelope{
		Type:           gossip.TypeHolePunchCoord,
		RequestID:      "req-1",
		CoordStage:     "reply",
		RelaySource:    "target-peer",
		RelayTarget:    localID,
		AdvertisedAddr: "127.0.0.1:6554",
	})

	node.mu.RLock()
	_, stillPending = node.holePunchWait["req-1"]
	known := node.knownPeers["target-peer"]
	node.mu.RUnlock()
	if stillPending {
		t.Fatalf("valid reply did not consume pending request")
	}
	if known.addr != "127.0.0.1:6554" {
		t.Fatalf("valid reply did not update target peer: %q", known.addr)
	}
}

func TestDirectPeerConnectionMigratesRelaySession(t *testing.T) {
	cfgRelay := DefaultConfig()
	cfgRelay.Trackers = nil
	cfgRelay.GossipSub.HeartbeatMS = 50
	relayNode, err := NewNode("mesh-migrate", nil, cfgRelay)
	if err != nil {
		t.Fatalf("NewNode relay failed: %v", err)
	}
	if code := relayNode.Start(); code != MOSS_OK {
		t.Fatalf("relayNode.Start failed: %d", code)
	}
	defer relayNode.Stop()

	cfgA := DefaultConfig()
	cfgA.Trackers = nil
	cfgA.GossipSub.HeartbeatMS = 50
	cfgA.MaxPeers = 1
	cfgA.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(relayNode.ListenPort()))}
	nodeA, err := NewNode("mesh-migrate", nil, cfgA)
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
	nodeB, err := NewNode("mesh-migrate", nil, cfgB)
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

	relayPub := relayNode.PublicKey()
	targetPub := nodeB.PublicKey()
	targetID := hex.EncodeToString(targetPub[:])
	waitForKnownPeerPort(t, nodeA, targetID, strconv.Itoa(nodeB.ListenPort()))
	sessionID, err := nodeA.OpenRelaySession(hex.EncodeToString(relayPub[:]), hex.EncodeToString(targetPub[:]), 2*time.Second)
	if err != nil {
		t.Fatalf("OpenRelaySession failed: %v", err)
	}
	waitForRelaySession(t, nodeA, sessionID)
	waitForRelaySession(t, nodeB, sessionID)
	waitForRelayRoute(t, relayNode, sessionID)
	nodeA.config.MaxPeers = 2
	nodeB.config.MaxPeers = 2

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	nodeA.connectPeer(ctx, net.JoinHostPort("127.0.0.1", strconv.Itoa(nodeB.ListenPort())))
	cancel()

	sourcePub := nodeA.PublicKey()
	waitForKnownPeerPort(t, nodeB, hex.EncodeToString(sourcePub[:]), strconv.Itoa(nodeA.ListenPort()))
	waitForDirectPeer(t, nodeA, targetID)
	waitForDirectPeer(t, nodeB, hex.EncodeToString(sourcePub[:]))
	waitForRelaySessionClosed(t, nodeA, sessionID)
	waitForRelaySessionClosed(t, nodeB, sessionID)
	waitForRelayRouteClosed(t, relayNode, sessionID)
}

func TestRelaySessionAutoPromotesToDirectConnection(t *testing.T) {
	cfgRelay := DefaultConfig()
	cfgRelay.Trackers = nil
	cfgRelay.GossipSub.HeartbeatMS = 50
	relayNode, err := NewNode("mesh-auto-migrate", nil, cfgRelay)
	if err != nil {
		t.Fatalf("NewNode relay failed: %v", err)
	}
	if code := relayNode.Start(); code != MOSS_OK {
		t.Fatalf("relayNode.Start failed: %d", code)
	}
	defer relayNode.Stop()

	cfgA := DefaultConfig()
	cfgA.Trackers = nil
	cfgA.GossipSub.HeartbeatMS = 50
	cfgA.MaxPeers = 1
	cfgA.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(relayNode.ListenPort()))}
	nodeA, err := NewNode("mesh-auto-migrate", nil, cfgA)
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
	nodeB, err := NewNode("mesh-auto-migrate", nil, cfgB)
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

	relayPub := relayNode.PublicKey()
	targetPub := nodeB.PublicKey()
	sessionID, err := nodeA.OpenRelaySession(hex.EncodeToString(relayPub[:]), hex.EncodeToString(targetPub[:]), 2*time.Second)
	if err != nil {
		t.Fatalf("OpenRelaySession failed: %v", err)
	}
	waitForRelaySession(t, nodeA, sessionID)
	waitForRelaySession(t, nodeB, sessionID)
	waitForRelayRoute(t, relayNode, sessionID)
	nodeA.config.MaxPeers = 2
	nodeB.config.MaxPeers = 2

	sourcePub := nodeA.PublicKey()
	waitForDirectPeer(t, nodeA, hex.EncodeToString(targetPub[:]))
	waitForDirectPeer(t, nodeB, hex.EncodeToString(sourcePub[:]))
	waitForRelaySessionClosed(t, nodeA, sessionID)
	waitForRelaySessionClosed(t, nodeB, sessionID)
	waitForRelayRouteClosed(t, relayNode, sessionID)
}

func TestOpportunisticGraftingPrefersHighScoringNonMeshPeer(t *testing.T) {
	cfgA := DefaultConfig()
	cfgA.Trackers = nil
	cfgA.GossipSub.HeartbeatMS = 50
	cfgA.GossipSub.D = 2
	cfgA.GossipSub.DLo = 2
	cfgA.GossipSub.DHigh = 2
	cfgA.GossipSub.DLazy = 1
	nodeA, err := NewNode("mesh-graft", nil, cfgA)
	if err != nil {
		t.Fatalf("NewNode nodeA failed: %v", err)
	}
	if code := nodeA.Start(); code != MOSS_OK {
		t.Fatalf("nodeA.Start failed: %d", code)
	}
	defer nodeA.Stop()

	makePeer := func() *Node {
		cfg := DefaultConfig()
		cfg.Trackers = nil
		cfg.LANDiscoveryEnabled = false
		cfg.GossipSub.HeartbeatMS = 50
		cfg.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(nodeA.ListenPort()))}
		node, err := NewNode("mesh-graft", nil, cfg)
		if err != nil {
			t.Fatalf("NewNode peer failed: %v", err)
		}
		if code := node.Start(); code != MOSS_OK {
			t.Fatalf("peer.Start failed: %d", code)
		}
		return node
	}

	nodeB := makePeer()
	defer nodeB.Stop()
	nodeC := makePeer()
	defer nodeC.Stop()
	nodeD := makePeer()
	defer nodeD.Stop()

	waitForPeerCount(t, nodeA, 3)
	if code := nodeA.Subscribe("alpha"); code != MOSS_OK {
		t.Fatalf("nodeA.Subscribe failed: %d", code)
	}
	for _, node := range []*Node{nodeB, nodeC, nodeD} {
		if code := node.Subscribe("alpha"); code != MOSS_OK {
			t.Fatalf("peer.Subscribe failed: %d", code)
		}
	}
	waitForMeshCountAtLeast(t, nodeA, "alpha", 2)

	meshPeers := nodeA.pubsub.MeshPeers("alpha")
	if len(meshPeers) > 2 {
		nodeA.pubsub.SetMeshPeer("alpha", meshPeers[len(meshPeers)-1], false)
		meshPeers = nodeA.pubsub.MeshPeers("alpha")
	}
	meshSet := make(map[string]struct{}, len(meshPeers))
	for _, peerID := range meshPeers {
		meshSet[peerID] = struct{}{}
	}
	pubB := nodeB.PublicKey()
	pubC := nodeC.PublicKey()
	pubD := nodeD.PublicKey()
	peerIDs := []string{
		hex.EncodeToString(pubB[:]),
		hex.EncodeToString(pubC[:]),
		hex.EncodeToString(pubD[:]),
	}
	nonMeshPeer := ""
	for _, peerID := range peerIDs {
		if _, ok := meshSet[peerID]; !ok {
			nonMeshPeer = peerID
			break
		}
	}
	if nonMeshPeer == "" {
		t.Fatal("expected one non-mesh peer")
	}
	for _, peerID := range meshPeers {
		nodeA.scoring.SetApplicationScore(peerID, -2)
	}
	nodeA.scoring.SetApplicationScore(nonMeshPeer, 3)

	nodeA.maintainTopicMesh("alpha")

	deadline := time.Now().Add(2 * time.Second)
	for {
		updatedMesh := nodeA.pubsub.MeshPeers("alpha")
		for _, peerID := range updatedMesh {
			if peerID == nonMeshPeer {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected high-scoring non-mesh peer %s to join mesh, got %v", nonMeshPeer, updatedMesh)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestFiveNodeMeshPropagatesPublishedMessage(t *testing.T) {
	cfg0 := DefaultConfig()
	cfg0.Trackers = nil
	cfg0.GossipSub.HeartbeatMS = 50
	root, err := NewNode("mesh-five", nil, cfg0)
	if err != nil {
		t.Fatalf("NewNode root failed: %v", err)
	}
	if code := root.Start(); code != MOSS_OK {
		t.Fatalf("root.Start failed: %d", code)
	}
	defer root.Stop()

	nodes := []*Node{root}
	for i := 0; i < 4; i++ {
		cfg := DefaultConfig()
		cfg.Trackers = nil
		cfg.GossipSub.HeartbeatMS = 50
		cfg.MaxPeers = 1
		cfg.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(root.ListenPort()))}
		node, err := NewNode("mesh-five", nil, cfg)
		if err != nil {
			t.Fatalf("NewNode peer %d failed: %v", i, err)
		}
		if code := node.Start(); code != MOSS_OK {
			t.Fatalf("peer %d Start failed: %d", i, code)
		}
		defer node.Stop()
		nodes = append(nodes, node)
	}

	waitForPeerCount(t, root, 4)
	for _, node := range nodes[1:] {
		waitForPeerCount(t, node, 1)
	}

	received := make(chan string, 4)
	for i, node := range nodes[:4] {
		if code := node.Subscribe("alpha"); code != MOSS_OK {
			t.Fatalf("node %d Subscribe failed: %d", i, code)
		}
		index := i
		node.SetMessageCallback(func(channel string, senderID [32]byte, data []byte) {
			if channel == "alpha" && string(data) == "fanout" {
				received <- strconv.Itoa(index)
			}
		})
	}
	if code := nodes[4].Subscribe("alpha"); code != MOSS_OK {
		t.Fatalf("publisher Subscribe failed: %d", code)
	}
	time.Sleep(250 * time.Millisecond)

	if code := nodes[4].Publish("alpha", []byte("fanout")); code != MOSS_OK {
		t.Fatalf("publisher Publish failed: %d", code)
	}

	seen := make(map[string]struct{}, 4)
	deadline := time.After(4 * time.Second)
	for len(seen) < 4 {
		select {
		case idx := <-received:
			seen[idx] = struct{}{}
		case <-deadline:
			t.Fatalf("timed out waiting for 4 subscribers, got %d", len(seen))
		}
	}
}

func TestLocalPublishFloodsToNonMeshSubscribers(t *testing.T) {
	cfgRoot := DefaultConfig()
	cfgRoot.Trackers = nil
	cfgRoot.GossipSub.HeartbeatMS = 50
	cfgRoot.GossipSub.D = 1
	cfgRoot.GossipSub.DLo = 1
	cfgRoot.GossipSub.DHigh = 1
	root, err := NewNode("mesh-flood-publish", nil, cfgRoot)
	if err != nil {
		t.Fatalf("NewNode root failed: %v", err)
	}
	if code := root.Start(); code != MOSS_OK {
		t.Fatalf("root.Start failed: %d", code)
	}
	defer root.Stop()

	nodes := []*Node{root}
	for i := 0; i < 3; i++ {
		cfg := DefaultConfig()
		cfg.Trackers = nil
		cfg.GossipSub.HeartbeatMS = 50
		cfg.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(root.ListenPort()))}
		node, err := NewNode("mesh-flood-publish", nil, cfg)
		if err != nil {
			t.Fatalf("NewNode peer %d failed: %v", i, err)
		}
		if code := node.Start(); code != MOSS_OK {
			t.Fatalf("peer %d Start failed: %d", i, code)
		}
		defer node.Stop()
		nodes = append(nodes, node)
	}

	waitForPeerCount(t, root, 3)
	if code := root.Subscribe("alpha"); code != MOSS_OK {
		t.Fatalf("root Subscribe failed: %d", code)
	}
	received := make(chan string, 3)
	for i, node := range nodes[1:] {
		if code := node.Subscribe("alpha"); code != MOSS_OK {
			t.Fatalf("node %d Subscribe failed: %d", i+1, code)
		}
		index := i + 1
		node.SetMessageCallback(func(channel string, senderID [32]byte, data []byte) {
			if channel == "alpha" && string(data) == "flood-local" {
				received <- strconv.Itoa(index)
			}
		})
	}
	time.Sleep(100 * time.Millisecond)

	if code := root.Publish("alpha", []byte("flood-local")); code != MOSS_OK {
		t.Fatalf("root Publish failed: %d", code)
	}

	seen := make(map[string]struct{}, 3)
	deadline := time.After(2 * time.Second)
	for len(seen) < 3 {
		select {
		case idx := <-received:
			seen[idx] = struct{}{}
		case <-deadline:
			t.Fatalf("timed out waiting for 3 subscribers, got %d", len(seen))
		}
	}
}

func TestTenNodeLanPublishPropagatesToAllSubscribers(t *testing.T) {
	cfgRoot := DefaultConfig()
	cfgRoot.Trackers = nil
	cfgRoot.GossipSub.HeartbeatMS = 50
	cfgRoot.MaxPeers = 16
	root, err := NewNode("mesh-ten-latency", nil, cfgRoot)
	if err != nil {
		t.Fatalf("NewNode root failed: %v", err)
	}
	if code := root.Start(); code != MOSS_OK {
		t.Fatalf("root.Start failed: %d", code)
	}
	defer root.Stop()

	nodes := []*Node{root}
	for i := 0; i < 9; i++ {
		cfg := DefaultConfig()
		cfg.Trackers = nil
		cfg.GossipSub.HeartbeatMS = 50
		cfg.MaxPeers = 1
		cfg.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(root.ListenPort()))}
		node, err := NewNode("mesh-ten-latency", nil, cfg)
		if err != nil {
			t.Fatalf("NewNode peer %d failed: %v", i, err)
		}
		if code := node.Start(); code != MOSS_OK {
			t.Fatalf("peer %d Start failed: %d", i, code)
		}
		defer node.Stop()
		nodes = append(nodes, node)
	}

	waitForPeerCount(t, root, 9)
	for _, node := range nodes[1:] {
		waitForPeerCount(t, node, 1)
	}

	if code := root.Subscribe("alpha"); code != MOSS_OK {
		t.Fatalf("root Subscribe failed: %d", code)
	}

	for i, node := range nodes[1:] {
		if code := node.Subscribe("alpha"); code != MOSS_OK {
			t.Fatalf("node %d Subscribe failed: %d", i+1, code)
		}
	}
	time.Sleep(100 * time.Millisecond)

	start := time.Now()
	if code := root.Publish("alpha", []byte("ten-node-fanout")); code != MOSS_OK {
		t.Fatalf("root Publish failed: %d", code)
	}

	seen := make(map[int]struct{}, 9)
	deadline := time.Now().Add(2 * time.Second)
	for len(seen) < 9 && time.Now().Before(deadline) {
		for i, node := range nodes[1:] {
			if _, ok := seen[i+1]; ok {
				continue
			}
			if nodeHasCachedPayload(node, "alpha", "ten-node-fanout") {
				seen[i+1] = struct{}{}
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	if len(seen) < 9 {
		t.Fatalf("timed out waiting for 9 subscribers, got %d", len(seen))
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("expected 10-node LAN publish within 2s, got %s", elapsed)
	}
}

func TestRelaySessionEstablishesWithinFiveSeconds(t *testing.T) {
	cfgRelay := DefaultConfig()
	cfgRelay.Trackers = nil
	cfgRelay.GossipSub.HeartbeatMS = 50
	relayNode, err := NewNode("mesh-relay-latency", nil, cfgRelay)
	if err != nil {
		t.Fatalf("NewNode relay failed: %v", err)
	}
	if code := relayNode.Start(); code != MOSS_OK {
		t.Fatalf("relayNode.Start failed: %d", code)
	}
	defer relayNode.Stop()

	cfgA := DefaultConfig()
	cfgA.Trackers = nil
	cfgA.GossipSub.HeartbeatMS = 50
	cfgA.MaxPeers = 1
	cfgA.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(relayNode.ListenPort()))}
	nodeA, err := NewNode("mesh-relay-latency", nil, cfgA)
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
	nodeB, err := NewNode("mesh-relay-latency", nil, cfgB)
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

	relayPub := relayNode.PublicKey()
	targetPub := nodeB.PublicKey()
	start := time.Now()
	sessionID, err := nodeA.OpenRelaySession(hex.EncodeToString(relayPub[:]), hex.EncodeToString(targetPub[:]), 5*time.Second)
	if err != nil {
		t.Fatalf("OpenRelaySession failed: %v", err)
	}
	waitForRelaySession(t, nodeA, sessionID)
	waitForRelaySession(t, nodeB, sessionID)
	waitForRelayRoute(t, relayNode, sessionID)
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("expected relay session establishment within 5s, got %s", elapsed)
	}
}

func TestPeerLatencyProbeUpdatesRTT(t *testing.T) {
	cfgA := DefaultConfig()
	cfgA.Trackers = nil
	cfgA.GossipSub.HeartbeatMS = 50
	nodeA, err := NewNode("mesh-rtt", nil, cfgA)
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
	cfgB.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(nodeA.ListenPort()))}
	nodeB, err := NewNode("mesh-rtt", nil, cfgB)
	if err != nil {
		t.Fatalf("NewNode nodeB failed: %v", err)
	}
	if code := nodeB.Start(); code != MOSS_OK {
		t.Fatalf("nodeB.Start failed: %d", code)
	}
	defer nodeB.Stop()

	waitForPeerCount(t, nodeA, 1)
	waitForPeerCount(t, nodeB, 1)

	targetPub := nodeB.PublicKey()
	targetID := hex.EncodeToString(targetPub[:])
	nodeA.probePeerLatency(time.Now())
	waitForPeerRTT(t, nodeA, targetID, 2*time.Second)
}

func TestInboundConnectionsRespectMaxPeers(t *testing.T) {
	cfgHub := DefaultConfig()
	cfgHub.Trackers = nil
	cfgHub.LANDiscoveryEnabled = false
	cfgHub.GossipSub.HeartbeatMS = 50
	cfgHub.MaxPeers = 1
	hub, err := NewNode("mesh-max-peers", nil, cfgHub)
	if err != nil {
		t.Fatalf("NewNode hub failed: %v", err)
	}
	if code := hub.Start(); code != MOSS_OK {
		t.Fatalf("hub.Start failed: %d", code)
	}
	defer hub.Stop()

	cfgA := DefaultConfig()
	cfgA.Trackers = nil
	cfgA.LANDiscoveryEnabled = false
	cfgA.GossipSub.HeartbeatMS = 50
	cfgA.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(hub.ListenPort()))}
	nodeA, err := NewNode("mesh-max-peers", nil, cfgA)
	if err != nil {
		t.Fatalf("NewNode nodeA failed: %v", err)
	}
	if code := nodeA.Start(); code != MOSS_OK {
		t.Fatalf("nodeA.Start failed: %d", code)
	}
	defer nodeA.Stop()

	waitForPeerCount(t, hub, 1)
	waitForPeerCount(t, nodeA, 1)

	cfgB := DefaultConfig()
	cfgB.Trackers = nil
	cfgB.LANDiscoveryEnabled = false
	cfgB.GossipSub.HeartbeatMS = 50
	cfgB.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(hub.ListenPort()))}
	nodeB, err := NewNode("mesh-max-peers", nil, cfgB)
	if err != nil {
		t.Fatalf("NewNode nodeB failed: %v", err)
	}
	if code := nodeB.Start(); code != MOSS_OK {
		t.Fatalf("nodeB.Start failed: %d", code)
	}
	defer nodeB.Stop()

	waitForPeerCountAtMost(t, hub, 1, 1500*time.Millisecond)
	waitForPeerCountEventually(t, nodeB, 0)
	if got := hub.currentPeerCount(); got != 1 {
		t.Fatalf("expected hub to keep exactly 1 peer, got %d", got)
	}
}

func TestHighLatencyPeerIsPruned(t *testing.T) {
	cfgA := DefaultConfig()
	cfgA.Trackers = nil
	cfgA.GossipSub.HeartbeatMS = 50
	nodeA, err := NewNode("mesh-prune-latency", nil, cfgA)
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
	cfgB.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(nodeA.ListenPort()))}
	nodeB, err := NewNode("mesh-prune-latency", nil, cfgB)
	if err != nil {
		t.Fatalf("NewNode nodeB failed: %v", err)
	}
	if code := nodeB.Start(); code != MOSS_OK {
		t.Fatalf("nodeB.Start failed: %d", code)
	}
	defer nodeB.Stop()

	waitForPeerCount(t, nodeA, 1)
	waitForPeerCount(t, nodeB, 1)
	if code := nodeA.Subscribe("alpha"); code != MOSS_OK {
		t.Fatalf("nodeA.Subscribe failed: %d", code)
	}
	if code := nodeB.Subscribe("alpha"); code != MOSS_OK {
		t.Fatalf("nodeB.Subscribe failed: %d", code)
	}

	targetPub := nodeB.PublicKey()
	targetID := hex.EncodeToString(targetPub[:])
	waitForPeerMeshState(t, nodeA, "alpha", targetID, true)
	nodeA.mu.Lock()
	if peer := nodeA.peers[targetID]; peer != nil {
		peer.connectedAt = time.Now().Add(-time.Minute)
		peer.lastRTT = 3 * time.Second
	}
	nodeA.mu.Unlock()

	nodeA.pruneHighLatencyPeers()
	waitForPeerMeshState(t, nodeA, "alpha", targetID, false)
	waitForPeerCountAtLeast(t, nodeA, 1, 500*time.Millisecond)
}

func TestNegativeScorePeerIsPrunedFromMeshWithoutDisconnect(t *testing.T) {
	cfgA := DefaultConfig()
	cfgA.Trackers = nil
	cfgA.GossipSub.HeartbeatMS = 50
	nodeA, err := NewNode("mesh-negative-score", nil, cfgA)
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
	cfgB.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(nodeA.ListenPort()))}
	nodeB, err := NewNode("mesh-negative-score", nil, cfgB)
	if err != nil {
		t.Fatalf("NewNode nodeB failed: %v", err)
	}
	if code := nodeB.Start(); code != MOSS_OK {
		t.Fatalf("nodeB.Start failed: %d", code)
	}
	defer nodeB.Stop()

	waitForPeerCount(t, nodeA, 1)
	waitForPeerCount(t, nodeB, 1)
	if code := nodeA.Subscribe("alpha"); code != MOSS_OK {
		t.Fatalf("nodeA.Subscribe failed: %d", code)
	}
	if code := nodeB.Subscribe("alpha"); code != MOSS_OK {
		t.Fatalf("nodeB.Subscribe failed: %d", code)
	}

	targetPub := nodeB.PublicKey()
	targetID := hex.EncodeToString(targetPub[:])
	waitForPeerMeshState(t, nodeA, "alpha", targetID, true)

	nodeA.mu.Lock()
	if peer := nodeA.peers[targetID]; peer != nil {
		peer.connectedAt = time.Now().Add(-time.Minute)
	}
	nodeA.mu.Unlock()
	nodeA.scoring.SetApplicationScore(targetID, -5)

	nodeA.pruneLowScoringPeers()

	waitForPeerMeshState(t, nodeA, "alpha", targetID, false)
	waitForPeerCountAtLeast(t, nodeA, 1, 500*time.Millisecond)
}

func TestSimultaneousDirectDialsResolveToSingleConnection(t *testing.T) {
	cfgA := DefaultConfig()
	cfgA.Trackers = nil
	cfgA.GossipSub.HeartbeatMS = 50
	nodeA, err := NewNode("mesh-simultaneous-dial", nil, cfgA)
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
	nodeB, err := NewNode("mesh-simultaneous-dial", nil, cfgB)
	if err != nil {
		t.Fatalf("NewNode nodeB failed: %v", err)
	}
	if code := nodeB.Start(); code != MOSS_OK {
		t.Fatalf("nodeB.Start failed: %d", code)
	}
	defer nodeB.Stop()

	addrA := net.JoinHostPort("127.0.0.1", strconv.Itoa(nodeA.ListenPort()))
	addrB := net.JoinHostPort("127.0.0.1", strconv.Itoa(nodeB.ListenPort()))
	done := make(chan struct{}, 2)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = nodeA.connectPeer(ctx, addrB)
		done <- struct{}{}
	}()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = nodeB.connectPeer(ctx, addrA)
		done <- struct{}{}
	}()
	<-done
	<-done

	targetPub := nodeB.PublicKey()
	sourcePub := nodeA.PublicKey()
	waitForDirectPeer(t, nodeA, hex.EncodeToString(targetPub[:]))
	waitForDirectPeer(t, nodeB, hex.EncodeToString(sourcePub[:]))
	waitForPeerCountAtMost(t, nodeA, 1, 500*time.Millisecond)
	waitForPeerCountAtMost(t, nodeB, 1, 500*time.Millisecond)
}

func waitForPeerCount(t *testing.T, node *Node, want int) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		var info struct {
			PeerCount int `json:"peer_count"`
		}
		if err := json.Unmarshal([]byte(node.MeshInfoJSON()), &info); err == nil && info.PeerCount >= want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("peer count did not reach %d; info=%s", want, node.MeshInfoJSON())
}

func waitForKnownPeerAddr(t *testing.T, node *Node, peerID, want string) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		node.mu.RLock()
		info, ok := node.knownPeers[peerID]
		node.mu.RUnlock()
		if ok && info.addr == want {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("known peer %s addr did not converge to %s; info=%s", peerID, want, node.MeshInfoJSON())
}

func waitForPeerCountWithin(node *Node, want int, dur time.Duration) bool {
	deadline := time.Now().Add(dur)
	for time.Now().Before(deadline) {
		var info struct {
			PeerCount int `json:"peer_count"`
		}
		if err := json.Unmarshal([]byte(node.MeshInfoJSON()), &info); err == nil && info.PeerCount >= want {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}

func waitForPeerCountEventually(t *testing.T, node *Node, want int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if node.currentPeerCount() == want {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for peer count %d; info=%s", want, node.MeshInfoJSON())
}

func waitForPeerCountAtMost(t *testing.T, node *Node, max int, dur time.Duration) {
	t.Helper()
	deadline := time.Now().Add(dur)
	for time.Now().Before(deadline) {
		if got := node.currentPeerCount(); got > max {
			t.Fatalf("peer count exceeded limit: got %d want <= %d; info=%s", got, max, node.MeshInfoJSON())
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func waitForPeerCountAtLeast(t *testing.T, node *Node, min int, dur time.Duration) {
	t.Helper()
	deadline := time.Now().Add(dur)
	for time.Now().Before(deadline) {
		if got := node.currentPeerCount(); got >= min {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("peer count stayed below %d; info=%s", min, node.MeshInfoJSON())
}

func waitForSubscriberCount(t *testing.T, node *Node, channel string, want int) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if got := len(node.pubsub.Subscribers(channel)); got >= want {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("subscriber count for %s did not reach %d", channel, want)
}

func waitForMeshCount(t *testing.T, node *Node, channel string, want int) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if len(node.pubsub.MeshPeers(channel)) == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("mesh size did not reach %d for channel %s", want, channel)
}

func waitForMeshCountAtLeast(t *testing.T, node *Node, channel string, want int) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if len(node.pubsub.MeshPeers(channel)) >= want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("mesh size did not reach at least %d for channel %s", want, channel)
}

func waitForKnownPeer(t *testing.T, node *Node, wantPeerID string) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		node.mu.RLock()
		_, ok := node.knownPeers[wantPeerID]
		node.mu.RUnlock()
		if ok {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("known peer %s was not discovered", wantPeerID)
}

func waitForDirectPeer(t *testing.T, node *Node, wantPeerID string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		node.mu.RLock()
		_, ok := node.peers[wantPeerID]
		node.mu.RUnlock()
		if ok {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("direct peer %s was not connected", wantPeerID)
}

func waitForDirectPeerWithin(t *testing.T, node *Node, wantPeerID string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		node.mu.RLock()
		_, ok := node.peers[wantPeerID]
		node.mu.RUnlock()
		if ok {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("direct peer %s was not connected within %s", wantPeerID, timeout)
}

func waitForPeerGone(t *testing.T, node *Node, peerID string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		node.mu.RLock()
		_, ok := node.peers[peerID]
		node.mu.RUnlock()
		if !ok {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("peer %s was not pruned", peerID)
}

func waitForPeerRTT(t *testing.T, node *Node, peerID string, max time.Duration) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		node.mu.RLock()
		peer := node.peers[peerID]
		var rtt time.Duration
		if peer != nil {
			rtt = peer.lastRTT
		}
		node.mu.RUnlock()
		if rtt > 0 && rtt <= max {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("peer %s did not report RTT within %s", peerID, max)
}

func waitForPeerMeshState(t *testing.T, node *Node, channel, peerID string, want bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if node.pubsub.InMesh(channel, peerID) == want {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("peer %s mesh state for %s did not become %t", peerID, channel, want)
}

func waitForKnownPeerPort(t *testing.T, node *Node, wantPeerID, wantPort string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		node.mu.RLock()
		info, ok := node.knownPeers[wantPeerID]
		node.mu.RUnlock()
		if ok {
			_, port, err := net.SplitHostPort(info.addr)
			if err == nil && port == wantPort {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("known peer %s did not converge to port %s", wantPeerID, wantPort)
}

func waitForKnownPeerRelayCapable(t *testing.T, node *Node, wantPeerID string, want bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		node.mu.RLock()
		info, ok := node.knownPeers[wantPeerID]
		node.mu.RUnlock()
		if ok && info.relayCapable == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("known peer %s did not converge to relayCapable=%t", wantPeerID, want)
}

type compactTracker struct {
	server *httptest.Server
	mu     sync.RWMutex
	peers  []string
}

func newCompactTracker() *compactTracker {
	ct := &compactTracker{}
	ct.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ct.mu.RLock()
		peers := append([]string(nil), ct.peers...)
		ct.mu.RUnlock()
		compact := make([]byte, 0, len(peers)*6)
		for _, peer := range peers {
			host, port, err := net.SplitHostPort(peer)
			if err != nil {
				continue
			}
			ip := net.ParseIP(host).To4()
			if ip == nil {
				continue
			}
			portNum, err := strconv.Atoi(port)
			if err != nil {
				continue
			}
			entry := make([]byte, 6)
			copy(entry[:4], ip)
			binary.BigEndian.PutUint16(entry[4:6], uint16(portNum))
			compact = append(compact, entry...)
		}
		_, _ = w.Write([]byte("d8:intervali1e5:peers" + strconv.Itoa(len(compact)) + ":" + string(compact) + "e"))
	}))
	return ct
}

func (ct *compactTracker) SetPeers(peers []string) {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	ct.peers = append([]string(nil), peers...)
}

func (ct *compactTracker) URL() string {
	return ct.server.URL
}

func (ct *compactTracker) Close() {
	ct.server.Close()
}

func waitForRelaySession(t *testing.T, node *Node, sessionID string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		node.mu.RLock()
		session, ok := node.relayLocals[sessionID]
		node.mu.RUnlock()
		if ok && session.established {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("relay session %s was not established", sessionID)
}

func waitForRelaySessionClosed(t *testing.T, node *Node, sessionID string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		node.mu.RLock()
		_, ok := node.relayLocals[sessionID]
		node.mu.RUnlock()
		if !ok {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("relay session %s did not close", sessionID)
}

func waitForRelayRoute(t *testing.T, node *Node, sessionID string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		node.mu.RLock()
		_, ok := node.relayRoutes[sessionID]
		node.mu.RUnlock()
		if ok {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("relay route %s was not created", sessionID)
}

func waitForRelayRouteClosed(t *testing.T, node *Node, sessionID string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		node.mu.RLock()
		_, ok := node.relayRoutes[sessionID]
		node.mu.RUnlock()
		if !ok {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("relay route %s did not close", sessionID)
}

func nodeHasCachedPayload(node *Node, channel, payload string) bool {
	ids := node.cache.RecentIDs(channel, 16)
	for _, id := range ids {
		env, ok := node.cache.Get(id)
		if ok && env.Channel == channel && string(env.Payload) == payload {
			return true
		}
	}
	return false
}
