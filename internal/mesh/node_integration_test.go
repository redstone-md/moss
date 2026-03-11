package mesh

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"net"
	"strconv"
	"testing"
	"time"
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
	cfgA.GossipSub.HeartbeatMS = 50
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
		if err.Error() == "target peer became directly connected; use direct transport" {
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

func TestAttemptHolePunchEstablishesDirectPeer(t *testing.T) {
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

	if !nodeA.attemptHolePunch(targetID, 2*time.Second) {
		t.Fatal("attemptHolePunch did not establish a direct peer")
	}

	sourcePub := nodeA.PublicKey()
	waitForDirectPeer(t, nodeA, targetID)
	waitForDirectPeer(t, nodeB, hex.EncodeToString(sourcePub[:]))
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
	sessionID, err := nodeA.OpenRelaySession(hex.EncodeToString(relayPub[:]), hex.EncodeToString(targetPub[:]), 2*time.Second)
	if err != nil {
		t.Fatalf("OpenRelaySession failed: %v", err)
	}
	waitForRelaySession(t, nodeA, sessionID)
	waitForRelaySession(t, nodeB, sessionID)
	waitForRelayRoute(t, relayNode, sessionID)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	nodeA.connectPeer(ctx, net.JoinHostPort("127.0.0.1", strconv.Itoa(nodeB.ListenPort())))
	cancel()

	sourcePub := nodeA.PublicKey()
	waitForDirectPeer(t, nodeA, hex.EncodeToString(targetPub[:]))
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
	waitForMeshCount(t, nodeA, "alpha", 2)

	meshPeers := nodeA.pubsub.MeshPeers("alpha")
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

	updatedMesh := nodeA.pubsub.MeshPeers("alpha")
	if len(updatedMesh) != 2 {
		t.Fatalf("expected mesh size 2 after opportunistic graft, got %d", len(updatedMesh))
	}
	found := false
	for _, peerID := range updatedMesh {
		if peerID == nonMeshPeer {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected high-scoring non-mesh peer %s to join mesh, got %v", nonMeshPeer, updatedMesh)
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

func waitForPeerCount(t *testing.T, node *Node, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
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

func waitForMeshCount(t *testing.T, node *Node, channel string, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if len(node.pubsub.MeshPeers(channel)) == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("mesh size did not reach %d for channel %s", want, channel)
}

func waitForKnownPeer(t *testing.T, node *Node, wantPeerID string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
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
	deadline := time.Now().Add(3 * time.Second)
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
