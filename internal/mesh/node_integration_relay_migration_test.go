package mesh

import (
	"context"
	"encoding/hex"
	"net"
	"strconv"
	"testing"
	"time"
)

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
