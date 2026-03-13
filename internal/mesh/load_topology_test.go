package mesh

import (
	"encoding/hex"
	"fmt"
	"net"
	"strconv"
	"testing"
	"time"
)

func TestTwentyFiveNodePublishBurstPropagatesToAllSubscribers(t *testing.T) {
	cfgRoot := DefaultConfig()
	cfgRoot.Trackers = nil
	cfgRoot.GossipSub.HeartbeatMS = 50
	cfgRoot.MaxPeers = 32
	root, err := NewNode("mesh-burst-25", nil, cfgRoot)
	if err != nil {
		t.Fatalf("NewNode root failed: %v", err)
	}
	if code := root.Start(); code != MOSS_OK {
		t.Fatalf("root.Start failed: %d", code)
	}
	defer root.Stop()

	nodes := []*Node{root}
	for i := 0; i < 24; i++ {
		cfg := DefaultConfig()
		cfg.Trackers = nil
		cfg.GossipSub.HeartbeatMS = 50
		cfg.MaxPeers = 1
		cfg.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(root.ListenPort()))}
		node, err := NewNode("mesh-burst-25", nil, cfg)
		if err != nil {
			t.Fatalf("NewNode peer %d failed: %v", i, err)
		}
		if code := node.Start(); code != MOSS_OK {
			t.Fatalf("peer %d Start failed: %d", i, code)
		}
		defer node.Stop()
		nodes = append(nodes, node)
	}

	waitForPeerCount(t, root, 24)
	for _, node := range nodes[1:] {
		waitForPeerCount(t, node, 1)
	}

	for idx, node := range nodes {
		if code := node.Subscribe("alpha"); code != MOSS_OK {
			t.Fatalf("node %d Subscribe failed: %d", idx, code)
		}
	}
	time.Sleep(150 * time.Millisecond)

	payloads := make([]string, 0, 8)
	for i := 0; i < 8; i++ {
		payload := fmt.Sprintf("burst-%02d", i)
		payloads = append(payloads, payload)
		if code := root.Publish("alpha", []byte(payload)); code != MOSS_OK {
			t.Fatalf("root Publish %s failed: %d", payload, code)
		}
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if everyNodeHasPayloads(nodes[1:], "alpha", payloads) {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("25-node burst did not converge; root=%s", root.MeshInfoJSON())
}

func TestRelayBurstDeliveryRemainsStable(t *testing.T) {
	cfgRelay := DefaultConfig()
	cfgRelay.Trackers = nil
	cfgRelay.GossipSub.HeartbeatMS = 50
	relayNode, err := NewNode("mesh-relay-burst", nil, cfgRelay)
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
	nodeA, err := NewNode("mesh-relay-burst", nil, cfgA)
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
	nodeB, err := NewNode("mesh-relay-burst", nil, cfgB)
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
	sessionID, err := nodeA.OpenRelaySession(hex.EncodeToString(relayPub[:]), hex.EncodeToString(targetPub[:]), 3*time.Second)
	if err != nil {
		t.Fatalf("OpenRelaySession failed: %v", err)
	}
	waitForRelaySession(t, nodeA, sessionID)
	waitForRelaySession(t, nodeB, sessionID)
	waitForRelayRoute(t, relayNode, sessionID)

	want := make(map[string]struct{}, 16)
	received := make(chan string, 32)
	nodeB.SetRelayCallback(func(senderID [32]byte, data []byte) {
		_ = senderID
		received <- string(data)
	})

	for i := 0; i < 16; i++ {
		payload := fmt.Sprintf("relay-burst-%02d", i)
		want[payload] = struct{}{}
		if err := nodeA.RelaySend(sessionID, []byte(payload)); err != nil {
			t.Fatalf("RelaySend %s failed: %v", payload, err)
		}
	}

	deadline := time.Now().Add(5 * time.Second)
	for len(want) > 0 && time.Now().Before(deadline) {
		select {
		case payload := <-received:
			delete(want, payload)
		case <-time.After(25 * time.Millisecond):
		}
	}
	if len(want) != 0 {
		t.Fatalf("relay burst missed %d payloads", len(want))
	}

	nodeA.mu.RLock()
	session := nodeA.relayLocals[sessionID]
	nodeA.mu.RUnlock()
	if !session.established {
		t.Fatalf("expected relay session %s to remain established", sessionID)
	}
}

func TestMixedTopologyPubSubAndRelayRemainStable(t *testing.T) {
	cfgRelay := DefaultConfig()
	cfgRelay.Trackers = nil
	cfgRelay.GossipSub.HeartbeatMS = 50
	cfgRelay.MaxPeers = 8
	relayNode, err := NewNode("mesh-mixed-topology", nil, cfgRelay)
	if err != nil {
		t.Fatalf("NewNode relay failed: %v", err)
	}
	if code := relayNode.Start(); code != MOSS_OK {
		t.Fatalf("relayNode.Start failed: %d", code)
	}
	defer relayNode.Stop()

	cfgRoot := DefaultConfig()
	cfgRoot.Trackers = nil
	cfgRoot.GossipSub.HeartbeatMS = 50
	cfgRoot.MaxPeers = 4
	cfgRoot.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(relayNode.ListenPort()))}
	root, err := NewNode("mesh-mixed-topology", nil, cfgRoot)
	if err != nil {
		t.Fatalf("NewNode root failed: %v", err)
	}
	if code := root.Start(); code != MOSS_OK {
		t.Fatalf("root.Start failed: %d", code)
	}
	defer root.Stop()

	newStaticNode := func(port int) *Node {
		cfg := DefaultConfig()
		cfg.Trackers = nil
		cfg.GossipSub.HeartbeatMS = 50
		cfg.MaxPeers = 1
		cfg.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(port))}
		node, nodeErr := NewNode("mesh-mixed-topology", nil, cfg)
		if nodeErr != nil {
			t.Fatalf("NewNode static peer failed: %v", nodeErr)
		}
		if code := node.Start(); code != MOSS_OK {
			t.Fatalf("static peer Start failed: %d", code)
		}
		return node
	}

	directA := newStaticNode(root.ListenPort())
	defer directA.Stop()
	directB := newStaticNode(root.ListenPort())
	defer directB.Stop()
	natA := newStaticNode(relayNode.ListenPort())
	defer natA.Stop()
	natB := newStaticNode(relayNode.ListenPort())
	defer natB.Stop()

	waitForPeerCount(t, relayNode, 3)
	waitForPeerCount(t, root, 3)
	waitForPeerCount(t, directA, 1)
	waitForPeerCount(t, directB, 1)
	waitForPeerCount(t, natA, 1)
	waitForPeerCount(t, natB, 1)

	nodes := []*Node{relayNode, root, directA, directB, natA, natB}
	for idx, node := range nodes {
		if code := node.Subscribe("mix"); code != MOSS_OK {
			t.Fatalf("node %d Subscribe failed: %d", idx, code)
		}
	}
	time.Sleep(200 * time.Millisecond)

	relayPub := relayNode.PublicKey()
	targetPub := natB.PublicKey()
	sessionID, err := natA.OpenRelaySession(hex.EncodeToString(relayPub[:]), hex.EncodeToString(targetPub[:]), 3*time.Second)
	if err != nil {
		t.Fatalf("OpenRelaySession failed: %v", err)
	}
	waitForRelaySession(t, natA, sessionID)
	waitForRelaySession(t, natB, sessionID)
	waitForRelayRoute(t, relayNode, sessionID)

	relayWant := make(map[string]struct{}, 10)
	relayReceived := make(chan string, 16)
	natB.SetRelayCallback(func(senderID [32]byte, data []byte) {
		_ = senderID
		relayReceived <- string(data)
	})

	pubsubPayloads := make([]string, 0, 10)
	publishers := []*Node{root, directA, natA, relayNode, directB}
	for i := 0; i < 10; i++ {
		payload := fmt.Sprintf("mix-pubsub-%02d", i)
		pubsubPayloads = append(pubsubPayloads, payload)
		publisher := publishers[i%len(publishers)]
		if code := publisher.Publish("mix", []byte(payload)); code != MOSS_OK {
			t.Fatalf("publisher %d Publish %s failed: %d", i, payload, code)
		}

		relayPayload := fmt.Sprintf("mix-relay-%02d", i)
		relayWant[relayPayload] = struct{}{}
		if err := natA.RelaySend(sessionID, []byte(relayPayload)); err != nil {
			t.Fatalf("RelaySend %s failed: %v", relayPayload, err)
		}
		time.Sleep(20 * time.Millisecond)
	}

	deadline := time.Now().Add(6 * time.Second)
	for len(relayWant) > 0 && time.Now().Before(deadline) {
		select {
		case payload := <-relayReceived:
			delete(relayWant, payload)
		case <-time.After(25 * time.Millisecond):
		}
	}
	if len(relayWant) != 0 {
		t.Fatalf("mixed topology relay traffic missed %d payloads", len(relayWant))
	}

	for time.Now().Before(deadline) {
		if everyNodeHasPayloads(nodes, "mix", pubsubPayloads) {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !everyNodeHasPayloads(nodes, "mix", pubsubPayloads) {
		t.Fatalf("mixed topology pubsub did not converge; relay=%s root=%s", relayNode.MeshInfoJSON(), root.MeshInfoJSON())
	}

	waitForPeerCountAtLeast(t, relayNode, 3, 250*time.Millisecond)
	waitForPeerCountAtLeast(t, root, 3, 250*time.Millisecond)

	natA.mu.RLock()
	session := natA.relayLocals[sessionID]
	natA.mu.RUnlock()
	if !session.established {
		t.Fatalf("expected relay session %s to remain established during mixed topology load", sessionID)
	}
}

func TestMixedTopologySteadyStateSoakRetainsConnectivity(t *testing.T) {
	cfgRelay := DefaultConfig()
	cfgRelay.Trackers = nil
	cfgRelay.GossipSub.HeartbeatMS = 50
	cfgRelay.MaxPeers = 8
	relayNode, err := NewNode("mesh-mixed-soak", nil, cfgRelay)
	if err != nil {
		t.Fatalf("NewNode relay failed: %v", err)
	}
	if code := relayNode.Start(); code != MOSS_OK {
		t.Fatalf("relayNode.Start failed: %d", code)
	}
	defer relayNode.Stop()

	cfgRoot := DefaultConfig()
	cfgRoot.Trackers = nil
	cfgRoot.GossipSub.HeartbeatMS = 50
	cfgRoot.MaxPeers = 4
	cfgRoot.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(relayNode.ListenPort()))}
	root, err := NewNode("mesh-mixed-soak", nil, cfgRoot)
	if err != nil {
		t.Fatalf("NewNode root failed: %v", err)
	}
	if code := root.Start(); code != MOSS_OK {
		t.Fatalf("root.Start failed: %d", code)
	}
	defer root.Stop()

	newStaticNode := func(port int) *Node {
		cfg := DefaultConfig()
		cfg.Trackers = nil
		cfg.GossipSub.HeartbeatMS = 50
		cfg.MaxPeers = 1
		cfg.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(port))}
		node, nodeErr := NewNode("mesh-mixed-soak", nil, cfg)
		if nodeErr != nil {
			t.Fatalf("NewNode static peer failed: %v", nodeErr)
		}
		if code := node.Start(); code != MOSS_OK {
			t.Fatalf("static peer Start failed: %d", code)
		}
		return node
	}

	directA := newStaticNode(root.ListenPort())
	defer directA.Stop()
	directB := newStaticNode(root.ListenPort())
	defer directB.Stop()
	natA := newStaticNode(relayNode.ListenPort())
	defer natA.Stop()
	natB := newStaticNode(relayNode.ListenPort())
	defer natB.Stop()

	waitForPeerCount(t, relayNode, 3)
	waitForPeerCount(t, root, 3)
	waitForPeerCount(t, directA, 1)
	waitForPeerCount(t, directB, 1)
	waitForPeerCount(t, natA, 1)
	waitForPeerCount(t, natB, 1)

	nodes := []*Node{relayNode, root, directA, directB, natA, natB}
	for idx, node := range nodes {
		if code := node.Subscribe("soak"); code != MOSS_OK {
			t.Fatalf("node %d Subscribe failed: %d", idx, code)
		}
	}
	time.Sleep(200 * time.Millisecond)

	relayPub := relayNode.PublicKey()
	targetPub := natB.PublicKey()
	sessionID, err := natA.OpenRelaySession(hex.EncodeToString(relayPub[:]), hex.EncodeToString(targetPub[:]), 3*time.Second)
	if err != nil {
		t.Fatalf("OpenRelaySession failed: %v", err)
	}
	waitForRelaySession(t, natA, sessionID)
	waitForRelaySession(t, natB, sessionID)
	waitForRelayRoute(t, relayNode, sessionID)

	relayWant := make(map[string]struct{}, 0)
	relayReceived := make(chan string, 128)
	natB.SetRelayCallback(func(senderID [32]byte, data []byte) {
		_ = senderID
		relayReceived <- string(data)
	})

	pubsubPayloads := make([]string, 0, 24)
	publishers := []*Node{root, directA, natA, relayNode, directB, natB}
	start := time.Now()
	for i := 0; time.Since(start) < 2500*time.Millisecond; i++ {
		payload := fmt.Sprintf("soak-pubsub-%02d", i)
		pubsubPayloads = append(pubsubPayloads, payload)
		publisher := publishers[i%len(publishers)]
		if code := publisher.Publish("soak", []byte(payload)); code != MOSS_OK {
			t.Fatalf("publisher %d Publish %s failed: %d", i, payload, code)
		}

		if i%2 == 0 {
			relayPayload := fmt.Sprintf("soak-relay-%02d", i)
			relayWant[relayPayload] = struct{}{}
			if err := natA.RelaySend(sessionID, []byte(relayPayload)); err != nil {
				t.Fatalf("RelaySend %s failed: %v", relayPayload, err)
			}
		}

		if relayNode.currentPeerCount() < 3 {
			t.Fatalf("relay peer count dropped below 3 during soak; info=%s", relayNode.MeshInfoJSON())
		}
		if root.currentPeerCount() < 3 {
			t.Fatalf("root peer count dropped below 3 during soak; info=%s", root.MeshInfoJSON())
		}
		if natA.currentPeerCount() < 1 || natB.currentPeerCount() < 1 {
			t.Fatalf("nat peers lost connectivity during soak; natA=%s natB=%s", natA.MeshInfoJSON(), natB.MeshInfoJSON())
		}
		time.Sleep(125 * time.Millisecond)
	}

	deadline := time.Now().Add(6 * time.Second)
	for len(relayWant) > 0 && time.Now().Before(deadline) {
		select {
		case payload := <-relayReceived:
			delete(relayWant, payload)
		case <-time.After(25 * time.Millisecond):
		}
	}
	if len(relayWant) != 0 {
		t.Fatalf("mixed topology soak missed %d relay payloads", len(relayWant))
	}

	recentPayloads := tailPayloads(pubsubPayloads, 12)
	for time.Now().Before(deadline) {
		if everyNodeHasPayloads(nodes, "soak", recentPayloads) {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !everyNodeHasPayloads(nodes, "soak", recentPayloads) {
		t.Fatalf("mixed topology soak did not converge; relay=%s root=%s", relayNode.MeshInfoJSON(), root.MeshInfoJSON())
	}

	natA.mu.RLock()
	session := natA.relayLocals[sessionID]
	natA.mu.RUnlock()
	if !session.established {
		t.Fatalf("expected relay session %s to remain established throughout soak", sessionID)
	}
}

func everyNodeHasPayloads(nodes []*Node, channel string, payloads []string) bool {
	for _, node := range nodes {
		for _, payload := range payloads {
			if !nodeHasCachedPayload(node, channel, payload) {
				return false
			}
		}
	}
	return true
}

func tailPayloads(payloads []string, max int) []string {
	if len(payloads) <= max {
		return payloads
	}
	return payloads[len(payloads)-max:]
}
