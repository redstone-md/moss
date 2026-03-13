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
