package mesh

import (
	"fmt"
	"net"
	"strconv"
	"testing"
	"time"
)

func TestTwelveNodeStarSustainsRotatingPublishers(t *testing.T) {
	cfgRoot := DefaultConfig()
	cfgRoot.Trackers = nil
	cfgRoot.GossipSub.HeartbeatMS = 50
	cfgRoot.MaxPeers = 16
	root, err := NewNode("mesh-steady-star", nil, cfgRoot)
	if err != nil {
		t.Fatalf("NewNode root failed: %v", err)
	}
	if code := root.Start(); code != MOSS_OK {
		t.Fatalf("root.Start failed: %d", code)
	}
	defer root.Stop()

	newLeaf := func() *Node {
		cfg := DefaultConfig()
		cfg.Trackers = nil
		cfg.GossipSub.HeartbeatMS = 50
		cfg.MaxPeers = 1
		cfg.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(root.ListenPort()))}
		node, nodeErr := NewNode("mesh-steady-star", nil, cfg)
		if nodeErr != nil {
			t.Fatalf("NewNode leaf failed: %v", nodeErr)
		}
		if code := node.Start(); code != MOSS_OK {
			t.Fatalf("leaf Start failed: %d", code)
		}
		return node
	}

	leaves := make([]*Node, 0, 11)
	for i := 0; i < 11; i++ {
		node := newLeaf()
		defer node.Stop()
		leaves = append(leaves, node)
	}

	waitForPeerCount(t, root, 11)
	for _, node := range leaves {
		waitForPeerCount(t, node, 1)
	}

	nodes := append([]*Node{root}, leaves...)
	for idx, node := range nodes {
		if code := node.Subscribe("steady"); code != MOSS_OK {
			t.Fatalf("node %d Subscribe failed: %d", idx, code)
		}
	}
	time.Sleep(200 * time.Millisecond)

	publishers := append([]*Node{root}, leaves...)
	payloads := make([]string, 0, 10)
	start := time.Now()
	for i := 0; i < 10; i++ {
		payload := fmt.Sprintf("steady-%02d", i)
		payloads = append(payloads, payload)
		publisher := publishers[i%len(publishers)]
		if code := publisher.Publish("steady", []byte(payload)); code != MOSS_OK {
			t.Fatalf("publisher %d Publish %s failed: %d", i, payload, code)
		}
		if root.currentPeerCount() < 11 {
			t.Fatalf("root peer count dropped during steady-state run; info=%s", root.MeshInfoJSON())
		}
		for idx, node := range leaves {
			if node.currentPeerCount() < 1 {
				t.Fatalf("leaf %d lost root connectivity during steady-state run; info=%s", idx, node.MeshInfoJSON())
			}
		}
		time.Sleep(125 * time.Millisecond)
	}
	if elapsed := time.Since(start); elapsed < time.Second {
		t.Fatalf("steady-state scenario finished too quickly, got %s", elapsed)
	}

	recentPayloads := tailPayloads(payloads, 10)
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		if everyNodeHasPayloads(leaves, "steady", recentPayloads) {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("steady-state star did not converge; root=%s", root.MeshInfoJSON())
}
