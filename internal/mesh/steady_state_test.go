package mesh

import (
	"fmt"
	"net"
	"strconv"
	"testing"
	"time"
)

func TestTwelveNodeStarSustainsRotatingPublishers(t *testing.T) {
	root, leaves := startStarTopology(t, "mesh-steady-star", 11)
	defer root.Stop()
	for _, node := range leaves {
		defer node.Stop()
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

func TestStarTopologyResumesDeliveryAfterLeafRestart(t *testing.T) {
	root, leaves := startStarTopology(t, "mesh-steady-restart", 7)
	defer root.Stop()
	for _, node := range leaves {
		defer node.Stop()
	}

	nodes := append([]*Node{root}, leaves...)
	for idx, node := range nodes {
		if code := node.Subscribe("restart"); code != MOSS_OK {
			t.Fatalf("node %d Subscribe failed: %d", idx, code)
		}
	}
	time.Sleep(200 * time.Millisecond)

	if code := root.Publish("restart", []byte("before-restart")); code != MOSS_OK {
		t.Fatalf("root Publish before-restart failed: %d", code)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if everyNodeHasPayloads(leaves, "restart", []string{"before-restart"}) {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !everyNodeHasPayloads(leaves, "restart", []string{"before-restart"}) {
		t.Fatalf("initial publish did not converge; root=%s", root.MeshInfoJSON())
	}

	restarted := leaves[len(leaves)-1]
	restarted.Stop()
	replacement := startStarLeaf(t, "mesh-steady-restart", root.ListenPort())
	defer replacement.Stop()
	if code := replacement.Subscribe("restart"); code != MOSS_OK {
		t.Fatalf("replacement Subscribe failed: %d", code)
	}
	waitForPeerCount(t, root, 7)
	waitForPeerCount(t, replacement, 1)
	time.Sleep(200 * time.Millisecond)

	payloads := []string{"after-restart-1", "after-restart-2"}
	for _, payload := range payloads {
		if code := root.Publish("restart", []byte(payload)); code != MOSS_OK {
			t.Fatalf("root Publish %s failed: %d", payload, code)
		}
	}

	activeLeaves := append([]*Node(nil), leaves[:len(leaves)-1]...)
	activeLeaves = append(activeLeaves, replacement)
	deadline = time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		if everyNodeHasPayloads(activeLeaves, "restart", payloads) {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("star topology did not resume delivery after leaf restart; root=%s replacement=%s", root.MeshInfoJSON(), replacement.MeshInfoJSON())
}

func startStarTopology(t *testing.T, meshID string, leafCount int) (*Node, []*Node) {
	t.Helper()
	cfgRoot := DefaultConfig()
	cfgRoot.Trackers = nil
	cfgRoot.GossipSub.HeartbeatMS = 50
	cfgRoot.MaxPeers = max(leafCount+1, 16)
	root, err := NewNode(meshID, nil, cfgRoot)
	if err != nil {
		t.Fatalf("NewNode root failed: %v", err)
	}
	if code := root.Start(); code != MOSS_OK {
		t.Fatalf("root.Start failed: %d", code)
	}

	leaves := make([]*Node, 0, leafCount)
	for i := 0; i < leafCount; i++ {
		leaves = append(leaves, startStarLeaf(t, meshID, root.ListenPort()))
	}

	waitForPeerCount(t, root, leafCount)
	for _, node := range leaves {
		waitForPeerCount(t, node, 1)
	}
	return root, leaves
}

func startStarLeaf(t *testing.T, meshID string, rootPort int) *Node {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Trackers = nil
	cfg.GossipSub.HeartbeatMS = 50
	cfg.MaxPeers = 1
	cfg.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(rootPort))}
	node, err := NewNode(meshID, nil, cfg)
	if err != nil {
		t.Fatalf("NewNode leaf failed: %v", err)
	}
	if code := node.Start(); code != MOSS_OK {
		t.Fatalf("leaf Start failed: %d", code)
	}
	return node
}
