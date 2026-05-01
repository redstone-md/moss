package mesh

import (
	"fmt"
	"net"
	"strconv"
	"testing"
	"time"
)

func TestStarTopologyRecoversAfterPeerReplacement(t *testing.T) {
	cfgRoot := DefaultConfig()
	cfgRoot.Trackers = nil
	cfgRoot.GossipSub.HeartbeatMS = 50
	cfgRoot.MaxPeers = 16
	root, err := NewNode("mesh-churn-star", nil, cfgRoot)
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
		node, nodeErr := NewNode("mesh-churn-star", nil, cfg)
		if nodeErr != nil {
			t.Fatalf("NewNode leaf failed: %v", nodeErr)
		}
		if code := node.Start(); code != MOSS_OK {
			t.Fatalf("leaf Start failed: %d", code)
		}
		return node
	}

	leaves := make([]*Node, 0, 8)
	for i := 0; i < 8; i++ {
		node := newLeaf()
		defer node.Stop()
		leaves = append(leaves, node)
	}

	waitForPeerCount(t, root, 8)
	for _, node := range leaves {
		waitForPeerCount(t, node, 1)
	}

	nodes := append([]*Node{root}, leaves...)
	for idx, node := range nodes {
		if code := node.Subscribe("churn"); code != MOSS_OK {
			t.Fatalf("node %d Subscribe failed: %d", idx, code)
		}
	}
	time.Sleep(200 * time.Millisecond)

	if code := root.Publish("churn", []byte("before-churn")); code != MOSS_OK {
		t.Fatalf("root Publish before-churn failed: %d", code)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if everyNodeHasPayloads(leaves, "churn", []string{"before-churn"}) {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !everyNodeHasPayloads(leaves, "churn", []string{"before-churn"}) {
		t.Fatalf("initial publish did not reach all leaves; root=%s", root.MeshInfoJSON())
	}

	stopped := []*Node{leaves[1], leaves[4], leaves[6]}
	for _, node := range stopped {
		node.Stop()
	}
	waitForPeerCountEventuallyAtMost(t, root, 7, 3*time.Second)

	replacements := make([]*Node, 0, len(stopped))
	for i := 0; i < len(stopped); i++ {
		node := newLeaf()
		defer node.Stop()
		replacements = append(replacements, node)
	}
	for _, node := range replacements {
		waitForPeerCount(t, node, 1)
		if code := node.Subscribe("churn"); code != MOSS_OK {
			t.Fatalf("replacement Subscribe failed: %d", code)
		}
	}

	waitForPeerCount(t, root, 8)
	activeLeaves := []*Node{
		leaves[0],
		leaves[2],
		leaves[3],
		leaves[5],
		leaves[7],
		replacements[0],
		replacements[1],
		replacements[2],
	}
	time.Sleep(200 * time.Millisecond)

	payloads := make([]string, 0, 4)
	publishers := []*Node{root, activeLeaves[0], activeLeaves[5], activeLeaves[7]}
	for i, publisher := range publishers {
		payload := fmt.Sprintf("after-churn-%02d", i)
		payloads = append(payloads, payload)
		if code := publisher.Publish("churn", []byte(payload)); code != MOSS_OK {
			t.Fatalf("publisher %d Publish %s failed: %d", i, payload, code)
		}
	}

	deadline = time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		if everyNodeHasPayloads(activeLeaves, "churn", payloads) {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("star topology did not recover after peer replacement; root=%s", root.MeshInfoJSON())
}
