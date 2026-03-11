package mesh

import (
	"testing"

	"moss/internal/gossip"
)

func TestSelectLazyPeersCapsToDLazy(t *testing.T) {
	node, err := NewNode("mesh-lazy", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	node.pubsub.Subscribe("alpha")
	for _, peerID := range []string{"peer-a", "peer-b", "peer-c", "peer-d"} {
		node.pubsub.SetPeerSubscription(peerID, "alpha", true)
	}
	node.pubsub.SetMeshPeer("alpha", "peer-a", true)
	node.pubsub.SetMeshPeer("alpha", "peer-b", true)

	selected := node.selectLazyPeers("alpha", "", 2)
	if len(selected) != 2 {
		t.Fatalf("expected 2 lazy peers, got %d", len(selected))
	}
	for _, peerID := range selected {
		if peerID == "peer-a" || peerID == "peer-b" {
			t.Fatalf("selected mesh peer %s as lazy target", peerID)
		}
	}
}

func TestMedianMeshScore(t *testing.T) {
	engine := gossip.NewEngine()
	engine.SetApplicationScore("peer-a", -2)
	engine.SetApplicationScore("peer-b", 1)
	engine.SetApplicationScore("peer-c", 3)
	if score := medianMeshScore(engine, []string{"peer-a", "peer-b", "peer-c"}); score != 1 {
		t.Fatalf("unexpected median score %f", score)
	}
}
