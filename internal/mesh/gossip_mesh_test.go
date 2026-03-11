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

func TestSelectLazyPeersSkipsPeersBelowGossipThreshold(t *testing.T) {
	node, err := NewNode("mesh-lazy-threshold", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	node.pubsub.Subscribe("alpha")
	for _, peerID := range []string{"peer-a", "peer-b", "peer-c"} {
		node.pubsub.SetPeerSubscription(peerID, "alpha", true)
	}
	node.scoring.SetApplicationScore("peer-a", -11)
	node.scoring.SetApplicationScore("peer-b", -10)
	node.scoring.SetApplicationScore("peer-c", 1)

	selected := node.selectLazyPeers("alpha", "", 3)
	if len(selected) != 2 {
		t.Fatalf("expected 2 eligible lazy peers, got %d", len(selected))
	}
	for _, peerID := range selected {
		if peerID == "peer-a" {
			t.Fatalf("selected peer below gossip threshold: %s", peerID)
		}
	}
}

func TestRecalculateIPColocationPenalties(t *testing.T) {
	node, err := NewNode("mesh-ip-penalty", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	node.peers["peer-a"] = &peerConn{id: "peer-a", addr: "198.51.100.10:41030"}
	node.peers["peer-b"] = &peerConn{id: "peer-b", addr: "198.51.100.10:41031"}
	node.peers["peer-c"] = &peerConn{id: "peer-c", addr: "203.0.113.20:41032"}

	node.recalculateIPColocationPenalties()

	if score := node.scoring.Score("peer-a"); score != -5 {
		t.Fatalf("expected peer-a colocation penalty -5, got %f", score)
	}
	if score := node.scoring.Score("peer-b"); score != -5 {
		t.Fatalf("expected peer-b colocation penalty -5, got %f", score)
	}
	if score := node.scoring.Score("peer-c"); score != 0 {
		t.Fatalf("expected peer-c to have no colocation penalty, got %f", score)
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

func TestPublishBelowThresholdIsDropped(t *testing.T) {
	node, err := NewNode("mesh-publish-threshold", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	node.pubsub.Subscribe("alpha")

	peerID := "peer-low-score"
	node.scoring.SetApplicationScore(peerID, gossip.PublishThreshold-1)
	node.handleEnvelope(&peerConn{id: peerID}, gossip.Envelope{
		Type:      gossip.TypePublish,
		Channel:   "alpha",
		MessageID: "msg-1",
		Payload:   []byte("payload"),
	})

	if _, ok := node.cache.Get("msg-1"); ok {
		t.Fatal("expected low-scored publish to be dropped before cache store")
	}
}
