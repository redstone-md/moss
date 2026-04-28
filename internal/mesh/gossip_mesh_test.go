package mesh

import (
	"fmt"
	"testing"
	"time"

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
	node.peers["peer-c"] = &peerConn{id: "peer-c", addr: "198.51.100.10:41032"}
	node.peers["peer-d"] = &peerConn{id: "peer-d", addr: "203.0.113.20:41033"}

	node.recalculateIPColocationPenalties()

	if score := node.scoring.Score("peer-a"); score != -1 {
		t.Fatalf("expected peer-a colocation penalty -1, got %f", score)
	}
	if score := node.scoring.Score("peer-b"); score != -1 {
		t.Fatalf("expected peer-b colocation penalty -1, got %f", score)
	}
	if score := node.scoring.Score("peer-c"); score != -1 {
		t.Fatalf("expected peer-c colocation penalty -1, got %f", score)
	}
	if score := node.scoring.Score("peer-d"); score != 0 {
		t.Fatalf("expected peer-d to have no colocation penalty, got %f", score)
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

func TestMeshDeliveryDeficitPenalizesSilentMeshPeers(t *testing.T) {
	node, err := NewNode("mesh-delivery-deficit", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	node.pubsub.SetMeshPeer("alpha", "peer-a", true)
	node.pubsub.SetMeshPeer("alpha", "peer-b", true)

	node.observeMeshDelivery("alpha", "msg-1", "peer-a")
	node.evaluateMeshDeliveryDeficits(time.Now().Add(2 * node.config.Heartbeat()))

	if score := node.scoring.Score("peer-a"); score != 0 {
		t.Fatalf("expected delivering peer to avoid deficit penalty, got %f", score)
	}
	if score := node.scoring.Score("peer-b"); score != -0.5 {
		t.Fatalf("expected silent mesh peer to receive deficit penalty, got %f", score)
	}
}

func TestMeshDeliveryDeficitSkipsPeersThatForward(t *testing.T) {
	node, err := NewNode("mesh-delivery-forward", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	node.pubsub.SetMeshPeer("alpha", "peer-a", true)
	node.pubsub.SetMeshPeer("alpha", "peer-b", true)

	node.observeMeshDelivery("alpha", "msg-2", "peer-a")
	node.observeMeshDelivery("alpha", "msg-2", "peer-b")
	node.evaluateMeshDeliveryDeficits(time.Now().Add(2 * node.config.Heartbeat()))

	if score := node.scoring.Score("peer-a"); score != 0 {
		t.Fatalf("expected peer-a to avoid deficit penalty, got %f", score)
	}
	if score := node.scoring.Score("peer-b"); score != 0 {
		t.Fatalf("expected peer-b to avoid deficit penalty, got %f", score)
	}
}

func TestInboundPublishAboveMaxSizeIsRejected(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Security.MaxMessageSizeBytes = 8
	node, err := NewNode("mesh-publish-size-limit", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	node.pubsub.Subscribe("alpha")
	peerID := "peer-oversize"
	payload := []byte("payload-too-large")

	node.handleEnvelope(&peerConn{id: peerID}, gossip.Envelope{
		Type:      gossip.TypePublish,
		Channel:   "alpha",
		MessageID: "msg-oversized",
		Payload:   payload,
	})

	if _, ok := node.cache.Get("msg-oversized"); ok {
		t.Fatal("expected oversized publish to be rejected before caching")
	}
	if score := node.scoring.Score(peerID); score >= 0 {
		t.Fatalf("expected invalid publish to penalize peer, got %f", score)
	}
}

func TestRememberSuppressionCapsPerPeerEntries(t *testing.T) {
	node, err := NewNode("mesh-suppression-cap", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	peerID := "peer-suppress"
	ids := make([]string, maxSuppressionEntriesPerPeer+32)
	for i := range ids {
		ids[i] = fmt.Sprintf("msg-%d", i)
	}

	node.rememberSuppression(peerID, ids, "")

	node.mu.RLock()
	defer node.mu.RUnlock()
	entry := node.suppress[peerID]
	if len(entry) > maxSuppressionEntriesPerPeer {
		t.Fatalf("expected suppression entries to be capped at %d, got %d", maxSuppressionEntriesPerPeer, len(entry))
	}
}
