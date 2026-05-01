package mesh

import (
	"strconv"
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

func TestInboundPublishOverMaxMessageSizeDropped(t *testing.T) {
	node, err := NewNode("mesh-publish-max-size", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	node.pubsub.Subscribe("alpha")
	node.config.Security.MaxMessageSizeBytes = 8

	peerID := "peer-large-message"
	node.handleEnvelope(&peerConn{id: peerID}, gossip.Envelope{
		Type:      gossip.TypePublish,
		Channel:   "alpha",
		MessageID: "msg-large",
		Payload:   []byte("payload-too-large"),
	})

	if _, ok := node.cache.Get("msg-large"); ok {
		t.Fatal("expected oversized publish to be dropped before cache store")
	}
}

func TestRememberSuppressionCapsEntriesPerPeer(t *testing.T) {
	node, err := NewNode("mesh-suppress-cap", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	ids := make([]string, 0, maxSuppressionEntriesPerPeer+10)
	for i := 0; i < cap(ids); i++ {
		ids = append(ids, "msg-"+strconv.Itoa(i))
	}

	node.rememberSuppression("peer-a", ids, "")

	if got := len(node.suppress["peer-a"]); got != maxSuppressionEntriesPerPeer {
		t.Fatalf("expected suppression map capped at %d entries, got %d", maxSuppressionEntriesPerPeer, got)
	}

	for i := 0; i < 10; i++ {
		node.rememberSuppression("peer-a", []string{"extra-" + strconv.Itoa(i)}, "")
	}

	if got := len(node.suppress["peer-a"]); got != maxSuppressionEntriesPerPeer {
		t.Fatalf("expected repeated suppression calls to stay capped at %d entries, got %d", maxSuppressionEntriesPerPeer, got)
	}
}

func TestMeshDeliveryDeficitDoesNotPenalizeOtherMeshPeers(t *testing.T) {
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
	if score := node.scoring.Score("peer-b"); score != 0 {
		t.Fatalf("expected non-delivering mesh peer to avoid deficit penalty, got %f", score)
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
	obs := node.meshDeliveries["msg-2"]
	if obs == nil {
		t.Fatal("expected mesh delivery observation to be tracked")
	}
	if _, ok := obs.delivered["peer-a"]; !ok {
		t.Fatal("expected peer-a delivery to be tracked")
	}
	if _, ok := obs.delivered["peer-b"]; !ok {
		t.Fatal("expected peer-b delivery to be tracked")
	}
	node.evaluateMeshDeliveryDeficits(time.Now().Add(2 * node.config.Heartbeat()))

	if score := node.scoring.Score("peer-a"); score != 0 {
		t.Fatalf("expected peer-a to avoid deficit penalty, got %f", score)
	}
	if score := node.scoring.Score("peer-b"); score != 0 {
		t.Fatalf("expected peer-b to avoid deficit penalty, got %f", score)
	}
}

func TestSelectMeshCandidatesSkipsHighLatencyPeers(t *testing.T) {
	node, err := NewNode("mesh-candidate-latency", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	node.pubsub.Subscribe("alpha")
	node.pubsub.SetPeerSubscription("peer-fast", "alpha", true)
	node.pubsub.SetPeerSubscription("peer-slow", "alpha", true)
	node.peers["peer-fast"] = &peerConn{id: "peer-fast", addr: "198.51.100.10:41030"}
	node.peers["peer-slow"] = &peerConn{id: "peer-slow", addr: "198.51.100.11:41030", lastRTT: 3 * time.Second}

	selected := node.selectMeshCandidates("alpha", 2)
	if len(selected) != 1 || selected[0] != "peer-fast" {
		t.Fatalf("expected only low-latency candidate, got %#v", selected)
	}
}
