package mesh

import (
	"testing"
	"time"
)

func TestBootstrapSeedTargetsRespectsCooldownAndConnections(t *testing.T) {
	cfg := DefaultConfig()
	cfg.GossipSub.DOut = 3
	node, err := NewNode("bootstrap-seeds-test", nil, cfg)
	if err != nil {
		t.Fatalf("new node: %v", err)
	}
	node.listenPort = 41030
	now := time.Now()
	node.trackerSeeds["198.51.100.10:41030"] = now
	node.trackerSeeds["198.51.100.11:41030"] = now
	node.trackerSeeds["198.51.100.12:41030"] = now.Add(-11 * time.Minute)
	node.bootstrapDials["198.51.100.10:41030"] = now
	node.peers["peer-b"] = &peerConn{id: "peer-b", addr: "198.51.100.11:41030"}

	targets := node.bootstrapSeedTargets()
	if len(targets) != 0 {
		t.Fatalf("expected no eligible targets, got %v", targets)
	}
	if _, ok := node.trackerSeeds["198.51.100.12:41030"]; ok {
		t.Fatalf("expected stale tracker seed to be pruned")
	}

	node.bootstrapDials["198.51.100.10:41030"] = now.Add(-cfg.HandshakeTimeout())
	targets = node.bootstrapSeedTargets()
	if len(targets) != 1 || targets[0] != "198.51.100.10:41030" {
		t.Fatalf("unexpected targets: %v", targets)
	}
}
