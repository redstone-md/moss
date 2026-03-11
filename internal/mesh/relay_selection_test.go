package mesh

import (
	"testing"

	"moss/internal/nat"
)

func TestSelectRelayPeerPrefersRelayCapablePeer(t *testing.T) {
	node, err := NewNode("mesh-relay-rank", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}

	node.peers["peer-private"] = &peerConn{id: "peer-private"}
	node.peers["peer-public"] = &peerConn{id: "peer-public"}
	node.knownPeers["peer-private"] = knownPeer{
		id:              "peer-private",
		addr:            "10.0.0.10:4000",
		natType:         nat.TypeRestrictedCone,
		publicReachable: false,
		relayCapable:    false,
	}
	node.knownPeers["peer-public"] = knownPeer{
		id:              "peer-public",
		addr:            "198.51.100.20:4000",
		natType:         nat.TypePublic,
		publicReachable: true,
		relayCapable:    true,
	}
	node.scoring.SetApplicationScore("peer-private", 10)
	node.scoring.SetApplicationScore("peer-public", 1)

	selected, err := node.selectRelayPeer("target-peer")
	if err != nil {
		t.Fatalf("selectRelayPeer failed: %v", err)
	}
	if selected != "peer-public" {
		t.Fatalf("expected relay-capable peer to be selected, got %s", selected)
	}
}

func TestSelectRelayPeerFallsBackToScoreWhenCapabilityMatches(t *testing.T) {
	node, err := NewNode("mesh-relay-score", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}

	node.peers["peer-a"] = &peerConn{id: "peer-a"}
	node.peers["peer-b"] = &peerConn{id: "peer-b"}
	node.knownPeers["peer-a"] = knownPeer{
		id:              "peer-a",
		addr:            "198.51.100.30:4000",
		natType:         nat.TypePublic,
		publicReachable: true,
		relayCapable:    true,
	}
	node.knownPeers["peer-b"] = knownPeer{
		id:              "peer-b",
		addr:            "203.0.113.30:4000",
		natType:         nat.TypePublic,
		publicReachable: true,
		relayCapable:    true,
	}
	node.scoring.SetApplicationScore("peer-a", 1)
	node.scoring.SetApplicationScore("peer-b", 5)

	selected, err := node.selectRelayPeer("target-peer")
	if err != nil {
		t.Fatalf("selectRelayPeer failed: %v", err)
	}
	if selected != "peer-b" {
		t.Fatalf("expected higher-scored peer to be selected, got %s", selected)
	}
}
