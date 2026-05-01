package mesh

import (
	"testing"

	"moss/internal/nat"
)

func TestScoringCallbackOverridesRelaySelectionScore(t *testing.T) {
	node, err := NewNode("mesh-scoring-callback", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}

	peerA := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	peerB := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

	node.peers[peerA] = &peerConn{id: peerA}
	node.peers[peerB] = &peerConn{id: peerB}
	node.knownPeers[peerA] = knownPeer{
		id:              peerA,
		addr:            "198.51.100.40:4000",
		natType:         nat.TypePublic,
		natTrusted:      true,
		publicReachable: true,
		relayCapable:    true,
	}
	node.knownPeers[peerB] = knownPeer{
		id:              peerB,
		addr:            "203.0.113.40:4000",
		natType:         nat.TypePublic,
		natTrusted:      true,
		publicReachable: true,
		relayCapable:    true,
	}
	node.scoring.SetApplicationScore(peerA, 1)
	node.scoring.SetApplicationScore(peerB, 5)
	node.SetScoringCallback(func(peerID [32]byte, baseScore float64) float64 {
		switch {
		case peerID == decodePeerID(peerA):
			return baseScore + 10
		case peerID == decodePeerID(peerB):
			return baseScore - 10
		default:
			return baseScore
		}
	})

	selected, err := node.selectRelayPeer("target-peer")
	if err != nil {
		t.Fatalf("selectRelayPeer failed: %v", err)
	}
	if selected != peerA {
		t.Fatalf("expected scoring callback to prefer peer-a, got %s", selected)
	}
}
