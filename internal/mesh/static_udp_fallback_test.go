package mesh

// Bug №6 harness: a static peer dial is operator intent, but the transport
// choice was gated on address rank — anything loopback or private got the
// TCP-only dial, both at Start and on every seed retry. With the peer's TCP
// unreachable and its UDP ear alive, the pair never formed: the idlepair
// repro ran a full 60 seconds against a live UDP mesh endpoint and ended
// with zero peers and nothing but "connection refused" dial lines.

import (
	"testing"
	"time"
)

// waitForStaticPeer polls the peer table until want peers are present or the
// deadline passes.
func waitForStaticPeer(t *testing.T, node *Node, want int, within time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if peerCountSnapshot(node) >= want {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return peerCountSnapshot(node) >= want
}

// The static dial must fall back to UDP when TCP is refused: a loopback peer
// (rank 0) with a live UDP ear forms the pair.
func TestStaticPeerDialFallsBackToUDP(t *testing.T) {
	nodeA := confirmUDPTestNode(t, "static-udp-fallback")
	nodeA.mu.RLock()
	networkID := nodeA.networkID
	nodeA.mu.RUnlock()
	echoAddr := echoUDPListener(t, networkID)

	nodeA.Stop()
	cfg := isolatedTestConfig("static-udp-fallback")
	cfg.MasqConfig = MasqConfig{}
	cfg.Trackers = nil
	cfg.StaticPeers = []string{echoAddr}
	nodeB, err := NewNode("mesh-udp-confirm", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode static failed: %v", err)
	}
	if code := nodeB.Start(); code != MOSS_OK {
		t.Fatalf("static node Start failed: %d", code)
	}
	t.Cleanup(func() { _ = nodeB.Stop() })

	if !waitForStaticPeer(t, nodeB, 1, 12*time.Second) {
		t.Fatalf("static peer over UDP never registered: the rank gate sent the dial TCP-only against a refused port; peers=%d", peerCountSnapshot(nodeB))
	}
	// The session must hold: a fallback that registers a session the ping
	// machinery immediately drops is a churn loop, not a pair.
	time.Sleep(3 * time.Second)
	if got := peerCountSnapshot(nodeB); got != 1 {
		t.Fatalf("static UDP peer did not hold: peers=%d", got)
	}
}

// A static peer that is dead on both transports must stay at zero — the
// fallback is a path, not a guarantee.
func TestStaticPeerDeadAddrStaysZero(t *testing.T) {
	cfg := isolatedTestConfig("static-dead")
	cfg.MasqConfig = MasqConfig{}
	cfg.Trackers = nil
	// Port 1 on loopback: nothing listens there on TCP or UDP.
	cfg.StaticPeers = []string{"127.0.0.1:1"}
	node, err := NewNode("mesh-udp-confirm", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode dead static failed: %v", err)
	}
	if code := node.Start(); code != MOSS_OK {
		t.Fatalf("dead static Start failed: %d", code)
	}
	t.Cleanup(func() { _ = node.Stop() })

	if got := peerCountSnapshot(node); got != 0 {
		t.Fatalf("dead static peer registered %d peer(s)", got)
	}
}
