package mesh

import (
	"testing"
	"time"
)

// The masq-ON variant of the static UDP fallback: same shape as
// TestStaticPeerDialFallsBackToUDP but with masq left at DefaultConfig (ON),
// the mosh app shape. Bisects whether the masq listener path breaks the
// static UDP dial.
func TestStaticPeerUDPWithMasqOn(t *testing.T) {
	nodeA := confirmUDPTestNode(t, "static-masq-probe")
	nodeA.mu.RLock()
	networkID := nodeA.networkID
	nodeA.mu.RUnlock()
	echoAddr := echoUDPListener(t, networkID)

	cfg := DefaultConfig() // masq stays ON
	cfg.NetworkID = networkID
	cfg.Trackers = nil
	cfg.DHTEnabled = false
	cfg.LANDiscoveryEnabled = false
	cfg.StaticPeers = []string{echoAddr}
	node, err := NewNode("mesh-udp-confirm", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	if code := node.Start(); code != MOSS_OK {
		t.Fatalf("Start failed: %d", code)
	}
	t.Cleanup(func() { _ = node.Stop() })

	deadline := time.Now().Add(12 * time.Second)
	for time.Now().Before(deadline) {
		if peerCountSnapshot(node) >= 1 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("masq-ON static node never registered the UDP-ear peer: peers=%d", peerCountSnapshot(node))
}
