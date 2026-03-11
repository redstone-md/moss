package mesh

import (
	"context"
	"encoding/hex"
	"net"
	"strconv"
	"testing"
	"time"
)

func TestConnectBootstrapPeerFallsBackToUDPWithoutPeerHint(t *testing.T) {
	cfgA := DefaultConfig()
	cfgA.Trackers = nil
	cfgA.LANDiscoveryEnabled = false
	nodeA, err := NewNode("mesh-bootstrap-udp", nil, cfgA)
	if err != nil {
		t.Fatalf("NewNode nodeA failed: %v", err)
	}
	if code := nodeA.Start(); code != MOSS_OK {
		t.Fatalf("nodeA.Start failed: %d", code)
	}
	defer nodeA.Stop()

	cfgB := DefaultConfig()
	cfgB.Trackers = nil
	cfgB.LANDiscoveryEnabled = false
	nodeB, err := NewNode("mesh-bootstrap-udp", nil, cfgB)
	if err != nil {
		t.Fatalf("NewNode nodeB failed: %v", err)
	}
	if code := nodeB.Start(); code != MOSS_OK {
		t.Fatalf("nodeB.Start failed: %d", code)
	}
	defer nodeB.Stop()

	if nodeB.listener == nil {
		t.Fatal("expected TCP listener")
	}
	_ = nodeB.listener.Close()
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := nodeA.connectBootstrapPeer(ctx, net.JoinHostPort("127.0.0.1", strconv.Itoa(nodeB.ListenPort()))); err != nil {
		t.Fatalf("connectBootstrapPeer failed: %v", err)
	}

	targetPub := nodeB.PublicKey()
	sourcePub := nodeA.PublicKey()
	waitForDirectPeerWithin(t, nodeA, hex.EncodeToString(targetPub[:]), 10*time.Second)
	waitForDirectPeerWithin(t, nodeB, hex.EncodeToString(sourcePub[:]), 10*time.Second)
}
