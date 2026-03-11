package mesh

import (
	"encoding/hex"
	"net"
	"strconv"
	"testing"
	"time"

	"moss/internal/nat"
)

func TestShouldPreferRelayBetweenSymmetricAndCGNATPeers(t *testing.T) {
	if !shouldPreferRelayBetween(nat.TypeSymmetric, nat.TypeSymmetric) {
		t.Fatal("expected symmetric peers to prefer relay")
	}
	if !shouldPreferRelayBetween(nat.TypeCGNAT, nat.TypeSymmetric) {
		t.Fatal("expected cgnat+symmetric peers to prefer relay")
	}
	if shouldPreferRelayBetween(nat.TypePortRestricted, nat.TypePortRestricted) {
		t.Fatal("expected port-restricted peers to keep direct hole-punch path")
	}
	if shouldPreferRelayBetween(nat.TypeFullCone, nat.TypeCGNAT) {
		t.Fatal("expected public/cone peer to keep direct path available")
	}
}

func TestSymmetricNATPeersRelayWithinFiveSeconds(t *testing.T) {
	cfgRelay := DefaultConfig()
	cfgRelay.Trackers = nil
	cfgRelay.GossipSub.HeartbeatMS = 50
	relayNode, err := NewNode("mesh-symmetric-relay", nil, cfgRelay)
	if err != nil {
		t.Fatalf("NewNode relay failed: %v", err)
	}
	if code := relayNode.Start(); code != MOSS_OK {
		t.Fatalf("relayNode.Start failed: %d", code)
	}
	defer relayNode.Stop()

	cfgA := DefaultConfig()
	cfgA.Trackers = nil
	cfgA.GossipSub.HeartbeatMS = 50
	cfgA.MaxPeers = 1
	cfgA.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(relayNode.ListenPort()))}
	nodeA, err := NewNode("mesh-symmetric-relay", nil, cfgA)
	if err != nil {
		t.Fatalf("NewNode nodeA failed: %v", err)
	}
	if code := nodeA.Start(); code != MOSS_OK {
		t.Fatalf("nodeA.Start failed: %d", code)
	}
	defer nodeA.Stop()

	cfgB := DefaultConfig()
	cfgB.Trackers = nil
	cfgB.GossipSub.HeartbeatMS = 50
	cfgB.MaxPeers = 1
	cfgB.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(relayNode.ListenPort()))}
	nodeB, err := NewNode("mesh-symmetric-relay", nil, cfgB)
	if err != nil {
		t.Fatalf("NewNode nodeB failed: %v", err)
	}
	if code := nodeB.Start(); code != MOSS_OK {
		t.Fatalf("nodeB.Start failed: %d", code)
	}
	defer nodeB.Stop()

	waitForPeerCount(t, relayNode, 2)
	waitForPeerCount(t, nodeA, 1)
	waitForPeerCount(t, nodeB, 1)

	targetPub := nodeB.PublicKey()
	targetID := hex.EncodeToString(targetPub[:])
	sourcePub := nodeA.PublicKey()
	sourceID := hex.EncodeToString(sourcePub[:])
	waitForKnownPeer(t, nodeA, targetID)
	waitForKnownPeer(t, nodeB, sourceID)

	nodeA.natProfile.Store(nat.Profile{Type: nat.TypeSymmetric})
	nodeB.natProfile.Store(nat.Profile{Type: nat.TypeSymmetric})

	nodeA.mu.Lock()
	infoA := nodeA.knownPeers[targetID]
	infoA.natType = nat.TypeSymmetric
	nodeA.knownPeers[targetID] = infoA
	nodeA.mu.Unlock()

	nodeB.mu.Lock()
	infoB := nodeB.knownPeers[sourceID]
	infoB.natType = nat.TypeSymmetric
	nodeB.knownPeers[sourceID] = infoB
	nodeB.mu.Unlock()

	if !nodeA.shouldPreferRelayForTarget(targetID) {
		t.Fatal("expected symmetric target to prefer relay")
	}

	received := make(chan []byte, 1)
	nodeB.SetRelayCallback(func(senderID [32]byte, data []byte) {
		_ = senderID
		received <- append([]byte(nil), data...)
	})

	start := time.Now()
	if err := nodeA.RelaySendTo(targetID, []byte("symmetric-relay"), 5*time.Second); err != nil {
		t.Fatalf("RelaySendTo failed: %v", err)
	}
	select {
	case payload := <-received:
		if string(payload) != "symmetric-relay" {
			t.Fatalf("unexpected relay payload: %q", string(payload))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for symmetric relay payload")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("expected symmetric relay fallback within 5s, got %s", elapsed)
	}
	if nodeA.directPeerConnected(targetID) {
		t.Fatal("expected symmetric nat peers to stay off direct path during relay fallback")
	}
}
