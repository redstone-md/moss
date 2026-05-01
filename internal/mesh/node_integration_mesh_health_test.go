package mesh

import (
	"context"
	"encoding/hex"
	"net"
	"strconv"
	"testing"
	"time"
)

func TestRelaySessionEstablishesWithinFiveSeconds(t *testing.T) {
	cfgRelay := DefaultConfig()
	cfgRelay.Trackers = nil
	cfgRelay.GossipSub.HeartbeatMS = 50
	relayNode, err := NewNode("mesh-relay-latency", nil, cfgRelay)
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
	nodeA, err := NewNode("mesh-relay-latency", nil, cfgA)
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
	nodeB, err := NewNode("mesh-relay-latency", nil, cfgB)
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

	relayPub := relayNode.PublicKey()
	targetPub := nodeB.PublicKey()
	start := time.Now()
	sessionID, err := nodeA.OpenRelaySession(hex.EncodeToString(relayPub[:]), hex.EncodeToString(targetPub[:]), 5*time.Second)
	if err != nil {
		t.Fatalf("OpenRelaySession failed: %v", err)
	}
	waitForRelaySession(t, nodeA, sessionID)
	waitForRelaySession(t, nodeB, sessionID)
	waitForRelayRoute(t, relayNode, sessionID)
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("expected relay session establishment within 5s, got %s", elapsed)
	}
}

func TestPeerLatencyProbeUpdatesRTT(t *testing.T) {
	cfgA := DefaultConfig()
	cfgA.Trackers = nil
	cfgA.GossipSub.HeartbeatMS = 50
	nodeA, err := NewNode("mesh-rtt", nil, cfgA)
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
	cfgB.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(nodeA.ListenPort()))}
	nodeB, err := NewNode("mesh-rtt", nil, cfgB)
	if err != nil {
		t.Fatalf("NewNode nodeB failed: %v", err)
	}
	if code := nodeB.Start(); code != MOSS_OK {
		t.Fatalf("nodeB.Start failed: %d", code)
	}
	defer nodeB.Stop()

	waitForPeerCount(t, nodeA, 1)
	waitForPeerCount(t, nodeB, 1)

	targetPub := nodeB.PublicKey()
	targetID := hex.EncodeToString(targetPub[:])
	nodeA.probePeerLatency(time.Now())
	waitForPeerRTT(t, nodeA, targetID, 2*time.Second)
}

func TestInboundConnectionsRespectMaxPeers(t *testing.T) {
	cfgHub := DefaultConfig()
	cfgHub.Trackers = nil
	cfgHub.LANDiscoveryEnabled = false
	cfgHub.GossipSub.HeartbeatMS = 50
	cfgHub.MaxPeers = 1
	hub, err := NewNode("mesh-max-peers", nil, cfgHub)
	if err != nil {
		t.Fatalf("NewNode hub failed: %v", err)
	}
	if code := hub.Start(); code != MOSS_OK {
		t.Fatalf("hub.Start failed: %d", code)
	}
	defer hub.Stop()

	cfgA := DefaultConfig()
	cfgA.Trackers = nil
	cfgA.LANDiscoveryEnabled = false
	cfgA.GossipSub.HeartbeatMS = 50
	cfgA.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(hub.ListenPort()))}
	nodeA, err := NewNode("mesh-max-peers", nil, cfgA)
	if err != nil {
		t.Fatalf("NewNode nodeA failed: %v", err)
	}
	if code := nodeA.Start(); code != MOSS_OK {
		t.Fatalf("nodeA.Start failed: %d", code)
	}
	defer nodeA.Stop()

	waitForPeerCount(t, hub, 1)
	waitForPeerCount(t, nodeA, 1)

	cfgB := DefaultConfig()
	cfgB.Trackers = nil
	cfgB.LANDiscoveryEnabled = false
	cfgB.GossipSub.HeartbeatMS = 50
	cfgB.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(hub.ListenPort()))}
	nodeB, err := NewNode("mesh-max-peers", nil, cfgB)
	if err != nil {
		t.Fatalf("NewNode nodeB failed: %v", err)
	}
	if code := nodeB.Start(); code != MOSS_OK {
		t.Fatalf("nodeB.Start failed: %d", code)
	}
	defer nodeB.Stop()

	waitForPeerCountAtMost(t, hub, 1, 1500*time.Millisecond)
	waitForPeerCountEventually(t, nodeB, 0)
	if got := hub.currentPeerCount(); got != 1 {
		t.Fatalf("expected hub to keep exactly 1 peer, got %d", got)
	}
}

func TestHighLatencyPeerIsPruned(t *testing.T) {
	cfgA := DefaultConfig()
	cfgA.Trackers = nil
	cfgA.GossipSub.HeartbeatMS = 50
	nodeA, err := NewNode("mesh-prune-latency", nil, cfgA)
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
	cfgB.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(nodeA.ListenPort()))}
	nodeB, err := NewNode("mesh-prune-latency", nil, cfgB)
	if err != nil {
		t.Fatalf("NewNode nodeB failed: %v", err)
	}
	if code := nodeB.Start(); code != MOSS_OK {
		t.Fatalf("nodeB.Start failed: %d", code)
	}
	defer nodeB.Stop()

	waitForPeerCount(t, nodeA, 1)
	waitForPeerCount(t, nodeB, 1)
	if code := nodeA.Subscribe("alpha"); code != MOSS_OK {
		t.Fatalf("nodeA.Subscribe failed: %d", code)
	}
	if code := nodeB.Subscribe("alpha"); code != MOSS_OK {
		t.Fatalf("nodeB.Subscribe failed: %d", code)
	}

	targetPub := nodeB.PublicKey()
	targetID := hex.EncodeToString(targetPub[:])
	waitForPeerMeshState(t, nodeA, "alpha", targetID, true)
	nodeA.mu.Lock()
	if peer := nodeA.peers[targetID]; peer != nil {
		peer.connectedAt = time.Now().Add(-time.Minute)
		peer.lastRTT = 3 * time.Second
		peer.pingPending = ""
		peer.pingSentAt = time.Now()
	}
	nodeA.mu.Unlock()

	nodeA.pruneHighLatencyPeers()
	waitForPeerMeshState(t, nodeA, "alpha", targetID, false)
	waitForPeerCountAtLeast(t, nodeA, 1, 500*time.Millisecond)
}

func TestNegativeScorePeerIsPrunedFromMeshWithoutDisconnect(t *testing.T) {
	cfgA := DefaultConfig()
	cfgA.Trackers = nil
	cfgA.GossipSub.HeartbeatMS = 50
	nodeA, err := NewNode("mesh-negative-score", nil, cfgA)
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
	cfgB.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(nodeA.ListenPort()))}
	nodeB, err := NewNode("mesh-negative-score", nil, cfgB)
	if err != nil {
		t.Fatalf("NewNode nodeB failed: %v", err)
	}
	if code := nodeB.Start(); code != MOSS_OK {
		t.Fatalf("nodeB.Start failed: %d", code)
	}
	defer nodeB.Stop()

	waitForPeerCount(t, nodeA, 1)
	waitForPeerCount(t, nodeB, 1)
	if code := nodeA.Subscribe("alpha"); code != MOSS_OK {
		t.Fatalf("nodeA.Subscribe failed: %d", code)
	}
	if code := nodeB.Subscribe("alpha"); code != MOSS_OK {
		t.Fatalf("nodeB.Subscribe failed: %d", code)
	}

	targetPub := nodeB.PublicKey()
	targetID := hex.EncodeToString(targetPub[:])
	waitForPeerMeshState(t, nodeA, "alpha", targetID, true)

	nodeA.mu.Lock()
	if peer := nodeA.peers[targetID]; peer != nil {
		peer.connectedAt = time.Now().Add(-time.Minute)
	}
	nodeA.mu.Unlock()
	nodeA.scoring.SetApplicationScore(targetID, -5)

	nodeA.pruneLowScoringPeers()

	waitForPeerMeshState(t, nodeA, "alpha", targetID, false)
	waitForPeerCountAtLeast(t, nodeA, 1, 500*time.Millisecond)
}

func TestSimultaneousDirectDialsResolveToSingleConnection(t *testing.T) {
	cfgA := DefaultConfig()
	cfgA.Trackers = nil
	cfgA.GossipSub.HeartbeatMS = 50
	nodeA, err := NewNode("mesh-simultaneous-dial", nil, cfgA)
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
	nodeB, err := NewNode("mesh-simultaneous-dial", nil, cfgB)
	if err != nil {
		t.Fatalf("NewNode nodeB failed: %v", err)
	}
	if code := nodeB.Start(); code != MOSS_OK {
		t.Fatalf("nodeB.Start failed: %d", code)
	}
	defer nodeB.Stop()

	addrA := net.JoinHostPort("127.0.0.1", strconv.Itoa(nodeA.ListenPort()))
	addrB := net.JoinHostPort("127.0.0.1", strconv.Itoa(nodeB.ListenPort()))
	done := make(chan struct{}, 2)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = nodeA.connectPeer(ctx, addrB)
		done <- struct{}{}
	}()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = nodeB.connectPeer(ctx, addrA)
		done <- struct{}{}
	}()
	<-done
	<-done

	targetPub := nodeB.PublicKey()
	sourcePub := nodeA.PublicKey()
	waitForDirectPeer(t, nodeA, hex.EncodeToString(targetPub[:]))
	waitForDirectPeer(t, nodeB, hex.EncodeToString(sourcePub[:]))
	waitForPeerCountAtMost(t, nodeA, 1, 500*time.Millisecond)
	waitForPeerCountAtMost(t, nodeB, 1, 500*time.Millisecond)
}
