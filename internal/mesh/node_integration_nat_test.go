package mesh

import (
	"context"
	"encoding/hex"
	"net"
	"strconv"
	"testing"
	"time"

	"moss/internal/gossip"
	"moss/internal/nat"
)

func TestSupernodeDemotesWhenRelayCapacityIsSaturated(t *testing.T) {
	cfgA := DefaultConfig()
	cfgA.Trackers = nil
	cfgA.GossipSub.HeartbeatMS = 50
	cfgA.NAT.SuperNodeMinUptimeSec = 0
	cfgA.NAT.RelayMaxSessions = 1
	nodeA, err := NewNode("mesh-supernode-overload", nil, cfgA)
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
	nodeB, err := NewNode("mesh-supernode-overload", nil, cfgB)
	if err != nil {
		t.Fatalf("NewNode nodeB failed: %v", err)
	}
	if code := nodeB.Start(); code != MOSS_OK {
		t.Fatalf("nodeB.Start failed: %d", code)
	}
	defer nodeB.Stop()

	waitForPeerCount(t, nodeA, 1)
	waitForPeerCount(t, nodeB, 1)

	promoted := make(chan struct{}, 4)
	revoked := make(chan struct{}, 4)
	nodeA.SetEventCallback(func(eventType int32, detailJSON string) {
		switch eventType {
		case EventSupernodePromoted:
			promoted <- struct{}{}
		case EventSupernodeRevoked:
			revoked <- struct{}{}
		}
	})

	nodeAPub := nodeA.PublicKey()
	nodeAID := hex.EncodeToString(nodeAPub[:])
	waitForKnownPeer(t, nodeB, nodeAID)
	nodeA.natProfile.Store(nat.Profile{
		Type:            nat.TypePublic,
		PublicReachable: true,
		ExternalAddress: net.JoinHostPort("203.0.113.10", strconv.Itoa(nodeA.ListenPort())),
	})
	nodeA.refreshSupernodeStatus()
	waitForKnownPeerRelayCapable(t, nodeB, nodeAID, true)

	select {
	case <-promoted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for initial supernode promotion")
	}

	if !nodeA.relaySessions.Acquire("overload-session") {
		t.Fatal("expected to acquire relay session capacity")
	}
	nodeA.refreshSupernodeStatus()
	waitForKnownPeerRelayCapable(t, nodeB, nodeAID, false)

	select {
	case <-revoked:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for overload revoke")
	}

	nodeA.relaySessions.Release("overload-session")
	nodeA.refreshSupernodeStatus()
	waitForKnownPeerRelayCapable(t, nodeB, nodeAID, true)

	select {
	case <-promoted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for re-promotion after capacity recovery")
	}
}

// TestSupernodeAnnounceReachesPeerJoiningAfterPromotion covers the relay-mesh
// bootstrap case: a public SuperNode promotes while it has no peers, then a peer
// connects afterwards. Relay capability is only trusted from a signed
// SupernodeAnnounce (never a plain peer-announce), and that announce is otherwise
// broadcast just once at promotion — so a late-joining peer must be (re-)told on
// join, or it never learns the node can relay for it.
func TestSupernodeAnnounceReachesPeerJoiningAfterPromotion(t *testing.T) {
	cfgA := DefaultConfig()
	cfgA.Trackers = nil
	cfgA.GossipSub.HeartbeatMS = 50
	cfgA.NAT.SuperNodeMinUptimeSec = 0
	nodeA, err := NewNode("mesh-supernode-latejoin", nil, cfgA)
	if err != nil {
		t.Fatalf("NewNode nodeA failed: %v", err)
	}
	if code := nodeA.Start(); code != MOSS_OK {
		t.Fatalf("nodeA.Start failed: %d", code)
	}
	defer nodeA.Stop()

	// Promote A to SuperNode BEFORE any peer is connected.
	nodeA.natProfile.Store(nat.Profile{
		Type:            nat.TypePublic,
		PublicReachable: true,
		ExternalAddress: net.JoinHostPort("203.0.113.10", strconv.Itoa(nodeA.ListenPort())),
	})
	nodeA.refreshSupernodeStatus()

	// B connects only now — after promotion. It must still converge to seeing A
	// as relay-capable.
	cfgB := DefaultConfig()
	cfgB.Trackers = nil
	cfgB.GossipSub.HeartbeatMS = 50
	cfgB.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(nodeA.ListenPort()))}
	nodeB, err := NewNode("mesh-supernode-latejoin", nil, cfgB)
	if err != nil {
		t.Fatalf("NewNode nodeB failed: %v", err)
	}
	if code := nodeB.Start(); code != MOSS_OK {
		t.Fatalf("nodeB.Start failed: %d", code)
	}
	defer nodeB.Stop()

	waitForPeerCount(t, nodeB, 1)
	nodeAPub := nodeA.PublicKey()
	nodeAID := hex.EncodeToString(nodeAPub[:])
	waitForKnownPeer(t, nodeB, nodeAID)
	waitForKnownPeerRelayCapable(t, nodeB, nodeAID, true)
}

func TestRefreshExternalAddressPreservesListenPort(t *testing.T) {
	cfgRelay := DefaultConfig()
	cfgRelay.Trackers = nil
	cfgRelay.GossipSub.HeartbeatMS = 50
	relayNode, err := NewNode("mesh-binding", nil, cfgRelay)
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
	cfgA.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(relayNode.ListenPort()))}
	nodeA, err := NewNode("mesh-binding", nil, cfgA)
	if err != nil {
		t.Fatalf("NewNode nodeA failed: %v", err)
	}
	if code := nodeA.Start(); code != MOSS_OK {
		t.Fatalf("nodeA.Start failed: %d", code)
	}
	defer nodeA.Stop()

	waitForPeerCount(t, relayNode, 1)
	waitForPeerCount(t, nodeA, 1)

	if !nodeA.refreshExternalAddress(time.Now().Add(time.Second)) {
		t.Fatal("expected binding refresh to succeed through connected relay peer")
	}

	host, port, err := net.SplitHostPort(nodeA.advertisedListenAddr())
	if err != nil {
		t.Fatalf("advertisedListenAddr is invalid: %v", err)
	}
	if host == "" {
		t.Fatal("expected advertised host to be populated")
	}
	if port != strconv.Itoa(nodeA.ListenPort()) {
		t.Fatalf("expected binding refresh to preserve listen port %d, got %s", nodeA.ListenPort(), port)
	}
}

func TestReachabilityProbeReportsReachableAddress(t *testing.T) {
	cfgA := DefaultConfig()
	cfgA.Trackers = nil
	nodeA, err := NewNode("mesh-reachability", nil, cfgA)
	if err != nil {
		t.Fatalf("NewNode nodeA failed: %v", err)
	}
	if code := nodeA.Start(); code != MOSS_OK {
		t.Fatalf("nodeA.Start failed: %d", code)
	}
	defer nodeA.Stop()

	cfgB := DefaultConfig()
	cfgB.Trackers = nil
	cfgB.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(nodeA.ListenPort()))}
	nodeB, err := NewNode("mesh-reachability", nil, cfgB)
	if err != nil {
		t.Fatalf("NewNode nodeB failed: %v", err)
	}
	if code := nodeB.Start(); code != MOSS_OK {
		t.Fatalf("nodeB.Start failed: %d", code)
	}
	defer nodeB.Stop()

	waitForPeerCount(t, nodeA, 1)
	waitForPeerCount(t, nodeB, 1)

	proberPub := nodeB.PublicKey()
	proberID := hex.EncodeToString(proberPub[:])
	if !nodeA.requestReachabilityProbe(proberID, net.JoinHostPort("127.0.0.1", strconv.Itoa(nodeA.ListenPort())), time.Second) {
		t.Fatal("expected reachability probe to confirm reachable address")
	}
}

func TestReachabilityProbeReportsUnreachableAddress(t *testing.T) {
	cfgA := DefaultConfig()
	cfgA.Trackers = nil
	nodeA, err := NewNode("mesh-reachability-fail", nil, cfgA)
	if err != nil {
		t.Fatalf("NewNode nodeA failed: %v", err)
	}
	if code := nodeA.Start(); code != MOSS_OK {
		t.Fatalf("nodeA.Start failed: %d", code)
	}
	defer nodeA.Stop()

	cfgB := DefaultConfig()
	cfgB.Trackers = nil
	cfgB.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(nodeA.ListenPort()))}
	nodeB, err := NewNode("mesh-reachability-fail", nil, cfgB)
	if err != nil {
		t.Fatalf("NewNode nodeB failed: %v", err)
	}
	if code := nodeB.Start(); code != MOSS_OK {
		t.Fatalf("nodeB.Start failed: %d", code)
	}
	defer nodeB.Stop()

	waitForPeerCount(t, nodeA, 1)
	waitForPeerCount(t, nodeB, 1)

	proberPub := nodeB.PublicKey()
	proberID := hex.EncodeToString(proberPub[:])
	if nodeA.requestReachabilityProbe(proberID, "127.0.0.1:1", 500*time.Millisecond) {
		t.Fatal("expected reachability probe to fail for closed address")
	}
}

func TestDirectUDPConnectRegistersPeer(t *testing.T) {
	cfgA := DefaultConfig()
	cfgA.Trackers = nil
	nodeA, err := NewNode("mesh-udp-direct", nil, cfgA)
	if err != nil {
		t.Fatalf("NewNode nodeA failed: %v", err)
	}
	if code := nodeA.Start(); code != MOSS_OK {
		t.Fatalf("nodeA.Start failed: %d", code)
	}
	defer nodeA.Stop()

	cfgB := DefaultConfig()
	cfgB.Trackers = nil
	nodeB, err := NewNode("mesh-udp-direct", nil, cfgB)
	if err != nil {
		t.Fatalf("NewNode nodeB failed: %v", err)
	}
	if code := nodeB.Start(); code != MOSS_OK {
		t.Fatalf("nodeB.Start failed: %d", code)
	}
	defer nodeB.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	targetPub := nodeB.PublicKey()
	nodeA.connectPeerUDP(ctx, hex.EncodeToString(targetPub[:]), net.JoinHostPort("127.0.0.1", strconv.Itoa(nodeB.ListenPort())))
	cancel()

	sourcePub := nodeA.PublicKey()
	waitForDirectPeerWithin(t, nodeA, hex.EncodeToString(targetPub[:]), 10*time.Second)
	waitForDirectPeerWithin(t, nodeB, hex.EncodeToString(sourcePub[:]), 10*time.Second)
}

func TestTryHolePunchDialEstablishesDirectPeer(t *testing.T) {
	cfgRelay := DefaultConfig()
	cfgRelay.Trackers = nil
	cfgRelay.GossipSub.HeartbeatMS = 50
	relayNode, err := NewNode("mesh-holepunch", nil, cfgRelay)
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
	cfgA.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(relayNode.ListenPort()))}
	nodeA, err := NewNode("mesh-holepunch", nil, cfgA)
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
	cfgB.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(relayNode.ListenPort()))}
	nodeB, err := NewNode("mesh-holepunch", nil, cfgB)
	if err != nil {
		t.Fatalf("NewNode nodeB failed: %v", err)
	}
	if code := nodeB.Start(); code != MOSS_OK {
		t.Fatalf("nodeB.Start failed: %d", code)
	}
	defer nodeB.Stop()

	waitForPeerCount(t, relayNode, 2)
	targetPub := nodeB.PublicKey()
	targetID := hex.EncodeToString(targetPub[:])
	waitForKnownPeer(t, nodeA, targetID)
	waitForKnownPeerPort(t, nodeA, targetID, strconv.Itoa(nodeB.ListenPort()))
	sourcePub := nodeA.PublicKey()
	waitForKnownPeerPort(t, nodeB, hex.EncodeToString(sourcePub[:]), strconv.Itoa(nodeA.ListenPort()))

	nodeA.mu.RLock()
	targetAddr := nodeA.knownPeers[targetID].addr
	nodeA.mu.RUnlock()
	for attempt := 0; attempt < 5 && !nodeA.directPeerConnected(targetID); attempt++ {
		nodeA.tryHolePunchDial(targetID, targetAddr)
		time.Sleep(150 * time.Millisecond)
	}

	waitForDirectPeer(t, nodeA, targetID)
	waitForDirectPeer(t, nodeB, hex.EncodeToString(sourcePub[:]))
}

func TestHandleHolePunchCoordIgnoresUnsolicitedRequest(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	node, err := NewNode("mesh-holepunch-unsolicited", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}

	localID := node.localPeerID()
	peer := &peerConn{id: "relay-peer"}

	node.mu.RLock()
	before, hadBefore := node.knownPeers["attacker-source"]
	node.mu.RUnlock()

	node.handleHolePunchCoord(peer, gossip.Envelope{
		Type:           gossip.TypeHolePunchCoord,
		RequestID:      "attacker-request",
		CoordStage:     "reply",
		RelaySource:    "attacker-source",
		RelayTarget:    localID,
		AdvertisedAddr: "127.0.0.1:6553",
	})

	node.mu.RLock()
	after, hadAfter := node.knownPeers["attacker-source"]
	_, pending := node.holePunchWait["attacker-request"]
	node.mu.RUnlock()

	if hadAfter != hadBefore || after.addr != before.addr {
		t.Fatalf("unexpected known peer update for unsolicited request")
	}
	if pending {
		t.Fatalf("unexpected pending hole punch request for unsolicited request")
	}
}

func TestHandleHolePunchCoordKeepsPendingRequestAfterMismatchedReply(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	node, err := NewNode("mesh-holepunch-mismatch", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}

	localID := node.localPeerID()
	node.mu.Lock()
	node.holePunchWait["req-1"] = holePunchRequest{targetPeerID: "target-peer", relayPeerID: "relay-peer"}
	node.mu.Unlock()

	node.handleHolePunchCoord(&peerConn{id: "attacker-relay"}, gossip.Envelope{
		Type:           gossip.TypeHolePunchCoord,
		RequestID:      "req-1",
		CoordStage:     "reply",
		RelaySource:    "attacker-source",
		RelayTarget:    localID,
		AdvertisedAddr: "127.0.0.1:6553",
	})

	node.mu.RLock()
	_, stillPending := node.holePunchWait["req-1"]
	_, attackerKnown := node.knownPeers["attacker-source"]
	node.mu.RUnlock()
	if !stillPending {
		t.Fatalf("mismatched reply consumed pending request")
	}
	if attackerKnown {
		t.Fatalf("mismatched reply updated attacker peer")
	}

	node.handleHolePunchCoord(&peerConn{id: "relay-peer"}, gossip.Envelope{
		Type:           gossip.TypeHolePunchCoord,
		RequestID:      "req-1",
		CoordStage:     "reply",
		RelaySource:    "target-peer",
		RelayTarget:    localID,
		AdvertisedAddr: "127.0.0.1:6554",
	})

	node.mu.RLock()
	_, stillPending = node.holePunchWait["req-1"]
	known := node.knownPeers["target-peer"]
	node.mu.RUnlock()
	if stillPending {
		t.Fatalf("valid reply did not consume pending request")
	}
	if known.addr != "127.0.0.1:6554" {
		t.Fatalf("valid reply did not update target peer: %q", known.addr)
	}
}

func TestHolePunchCoordDoesNotPoisonPredictionObservations(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	node, err := NewNode("mesh-holepunch-prediction-poison", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}

	targetID := "target-peer"
	node.handleHolePunchCoord(&peerConn{id: "relay-peer"}, gossip.Envelope{
		Type:           gossip.TypeHolePunchCoord,
		RequestID:      "attacker-request",
		CoordStage:     "offer",
		RelaySource:    targetID,
		RelayTarget:    node.localPeerID(),
		AdvertisedAddr: "127.0.0.1:30000",
	})

	node.mu.RLock()
	info := node.knownPeers[targetID]
	node.mu.RUnlock()
	if got := info.observations[len(info.observations)-1]; got != "127.0.0.1:30000" {
		t.Fatalf("expected coord address in general observations, got %q", got)
	}
	if len(info.predictionObservations) != 0 {
		t.Fatalf("unexpected coord address in prediction observations: %#v", info.predictionObservations)
	}
}
