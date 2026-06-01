package mesh

import (
	"bytes"
	"encoding/hex"
	"net"
	"strconv"
	"testing"
	"time"

	"moss/internal/nat"
)

func TestRelaySendToAutoOpensRelaySession(t *testing.T) {
	cfgRelay := DefaultConfig()
	cfgRelay.Trackers = nil
	cfgRelay.GossipSub.HeartbeatMS = 50
	relayNode, err := NewNode("mesh-relay-auto", nil, cfgRelay)
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
	nodeA, err := NewNode("mesh-relay-auto", nil, cfgA)
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
	nodeB, err := NewNode("mesh-relay-auto", nil, cfgB)
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
	relayID := hex.EncodeToString(relayPub[:])
	nodeA.mu.Lock()
	relayInfo := nodeA.knownPeers[relayID]
	relayInfo.natType = nat.TypePublic
	relayInfo.natTrusted = true
	relayInfo.publicReachable = true
	relayInfo.relayCapable = true
	nodeA.knownPeers[relayID] = relayInfo
	nodeA.mu.Unlock()

	received := make(chan []byte, 1)
	nodeB.SetRelayCallback(func(senderID [32]byte, data []byte) {
		_ = senderID
		received <- append([]byte(nil), data...)
	})

	targetPub := nodeB.PublicKey()
	if err := nodeA.RelaySendTo(hex.EncodeToString(targetPub[:]), []byte("auto-relay"), 2*time.Second); err != nil {
		if err.Error() == "target peer became directly connected; use direct transport" || err.Error() == "target peer is directly connected" {
			return
		}
		t.Fatalf("RelaySendTo failed: %v", err)
	}

	select {
	case payload := <-received:
		if string(payload) != "auto-relay" {
			t.Fatalf("unexpected relay payload: %q", string(payload))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for auto-relayed payload")
	}
}

func TestRelayNodeEnforcesSessionLimit(t *testing.T) {
	cfgRelay := DefaultConfig()
	cfgRelay.Trackers = nil
	cfgRelay.GossipSub.HeartbeatMS = 5000
	cfgRelay.NAT.RelayMaxSessions = 1
	relayNode, err := NewNode("mesh-relay-limit", nil, cfgRelay)
	if err != nil {
		t.Fatalf("NewNode relay failed: %v", err)
	}
	if code := relayNode.Start(); code != MOSS_OK {
		t.Fatalf("relayNode.Start failed: %d", code)
	}
	defer relayNode.Stop()

	makeLeaf := func() *Node {
		cfg := DefaultConfig()
		cfg.Trackers = nil
		cfg.GossipSub.HeartbeatMS = 5000
		cfg.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(relayNode.ListenPort()))}
		node, err := NewNode("mesh-relay-limit", nil, cfg)
		if err != nil {
			t.Fatalf("NewNode leaf failed: %v", err)
		}
		if code := node.Start(); code != MOSS_OK {
			t.Fatalf("leaf.Start failed: %d", code)
		}
		return node
	}

	nodeA := makeLeaf()
	defer nodeA.Stop()
	nodeB := makeLeaf()
	defer nodeB.Stop()
	nodeC := makeLeaf()
	defer nodeC.Stop()
	nodeD := makeLeaf()
	defer nodeD.Stop()

	waitForPeerCount(t, relayNode, 4)

	relayPub := relayNode.PublicKey()
	nodeBPub := nodeB.PublicKey()
	sessionID, err := nodeA.OpenRelaySession(hex.EncodeToString(relayPub[:]), hex.EncodeToString(nodeBPub[:]), 2*time.Second)
	if err != nil {
		t.Fatalf("OpenRelaySession A->B failed: %v", err)
	}
	waitForRelaySession(t, nodeA, sessionID)
	waitForRelaySession(t, nodeB, sessionID)
	waitForRelayRoute(t, relayNode, sessionID)

	nodeDPub := nodeD.PublicKey()
	if _, err := nodeC.OpenRelaySession(hex.EncodeToString(relayPub[:]), hex.EncodeToString(nodeDPub[:]), 400*time.Millisecond); err == nil {
		t.Fatal("expected second relay session to be rejected by relay session limit")
	}
}

func TestRelayNodeEnforcesConfiguredBandwidth(t *testing.T) {
	cfgRelay := DefaultConfig()
	cfgRelay.Trackers = nil
	cfgRelay.GossipSub.HeartbeatMS = 5000
	cfgRelay.NAT.RelayMaxBandwidthKBPS = 1
	cfgRelay.Security.RateLimitBurst = 1 << 20
	cfgRelay.Security.RateLimitSustained = 1 << 20
	relayNode, err := NewNode("mesh-relay-bandwidth", nil, cfgRelay)
	if err != nil {
		t.Fatalf("NewNode relay failed: %v", err)
	}
	if code := relayNode.Start(); code != MOSS_OK {
		t.Fatalf("relayNode.Start failed: %d", code)
	}
	defer relayNode.Stop()

	cfgA := DefaultConfig()
	cfgA.Trackers = nil
	cfgA.GossipSub.HeartbeatMS = 5000
	cfgA.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(relayNode.ListenPort()))}
	nodeA, err := NewNode("mesh-relay-bandwidth", nil, cfgA)
	if err != nil {
		t.Fatalf("NewNode nodeA failed: %v", err)
	}
	if code := nodeA.Start(); code != MOSS_OK {
		t.Fatalf("nodeA.Start failed: %d", code)
	}
	defer nodeA.Stop()

	cfgB := DefaultConfig()
	cfgB.Trackers = nil
	cfgB.GossipSub.HeartbeatMS = 5000
	cfgB.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(relayNode.ListenPort()))}
	nodeB, err := NewNode("mesh-relay-bandwidth", nil, cfgB)
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

	received := make(chan []byte, 2)
	nodeB.SetRelayCallback(func(senderID [32]byte, data []byte) {
		_ = senderID
		received <- append([]byte(nil), data...)
	})

	relayPub := relayNode.PublicKey()
	targetPub := nodeB.PublicKey()
	sessionID, err := nodeA.OpenRelaySession(hex.EncodeToString(relayPub[:]), hex.EncodeToString(targetPub[:]), 2*time.Second)
	if err != nil {
		t.Fatalf("OpenRelaySession failed: %v", err)
	}

	firstPayload := bytes.Repeat([]byte("a"), 1024)
	secondPayload := bytes.Repeat([]byte("b"), 1024)
	if err := nodeA.RelaySend(sessionID, firstPayload); err != nil {
		t.Fatalf("first RelaySend failed: %v", err)
	}
	if err := nodeA.RelaySend(sessionID, secondPayload); err != nil {
		t.Fatalf("second RelaySend failed: %v", err)
	}

	select {
	case payload := <-received:
		if !bytes.Equal(payload, firstPayload) {
			t.Fatalf("unexpected first relay payload: %q", string(payload))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for first relayed payload")
	}

	select {
	case payload := <-received:
		t.Fatalf("expected second relay payload to be throttled, got %d bytes", len(payload))
	case <-time.After(400 * time.Millisecond):
	}
}

func TestRelayBandwidthOverloadDemotesAndRecoversSupernode(t *testing.T) {
	cfgRelay := DefaultConfig()
	cfgRelay.Trackers = nil
	cfgRelay.GossipSub.HeartbeatMS = 50
	cfgRelay.NAT.SuperNodeMinUptimeSec = 0
	cfgRelay.NAT.RelayMaxBandwidthKBPS = 1
	cfgRelay.Security.RateLimitBurst = 1 << 20
	cfgRelay.Security.RateLimitSustained = 1 << 20
	relayNode, err := NewNode("mesh-relay-overload", nil, cfgRelay)
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
	nodeA, err := NewNode("mesh-relay-overload", nil, cfgA)
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
	nodeB, err := NewNode("mesh-relay-overload", nil, cfgB)
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
	relayID := hex.EncodeToString(relayPub[:])
	waitForKnownPeer(t, nodeA, relayID)
	relayNode.natProfile.Store(nat.Profile{
		Type:            nat.TypePublic,
		PublicReachable: true,
		ExternalAddress: net.JoinHostPort("203.0.113.30", strconv.Itoa(relayNode.ListenPort())),
	})
	relayNode.refreshSupernodeStatus()
	waitForKnownPeerRelayCapable(t, nodeA, relayID, true)

	targetPub := nodeB.PublicKey()
	sessionID, err := nodeA.OpenRelaySession(relayID, hex.EncodeToString(targetPub[:]), 2*time.Second)
	if err != nil {
		t.Fatalf("OpenRelaySession failed: %v", err)
	}

	firstPayload := bytes.Repeat([]byte("a"), 1024)
	secondPayload := bytes.Repeat([]byte("b"), 1024)
	if err := nodeA.RelaySend(sessionID, firstPayload); err != nil {
		t.Fatalf("first RelaySend failed: %v", err)
	}
	if err := nodeA.RelaySend(sessionID, secondPayload); err != nil {
		t.Fatalf("second RelaySend failed: %v", err)
	}

	waitForKnownPeerRelayCapable(t, nodeA, relayID, false)
	time.Sleep(relayNode.relayOverloadCooldown() + 150*time.Millisecond)
	waitForKnownPeerRelayCapable(t, nodeA, relayID, true)
}

func TestSupernodeStatusAnnounceAndRevokePropagatesOnce(t *testing.T) {
	cfgA := DefaultConfig()
	cfgA.Trackers = nil
	cfgA.GossipSub.HeartbeatMS = 50
	cfgA.NAT.SuperNodeMinUptimeSec = 0
	nodeA, err := NewNode("mesh-supernode-status", nil, cfgA)
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
	nodeB, err := NewNode("mesh-supernode-status", nil, cfgB)
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
		t.Fatal("timed out waiting for supernode promotion event")
	}

	select {
	case <-promoted:
		t.Fatal("unexpected duplicate supernode promotion event")
	case <-time.After(200 * time.Millisecond):
	}

	nodeA.natProfile.Store(nat.Profile{
		Type:            nat.TypeSymmetric,
		PublicReachable: false,
		ExternalAddress: net.JoinHostPort("203.0.113.10", strconv.Itoa(nodeA.ListenPort())),
	})
	nodeA.refreshSupernodeStatus()

	waitForKnownPeerRelayCapable(t, nodeB, nodeAID, false)
	select {
	case <-revoked:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for supernode revoke event")
	}

	select {
	case <-revoked:
		t.Fatal("unexpected duplicate supernode revoke event")
	case <-time.After(200 * time.Millisecond):
	}
}

func TestPeerAnnouncementsPopulateKnownPeerDirectory(t *testing.T) {
	cfgRelay := DefaultConfig()
	cfgRelay.Trackers = nil
	cfgRelay.GossipSub.HeartbeatMS = 50
	relayNode, err := NewNode("mesh-directory", nil, cfgRelay)
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
	nodeA, err := NewNode("mesh-directory", nil, cfgA)
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
	nodeB, err := NewNode("mesh-directory", nil, cfgB)
	if err != nil {
		t.Fatalf("NewNode nodeB failed: %v", err)
	}
	if code := nodeB.Start(); code != MOSS_OK {
		t.Fatalf("nodeB.Start failed: %d", code)
	}
	defer nodeB.Stop()

	targetPub := nodeB.PublicKey()
	waitForKnownPeer(t, nodeA, hex.EncodeToString(targetPub[:]))
}
