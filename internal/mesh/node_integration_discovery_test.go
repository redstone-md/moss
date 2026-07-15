package mesh

import (
	"encoding/hex"
	"net"
	"strconv"
	"testing"
	"time"

	mcrypto "github.com/redstone-md/moss/internal/crypto"
	"github.com/redstone-md/moss/internal/nat"
)

func TestDiscoveredPeersAutoConnectIntoOverlay(t *testing.T) {
	cfgB := DefaultConfig()
	cfgB.Trackers = nil
	cfgB.GossipSub.HeartbeatMS = 50
	nodeB, err := NewNode("mesh-overlay", nil, cfgB)
	if err != nil {
		t.Fatalf("NewNode nodeB failed: %v", err)
	}
	if code := nodeB.Start(); code != MOSS_OK {
		t.Fatalf("nodeB.Start failed: %d", code)
	}
	defer nodeB.Stop()

	makeLeaf := func(port int) *Node {
		cfg := DefaultConfig()
		cfg.Trackers = nil
		cfg.GossipSub.HeartbeatMS = 50
		cfg.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(nodeB.ListenPort()))}
		node, err := NewNode("mesh-overlay", nil, cfg)
		if err != nil {
			t.Fatalf("NewNode leaf failed: %v", err)
		}
		if code := node.Start(); code != MOSS_OK {
			t.Fatalf("leaf.Start failed: %d", code)
		}
		return node
	}

	nodeA := makeLeaf(0)
	defer nodeA.Stop()
	nodeC := makeLeaf(0)
	defer nodeC.Stop()

	waitForPeerCount(t, nodeB, 2)
	waitForPeerCount(t, nodeA, 1)
	waitForPeerCount(t, nodeC, 1)

	targetPub := nodeC.PublicKey()
	targetID := hex.EncodeToString(targetPub[:])
	waitForKnownPeer(t, nodeA, targetID)
	waitForDirectPeer(t, nodeA, targetID)

	sourcePub := nodeA.PublicKey()
	waitForDirectPeer(t, nodeC, hex.EncodeToString(sourcePub[:]))
}

func TestDiscoveredPeerReconnectsAfterRestartWithNewPort(t *testing.T) {
	cfgHub := DefaultConfig()
	cfgHub.Trackers = nil
	cfgHub.GossipSub.HeartbeatMS = 50
	hub, err := NewNode("mesh-overlay-restart", nil, cfgHub)
	if err != nil {
		t.Fatalf("NewNode hub failed: %v", err)
	}
	if code := hub.Start(); code != MOSS_OK {
		t.Fatalf("hub.Start failed: %d", code)
	}
	defer hub.Stop()

	newLeaf := func(identity *mcrypto.Identity) *Node {
		cfg := DefaultConfig()
		cfg.Trackers = nil
		cfg.GossipSub.HeartbeatMS = 50
		cfg.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(hub.ListenPort()))}
		node, err := NewNodeWithIdentity("mesh-overlay-restart", nil, cfg, identity)
		if err != nil {
			t.Fatalf("NewNode leaf failed: %v", err)
		}
		if code := node.Start(); code != MOSS_OK {
			t.Fatalf("leaf.Start failed: %d", code)
		}
		return node
	}

	nodeA := newLeaf(nil)
	defer nodeA.Stop()

	restartIdentity, err := mcrypto.NewIdentity()
	if err != nil {
		t.Fatalf("NewIdentity failed: %v", err)
	}
	nodeB := newLeaf(restartIdentity)
	defer nodeB.Stop()

	waitForPeerCount(t, hub, 2)
	waitForPeerCount(t, nodeA, 1)
	waitForPeerCount(t, nodeB, 1)

	nodeBPub := nodeB.PublicKey()
	nodeBID := hex.EncodeToString(nodeBPub[:])
	nodeAPub := nodeA.PublicKey()
	nodeAID := hex.EncodeToString(nodeAPub[:])
	waitForKnownPeer(t, nodeA, nodeBID)
	waitForDirectPeer(t, nodeA, nodeBID)
	waitForDirectPeer(t, nodeB, nodeAID)
	waitForPeerCount(t, nodeA, 2)

	nodeB.Stop()

	nodeB = newLeaf(restartIdentity)
	defer nodeB.Stop()
	waitForPeerCount(t, nodeB, 1)
	if nodeB.ListenPort() == 0 {
		t.Fatal("expected restarted nodeB to have a listen port")
	}
	restartedAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(nodeB.ListenPort()))
	waitForKnownPeer(t, nodeA, nodeBID)
	waitForDirectPeer(t, nodeA, nodeBID)
	waitForDirectPeer(t, nodeB, nodeAID)
	waitForKnownPeerAddr(t, nodeA, nodeBID, restartedAddr)
	waitForPeerCount(t, nodeA, 2)
}

func TestRelaySendToFallsBackAfterDirectDialFailure(t *testing.T) {
	cfgRelay := DefaultConfig()
	cfgRelay.Trackers = nil
	cfgRelay.GossipSub.HeartbeatMS = 50
	relayNode, err := NewNode("mesh-relay-known", nil, cfgRelay)
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
	nodeA, err := NewNode("mesh-relay-known", nil, cfgA)
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
	nodeB, err := NewNode("mesh-relay-known", nil, cfgB)
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
	relayPub := relayNode.PublicKey()
	relayID := hex.EncodeToString(relayPub[:])
	waitForKnownPeer(t, nodeA, targetID)

	nodeA.mu.Lock()
	relayInfo := nodeA.knownPeers[relayID]
	relayInfo.natType = nat.TypePublic
	relayInfo.natTrusted = true
	relayInfo.publicReachable = true
	relayInfo.relayCapable = true
	nodeA.knownPeers[relayID] = relayInfo
	info := nodeA.knownPeers[targetID]
	info.addr = "127.0.0.1:1"
	nodeA.knownPeers[targetID] = info
	nodeA.mu.Unlock()

	received := make(chan []byte, 1)
	nodeB.SetRelayCallback(func(senderID [32]byte, data []byte) {
		_ = senderID
		received <- append([]byte(nil), data...)
	})

	if err := nodeA.RelaySendTo(targetID, []byte("fallback-relay"), 500*time.Millisecond); err != nil {
		t.Fatalf("RelaySendTo failed: %v", err)
	}

	select {
	case payload := <-received:
		if string(payload) != "fallback-relay" {
			t.Fatalf("unexpected relay payload: %q", string(payload))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for fallback relay payload")
	}
}

func TestRelaySendToFallsBackToSecondaryRelayPeer(t *testing.T) {
	cfgRelay1 := DefaultConfig()
	cfgRelay1.Trackers = nil
	cfgRelay1.GossipSub.HeartbeatMS = 50
	cfgRelay1.NAT.RelayMaxSessions = 1
	relay1, err := NewNode("mesh-relay-secondary", nil, cfgRelay1)
	if err != nil {
		t.Fatalf("NewNode relay1 failed: %v", err)
	}
	if code := relay1.Start(); code != MOSS_OK {
		t.Fatalf("relay1.Start failed: %d", code)
	}
	defer relay1.Stop()

	cfgRelay2 := DefaultConfig()
	cfgRelay2.Trackers = nil
	cfgRelay2.GossipSub.HeartbeatMS = 50
	relay2, err := NewNode("mesh-relay-secondary", nil, cfgRelay2)
	if err != nil {
		t.Fatalf("NewNode relay2 failed: %v", err)
	}
	if code := relay2.Start(); code != MOSS_OK {
		t.Fatalf("relay2.Start failed: %d", code)
	}
	defer relay2.Stop()

	cfgA := DefaultConfig()
	cfgA.Trackers = nil
	cfgA.GossipSub.HeartbeatMS = 50
	cfgA.MaxPeers = 2
	cfgA.StaticPeers = []string{
		net.JoinHostPort("127.0.0.1", strconv.Itoa(relay1.ListenPort())),
		net.JoinHostPort("127.0.0.1", strconv.Itoa(relay2.ListenPort())),
	}
	nodeA, err := NewNode("mesh-relay-secondary", nil, cfgA)
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
	cfgB.MaxPeers = 2
	cfgB.StaticPeers = []string{
		net.JoinHostPort("127.0.0.1", strconv.Itoa(relay1.ListenPort())),
		net.JoinHostPort("127.0.0.1", strconv.Itoa(relay2.ListenPort())),
	}
	nodeB, err := NewNode("mesh-relay-secondary", nil, cfgB)
	if err != nil {
		t.Fatalf("NewNode nodeB failed: %v", err)
	}
	if code := nodeB.Start(); code != MOSS_OK {
		t.Fatalf("nodeB.Start failed: %d", code)
	}
	defer nodeB.Stop()

	waitForPeerCount(t, relay1, 2)
	waitForPeerCount(t, relay2, 2)
	waitForPeerCount(t, nodeA, 2)
	waitForPeerCount(t, nodeB, 2)

	targetPub := nodeB.PublicKey()
	targetID := hex.EncodeToString(targetPub[:])
	relay1Pub := relay1.PublicKey()
	relay1ID := hex.EncodeToString(relay1Pub[:])
	relay2Pub := relay2.PublicKey()
	relay2ID := hex.EncodeToString(relay2Pub[:])

	waitForKnownPeer(t, nodeA, targetID)
	nodeA.mu.Lock()
	info1 := nodeA.knownPeers[relay1ID]
	info1.relayCapable = true
	info1.publicReachable = true
	info1.natType = nat.TypePublic
	info1.natTrusted = true
	nodeA.knownPeers[relay1ID] = info1
	info2 := nodeA.knownPeers[relay2ID]
	info2.relayCapable = true
	info2.publicReachable = true
	info2.natType = nat.TypePublic
	info2.natTrusted = true
	nodeA.knownPeers[relay2ID] = info2
	nodeA.mu.Unlock()
	nodeA.scoring.SetApplicationScore(relay1ID, 10)
	nodeA.scoring.SetApplicationScore(relay2ID, 1)

	if !relay1.relaySessions.Acquire("saturated") {
		t.Fatal("expected relay1 capacity acquisition to succeed")
	}
	defer relay1.relaySessions.Release("saturated")

	received := make(chan []byte, 1)
	nodeB.SetRelayCallback(func(senderID [32]byte, data []byte) {
		_ = senderID
		received <- append([]byte(nil), data...)
	})

	if err := nodeA.RelaySendTo(targetID, []byte("secondary-relay"), 2*time.Second); err != nil {
		t.Fatalf("RelaySendTo failed: %v", err)
	}

	select {
	case payload := <-received:
		if string(payload) != "secondary-relay" {
			t.Fatalf("unexpected relay payload: %q", string(payload))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for fallback secondary relay payload")
	}
}

func TestRelaySessionAnyPrefersLessLoadedRelayPeer(t *testing.T) {
	cfgRelay1 := DefaultConfig()
	cfgRelay1.Trackers = nil
	cfgRelay1.GossipSub.HeartbeatMS = 50
	relay1, err := NewNode("mesh-relay-balance", nil, cfgRelay1)
	if err != nil {
		t.Fatalf("NewNode relay1 failed: %v", err)
	}
	if code := relay1.Start(); code != MOSS_OK {
		t.Fatalf("relay1.Start failed: %d", code)
	}
	defer relay1.Stop()

	cfgRelay2 := DefaultConfig()
	cfgRelay2.Trackers = nil
	cfgRelay2.GossipSub.HeartbeatMS = 50
	relay2, err := NewNode("mesh-relay-balance", nil, cfgRelay2)
	if err != nil {
		t.Fatalf("NewNode relay2 failed: %v", err)
	}
	if code := relay2.Start(); code != MOSS_OK {
		t.Fatalf("relay2.Start failed: %d", code)
	}
	defer relay2.Stop()

	cfgA := DefaultConfig()
	cfgA.Trackers = nil
	cfgA.GossipSub.HeartbeatMS = 50
	cfgA.MaxPeers = 2
	cfgA.StaticPeers = []string{
		net.JoinHostPort("127.0.0.1", strconv.Itoa(relay1.ListenPort())),
		net.JoinHostPort("127.0.0.1", strconv.Itoa(relay2.ListenPort())),
	}
	nodeA, err := NewNode("mesh-relay-balance", nil, cfgA)
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
	cfgB.MaxPeers = 2
	cfgB.StaticPeers = []string{
		net.JoinHostPort("127.0.0.1", strconv.Itoa(relay1.ListenPort())),
		net.JoinHostPort("127.0.0.1", strconv.Itoa(relay2.ListenPort())),
	}
	nodeB, err := NewNode("mesh-relay-balance", nil, cfgB)
	if err != nil {
		t.Fatalf("NewNode nodeB failed: %v", err)
	}
	if code := nodeB.Start(); code != MOSS_OK {
		t.Fatalf("nodeB.Start failed: %d", code)
	}
	defer nodeB.Stop()

	waitForPeerCount(t, relay1, 2)
	waitForPeerCount(t, relay2, 2)
	waitForPeerCount(t, nodeA, 2)
	waitForPeerCount(t, nodeB, 2)

	targetPub := nodeB.PublicKey()
	targetID := hex.EncodeToString(targetPub[:])
	relay1Pub := relay1.PublicKey()
	relay1ID := hex.EncodeToString(relay1Pub[:])
	relay2Pub := relay2.PublicKey()
	relay2ID := hex.EncodeToString(relay2Pub[:])

	waitForKnownPeer(t, nodeA, targetID)
	nodeA.mu.Lock()
	info1 := nodeA.knownPeers[relay1ID]
	info1.relayCapable = true
	info1.publicReachable = true
	info1.natType = nat.TypePublic
	info1.natTrusted = true
	nodeA.knownPeers[relay1ID] = info1
	info2 := nodeA.knownPeers[relay2ID]
	info2.relayCapable = true
	info2.publicReachable = true
	info2.natType = nat.TypePublic
	info2.natTrusted = true
	nodeA.knownPeers[relay2ID] = info2
	nodeA.relayLocals["existing-1"] = relayLocalSession{sessionID: "existing-1", viaPeerID: relay1ID, remotePeerID: "other-1", established: true}
	nodeA.relayLocals["existing-2"] = relayLocalSession{sessionID: "existing-2", viaPeerID: relay1ID, remotePeerID: "other-2", established: true}
	nodeA.mu.Unlock()
	nodeA.scoring.SetApplicationScore(relay1ID, 10)
	nodeA.scoring.SetApplicationScore(relay2ID, 10)
	nodeA.SetScoringCallback(func(peerID [32]byte, baseScore float64) float64 {
		if peerID == decodePeerID(relay1ID) || peerID == decodePeerID(relay2ID) {
			return 10
		}
		return baseScore
	})

	sessionID, err := nodeA.OpenRelaySessionAny(targetID, 2*time.Second)
	if err != nil {
		t.Fatalf("OpenRelaySessionAny failed: %v", err)
	}

	nodeA.mu.RLock()
	session := nodeA.relayLocals[sessionID]
	nodeA.mu.RUnlock()
	if session.viaPeerID != relay2ID {
		t.Fatalf("expected less-loaded relay %s, got %s", relay2ID, session.viaPeerID)
	}
}
