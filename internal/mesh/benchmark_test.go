package mesh

import (
	"encoding/hex"
	"net"
	"strconv"
	"testing"
	"time"
)

func BenchmarkDirectPublishThroughput(b *testing.B) {
	payload := make([]byte, 32*1024)
	cfgA := DefaultConfig()
	cfgA.Trackers = nil
	cfgA.GossipSub.HeartbeatMS = 50
	nodeA, err := NewNode("mesh-bench-direct", nil, cfgA)
	if err != nil {
		b.Fatalf("NewNode nodeA failed: %v", err)
	}
	if code := nodeA.Start(); code != MOSS_OK {
		b.Fatalf("nodeA.Start failed: %d", code)
	}
	b.Cleanup(func() { nodeA.Stop() })

	cfgB := DefaultConfig()
	cfgB.Trackers = nil
	cfgB.GossipSub.HeartbeatMS = 50
	cfgB.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(nodeA.ListenPort()))}
	nodeB, err := NewNode("mesh-bench-direct", nil, cfgB)
	if err != nil {
		b.Fatalf("NewNode nodeB failed: %v", err)
	}
	if code := nodeB.Start(); code != MOSS_OK {
		b.Fatalf("nodeB.Start failed: %d", code)
	}
	b.Cleanup(func() { nodeB.Stop() })

	waitForPeerCountBench(b, nodeA, 1)
	waitForPeerCountBench(b, nodeB, 1)
	if code := nodeA.Subscribe("alpha"); code != MOSS_OK {
		b.Fatalf("nodeA.Subscribe failed: %d", code)
	}
	if code := nodeB.Subscribe("alpha"); code != MOSS_OK {
		b.Fatalf("nodeB.Subscribe failed: %d", code)
	}
	time.Sleep(150 * time.Millisecond)

	received := make(chan struct{}, benchMax(1, b.N))
	nodeB.SetMessageCallback(func(channel string, senderID [32]byte, data []byte) {
		if channel == "alpha" && len(data) == len(payload) {
			select {
			case received <- struct{}{}:
			default:
			}
		}
	})

	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if code := nodeA.Publish("alpha", payload); code != MOSS_OK {
			b.Fatalf("Publish failed: %d", code)
		}
		select {
		case <-received:
		case <-time.After(3 * time.Second):
			b.Fatal("timed out waiting for direct delivery")
		}
	}
}

func BenchmarkRelaySendThroughput(b *testing.B) {
	payload := make([]byte, 16*1024)
	cfgRelay := DefaultConfig()
	cfgRelay.Trackers = nil
	cfgRelay.GossipSub.HeartbeatMS = 10_000
	relayNode, err := NewNode("mesh-bench-relay", nil, cfgRelay)
	if err != nil {
		b.Fatalf("NewNode relay failed: %v", err)
	}
	if code := relayNode.Start(); code != MOSS_OK {
		b.Fatalf("relayNode.Start failed: %d", code)
	}
	b.Cleanup(func() { relayNode.Stop() })

	cfgA := DefaultConfig()
	cfgA.Trackers = nil
	cfgA.GossipSub.HeartbeatMS = 10_000
	cfgA.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(relayNode.ListenPort()))}
	nodeA, err := NewNode("mesh-bench-relay", nil, cfgA)
	if err != nil {
		b.Fatalf("NewNode nodeA failed: %v", err)
	}
	if code := nodeA.Start(); code != MOSS_OK {
		b.Fatalf("nodeA.Start failed: %d", code)
	}
	b.Cleanup(func() { nodeA.Stop() })

	cfgB := DefaultConfig()
	cfgB.Trackers = nil
	cfgB.GossipSub.HeartbeatMS = 10_000
	cfgB.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(relayNode.ListenPort()))}
	nodeB, err := NewNode("mesh-bench-relay", nil, cfgB)
	if err != nil {
		b.Fatalf("NewNode nodeB failed: %v", err)
	}
	if code := nodeB.Start(); code != MOSS_OK {
		b.Fatalf("nodeB.Start failed: %d", code)
	}
	b.Cleanup(func() { nodeB.Stop() })

	waitForPeerCountBench(b, relayNode, 2)
	waitForPeerCountBench(b, nodeA, 1)
	waitForPeerCountBench(b, nodeB, 1)

	relayPub := relayNode.PublicKey()
	targetPub := nodeB.PublicKey()
	sessionID, err := nodeA.OpenRelaySession(hex.EncodeToString(relayPub[:]), hex.EncodeToString(targetPub[:]), 2*time.Second)
	if err != nil {
		b.Fatalf("OpenRelaySession failed: %v", err)
	}
	waitForRelaySessionBench(b, nodeA, sessionID)
	waitForRelaySessionBench(b, nodeB, sessionID)

	received := make(chan struct{}, benchMax(1, b.N))
	nodeB.SetRelayCallback(func(senderID [32]byte, data []byte) {
		if len(data) == len(payload) {
			select {
			case received <- struct{}{}:
			default:
			}
		}
	})

	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := nodeA.RelaySend(sessionID, payload); err != nil {
			b.Fatalf("RelaySend failed: %v", err)
		}
		select {
		case <-received:
		case <-time.After(3 * time.Second):
			b.Fatal("timed out waiting for relayed delivery")
		}
	}
}

func waitForPeerCountBench(tb testing.TB, node *Node, want int) {
	tb.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if peerCount(node) >= want {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	tb.Fatalf("peer count did not reach %d; info=%s", want, node.MeshInfoJSON())
}

func waitForRelaySessionBench(tb testing.TB, node *Node, sessionID string) {
	tb.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		node.mu.RLock()
		session, ok := node.relayLocals[sessionID]
		node.mu.RUnlock()
		if ok && session.established {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	tb.Fatalf("relay session %s was not established", sessionID)
}

func peerCount(node *Node) int {
	node.mu.RLock()
	defer node.mu.RUnlock()
	return len(node.peers)
}

func benchMax(a, b int) int {
	if a > b {
		return a
	}
	return b
}
