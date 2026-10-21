package mesh

import (
	"testing"
	"time"
)

func startDiscoveryBudgetNode(t *testing.T) *Node {
	t.Helper()
	cfg := isolatedTestConfig(t.Name())
	cfg.GossipSub.D, cfg.GossipSub.DLo, cfg.GossipSub.DHigh, cfg.GossipSub.DOut = 1, 1, 1, 1
	cfg.Security.HandshakeTimeoutSec = 1
	node, err := NewNode("discovery-budget-contact", nil, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if code := node.Start(); code != MOSS_OK {
		t.Fatalf("Start: %d", code)
	}
	t.Cleanup(func() { node.Stop() })
	return node
}

func TestExplicitContactAndDeliveryBeyondDiscoveryGoal(t *testing.T) {
	caller, neighbor, target := startDiscoveryBudgetNode(t), startDiscoveryBudgetNode(t), startDiscoveryBudgetNode(t)
	// Manual Connect also bypasses the automatic goal.
	if code := caller.Connect(nodeAddr(neighbor)); code != MOSS_OK {
		t.Fatalf("manual neighbor connect: %d", code)
	}
	waitForConnectedPeer(t, caller, neighbor.localPeerID(), 5*time.Second)
	caller.mu.RLock()
	budget := caller.discoveryDialBudgetLocked()
	caller.mu.RUnlock()
	if budget != 0 {
		t.Fatalf("fixture has not filled the automatic goal: budget=%d", budget)
	}
	if code := caller.ConnectToPeer(target.localPeerID()); code != MOSS_OK {
		t.Fatalf("register absent contact: %d", code)
	}
	time.Sleep(100 * time.Millisecond)
	injectKnownPeer(caller, target.localPeerID(), target.ListenPort())
	waitForConnectedPeer(t, caller, target.localPeerID(), 8*time.Second)
	assertRelayMessages(t, caller, target)
}

func TestInboundAndManualContactBeyondDiscoveryGoal(t *testing.T) {
	caller, neighbor, target := startDiscoveryBudgetNode(t), startDiscoveryBudgetNode(t), startDiscoveryBudgetNode(t)
	for _, peer := range []*Node{neighbor, target} {
		if code := peer.Connect(nodeAddr(caller)); code != MOSS_OK {
			t.Fatalf("manual contact connect: %d", code)
		}
		waitForConnectedPeer(t, caller, peer.localPeerID(), 5*time.Second)
	}
	assertRelayMessages(t, caller, target)
}
