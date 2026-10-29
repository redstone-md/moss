package mesh

import (
	"fmt"
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/nat"
)

func startDiscoveryBudgetNode(t *testing.T) *Node {
	t.Helper()
	cfg := isolatedTestConfig(t.Name())
	cfg.GossipSub.D, cfg.GossipSub.DLo, cfg.GossipSub.DHigh, cfg.GossipSub.DOut = 1, 1, 1, 1
	cfg.Security.HandshakeTimeoutSec = 1
	node, err := NewNode("discovery-budget-contact", nil, withFixtureListenPort(cfg))
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

func TestAutomaticRelayFallbackRespectsDiscoveryGoal(t *testing.T) {
	for _, goal := range []int{1, 2} {
		t.Run(fmt.Sprint(goal), func(t *testing.T) {
			core := startCappedOverlayNode(t, "automatic-relay-core")
			makeCore(core)
			cfg := overlayTestConfig()
			cfg.GossipSub.D, cfg.GossipSub.DLo, cfg.GossipSub.DOut = 1, 1, 1
			cfg.GossipSub.DHigh = goal
			cfg.Security.HandshakeTimeoutSec = 1
			caller := startOverlayNodeWithConfig(t, "automatic-relay-caller", cfg, 1)
			target := startCappedOverlayNode(t, "automatic-relay-target", 1)
			attachLeaf(t, caller, core)
			attachLeaf(t, target, core)
			caller.handleEnvelope(caller.peerByID(core.localPeerID()), core.signSupernodeEnvelope(gossip.Envelope{
				Type: gossip.TypeSupernodeAnnounce, AdvertisedPeerID: core.localPeerID(), AdvertisedAddr: nodeAddr(core),
				AdvertisedNATType: string(nat.TypePublic), AdvertisedReachable: true, AdvertisedRelayCapable: true,
			}))
			// The occupied direct slot forces the real discovery dial through
			// its relay fallback, without stubbing transport or key admission.
			caller.dialKnownPeer(target.localPeerID(), nodeAddr(target))
			peer := caller.peerByID(target.localPeerID())
			if goal == 1 {
				if peer != nil {
					t.Fatal("automatic fallback grew past the neighbor goal")
				}
				// A real contact still gets its encrypted relay beyond the goal.
				if _, err := caller.OpenRelaySession(core.localPeerID(), target.localPeerID(), 2*time.Second); err != nil {
					t.Fatal(err)
				}
			} else if peer == nil || !peer.relayed {
				t.Fatal("automatic fallback below the neighbor goal stopped working")
			}
			assertRelayMessages(t, caller, target)
		})
	}
}

// A discovery dial still in flight when Waiting begins must not grow the mesh
// through its relay fallback.
func TestDiscoveryRelayFallbackStopsOnceWaitingBegins(t *testing.T) {
	core := startCappedOverlayNode(t, "waiting-relay-core")
	makeCore(core)
	cfg := overlayTestConfig()
	cfg.GossipSub.D, cfg.GossipSub.DLo, cfg.GossipSub.DOut = 1, 1, 1
	cfg.GossipSub.DHigh = 2
	cfg.Security.HandshakeTimeoutSec = 1
	caller := startOverlayNodeWithConfig(t, "waiting-relay-caller", cfg, 1)
	target := startCappedOverlayNode(t, "waiting-relay-target", 1)
	attachLeaf(t, caller, core)
	attachLeaf(t, target, core)
	caller.handleEnvelope(caller.peerByID(core.localPeerID()), core.signSupernodeEnvelope(gossip.Envelope{
		Type: gossip.TypeSupernodeAnnounce, AdvertisedPeerID: core.localPeerID(), AdvertisedAddr: nodeAddr(core),
		AdvertisedNATType: string(nat.TypePublic), AdvertisedReachable: true, AdvertisedRelayCapable: true,
	}))
	caller.activity.requested.Store(int32(ActivityWaiting))
	caller.dialKnownPeer(target.localPeerID(), nodeAddr(target))
	if caller.peerByID(target.localPeerID()) != nil {
		t.Fatal("a discovery dial added a relayed peer after Waiting began")
	}
}
