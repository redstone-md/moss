package mesh

import (
	"fmt"
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/nat"
)

const discoveryCandidateAddr = "203.0.113.250:4001"

func discoveryBudgetFixture(t *testing.T) *Node {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Trackers = nil
	node, err := NewNode("discovery-budget", nil, cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < cfg.GossipSub.DHigh; i++ {
		id := fmt.Sprintf("ambient-%d", i)
		node.peers[id] = &peerConn{id: id, addr: fmt.Sprintf("198.51.100.%d:4001", i+1), outbound: true}
		node.knownPeers[id] = knownPeer{id: id, relayCapable: i < relayDialQuota}
	}
	node.knownPeers["candidate"] = knownPeer{id: "candidate", addr: discoveryCandidateAddr, verified: true}
	node.trackerSeeds[discoveryCandidateAddr] = time.Now()
	return node
}

func automaticDiscoveryCount(node *Node, path string) int {
	switch path {
	case "known":
		return len(node.discoveredPeerTargets())
	case "tracker_kick":
		return len(node.kickTargets([]string{discoveryCandidateAddr}))
	default:
		return len(node.bootstrapSeedTargets())
	}
}

func TestAutomaticDiscoveryStopsAtSoftGoalAndRefills(t *testing.T) {
	for _, path := range []string{"known", "tracker_kick", "tracker_seed"} {
		t.Run(path, func(t *testing.T) {
			node := discoveryBudgetFixture(t)
			if got := automaticDiscoveryCount(node, path); got != 0 {
				t.Fatalf("selected %d extra dials at the discovery goal; hard limit=%d", got, node.config.MaxPeers)
			}
			delete(node.peers, "ambient-5")
			if got := automaticDiscoveryCount(node, path); got != 1 {
				t.Fatalf("lost neighbor was not replaced: selected %d dials", got)
			}
		})
	}
}

func TestAutomaticDiscoveryKeepsOutboundFloor(t *testing.T) {
	for _, path := range []string{"known", "tracker_kick", "tracker_seed"} {
		t.Run(path, func(t *testing.T) {
			node := discoveryBudgetFixture(t)
			for _, peer := range node.peers {
				peer.outbound = false
			}
			if got := automaticDiscoveryCount(node, path); got != 1 {
				t.Fatalf("inbound peers prevented an outbound dial: %d", got)
			}
		})
	}
}

func TestDiscoveryRelayRepairDoesNotDialExtraPlainPeers(t *testing.T) {
	node := discoveryBudgetFixture(t)
	for id, info := range node.knownPeers {
		info.relayCapable = false
		node.knownPeers[id] = info
	}
	if targets := node.discoveredPeerTargets(); len(targets) != 0 {
		t.Fatalf("relay deficit admitted plain neighbors: %v", targets)
	}
	node.knownPeers["relay"] = knownPeer{id: "relay", addr: "203.0.113.249:4001", verified: true, relayCapable: true}
	targets := node.discoveredPeerTargets()
	if len(targets) != 1 || targets[0].peerID != "relay" {
		t.Fatalf("relay deficit did not select only the capable candidate: %v", targets)
	}
}

func TestDiscoveryRepairsRelayDeficitDespitePublicGlare(t *testing.T) {
	node := discoveryBudgetFixture(t)
	node.natProfile.Store(nat.Profile{PublicReachable: true})
	for id, info := range node.knownPeers {
		info.relayCapable = false
		node.knownPeers[id] = info
	}
	const relayID = "0000000000000000000000000000000000000000000000000000000000000000"
	node.knownPeers[relayID] = knownPeer{
		id: relayID, addr: "203.0.113.249:4001", verified: true,
		relayCapable: true, publicReachable: true,
	}
	targets := node.discoveredPeerTargets()
	if len(targets) != 1 || targets[0].peerID != relayID {
		t.Fatalf("public glare prevented relay repair at the working goal: %v", targets)
	}
}

func TestStaticSeedsBypassDiscoveryGoal(t *testing.T) {
	node := discoveryBudgetFixture(t)
	node.config.StaticPeers = []string{discoveryCandidateAddr}
	// An earlier tracker port on the same host must not hide the static seed.
	node.trackerSeeds["203.0.113.250:4000"] = time.Now()
	if targets := node.bootstrapSeedTargets(); len(targets) != 1 || targets[0] != discoveryCandidateAddr {
		t.Fatalf("configured static seed was skipped: %v", targets)
	}
}

func TestRelayedNeighborsCountTowardDiscoveryGoal(t *testing.T) {
	node := discoveryBudgetFixture(t)
	node.peers["ambient-5"].relayed = true
	if targets := node.discoveredPeerTargets(); len(targets) != 0 {
		t.Fatalf("relayed neighbor triggered unnecessary background growth: %v", targets)
	}
}

func TestDiscoveryNeverExceedsRemainingHardCapacity(t *testing.T) {
	for _, path := range []string{"known", "tracker_kick", "tracker_seed"} {
		t.Run(path, func(t *testing.T) {
			node := discoveryBudgetFixture(t)
			node.config.MaxPeers = len(node.peers)
			node.config.StaticPeers = []string{discoveryCandidateAddr}
			for _, peer := range node.peers {
				peer.outbound = false
			}
			if got := automaticDiscoveryCount(node, path); got != 0 {
				t.Fatalf("selected %d dials at hard capacity", got)
			}
		})
	}
}

func TestDiscoveryClampsBatchToOneRemainingHardSlot(t *testing.T) {
	for _, path := range []string{"known", "tracker_kick", "tracker_seed"} {
		t.Run(path, func(t *testing.T) {
			node := discoveryBudgetFixture(t)
			node.config.MaxPeers = len(node.peers) + 1
			for _, peer := range node.peers {
				peer.outbound = false
			}
			addresses := []string{discoveryCandidateAddr, "203.0.113.248:4001", "203.0.113.249:4001"}
			for i, addr := range addresses {
				id := fmt.Sprintf("candidate-%d", i)
				node.knownPeers[id] = knownPeer{id: id, addr: addr, verified: true}
				node.trackerSeeds[addr] = time.Now()
			}
			count := 0
			switch path {
			case "known":
				count = len(node.discoveredPeerTargets())
			case "tracker_kick":
				count = len(node.kickTargets(addresses))
			default:
				count = len(node.bootstrapSeedTargets())
			}
			if count != 1 {
				t.Fatalf("one remaining hard slot selected %d dials", count)
			}
		})
	}
}
