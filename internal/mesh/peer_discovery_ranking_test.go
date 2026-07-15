package mesh

import (
	"fmt"
	"testing"
	"time"
)

// rankingTestNode returns an unstarted node whose dial-target selection can be
// driven purely through injected knownPeers / peers state.
func rankingTestNode(t *testing.T, dOut int) *Node {
	t.Helper()
	cfg := isolatedTestConfig("dial-ranking")
	cfg.GossipSub.DOut = dOut
	node, err := NewNode("mesh-dial-ranking", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	return node
}

func addRankingCandidate(node *Node, id string, relayCapable bool) {
	node.mu.Lock()
	node.knownPeers[id] = knownPeer{
		id:              id,
		addr:            "203.0.113.10:4001",
		verified:        true,
		publicReachable: true,
		natTrusted:      true,
		relayCapable:    relayCapable,
		lastSeen:        time.Now(),
	}
	node.mu.Unlock()
}

func addConnectedRelayCapable(node *Node, id string) {
	addRankingCandidate(node, id, true)
	node.mu.Lock()
	node.peers[id] = &peerConn{id: id, addr: "203.0.113.9:4001"}
	node.mu.Unlock()
}

func selectedIDs(node *Node) map[string]bool {
	ids := make(map[string]bool)
	for _, target := range node.discoveredPeerTargets() {
		ids[target.peerID] = true
	}
	return ids
}

// With the relay quota already met by connected peers, dial targets must not
// prefer relay-capable peers: every node clustering on supernodes turns the
// mesh into a star and concentrates the whole network's gossip on them.
func TestDialTargetsSkipRelayCapableWhenQuotaMet(t *testing.T) {
	node := rankingTestNode(t, 3)
	addConnectedRelayCapable(node, "aa"+fmt.Sprintf("%062d", 1))
	addConnectedRelayCapable(node, "aa"+fmt.Sprintf("%062d", 2))

	plain := []string{}
	for i := 0; i < 3; i++ {
		id := "cc" + fmt.Sprintf("%062d", i)
		plain = append(plain, id)
		addRankingCandidate(node, id, false)
	}
	for i := 0; i < 3; i++ {
		addRankingCandidate(node, "bb"+fmt.Sprintf("%062d", i), true)
	}

	selected := selectedIDs(node)
	for _, id := range plain {
		if !selected[id] {
			t.Fatalf("plain peer %s not selected; selected=%v", id, selected)
		}
	}
	for id := range selected {
		if id[:2] == "bb" {
			t.Fatalf("relay-capable peer %s selected while quota is met and plain peers exist", id)
		}
	}
}

// With no relay-capable peer connected, the deficit (quota of 2) is filled
// with relay-capable candidates first — relay fallback and relay_ready need
// at least a couple of them — but never more than the deficit while plain
// candidates remain.
func TestDialTargetsFillRelayQuotaDeficitOnly(t *testing.T) {
	node := rankingTestNode(t, 4)
	for i := 0; i < 3; i++ {
		addRankingCandidate(node, "bb"+fmt.Sprintf("%062d", i), true)
	}
	for i := 0; i < 3; i++ {
		addRankingCandidate(node, "cc"+fmt.Sprintf("%062d", i), false)
	}

	selected := selectedIDs(node)
	relayCount := 0
	for id := range selected {
		if id[:2] == "bb" {
			relayCount++
		}
	}
	if relayCount != 2 {
		t.Fatalf("expected exactly 2 relay-capable targets (the quota deficit), got %d; selected=%v", relayCount, selected)
	}
	if len(selected) != 4 {
		t.Fatalf("expected DOut=4 targets, got %d: %v", len(selected), selected)
	}
}
