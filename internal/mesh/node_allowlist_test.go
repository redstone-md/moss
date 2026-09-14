package mesh

import (
	"encoding/hex"
	"net"
	"strconv"
	"testing"
	"time"
)

func startIsolatedNode(t *testing.T, name, meshID string, psk []byte) *Node {
	t.Helper()
	cfg := isolatedTestConfig(name)
	node, err := NewNode(meshID, psk, cfg)
	if err != nil {
		t.Fatalf("NewNode %s failed: %v", name, err)
	}
	if code := node.Start(); code != MOSS_OK {
		t.Fatalf("node %s start failed: %d", name, code)
	}
	t.Cleanup(func() { node.Stop() })
	return node
}

func publicKeyHex(t *testing.T, node *Node) string {
	t.Helper()
	pub := node.PublicKey()
	return hex.EncodeToString(pub[:])
}

// API contract: invalid IDs and the node's own ID are config errors; the
// first AllowPeer call switches the node to strict; nil map is accept-all.
func TestAllowlistAPIValidation(t *testing.T) {
	node, err := NewNode("mesh-allowlist", nil, isolatedTestConfig("allowlist-api"))
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	defer node.Stop()

	if !node.IsPeerAllowed("any-peer") {
		t.Fatal("no allowlist configured: open substrate must accept everyone")
	}
	if code := node.AllowPeer("short-id"); code != MOSS_ERR_CONFIG_INVALID {
		t.Fatalf("short peer ID must be MOSS_ERR_CONFIG_INVALID, got %d", code)
	}
	if code := node.AllowPeer(node.localPeerID()); code != MOSS_ERR_CONFIG_INVALID {
		t.Fatalf("self peer ID must be MOSS_ERR_CONFIG_INVALID, got %d", code)
	}
	if code := node.DisallowPeer("short-id"); code != MOSS_ERR_CONFIG_INVALID {
		t.Fatalf("short peer ID on disallow must be MOSS_ERR_CONFIG_INVALID, got %d", code)
	}

	nodeB := startIsolatedNode(t, "allowlist-api", "mesh-allowlist", nil)
	nodeC := startIsolatedNode(t, "allowlist-api", "mesh-allowlist", nil)
	idB := publicKeyHex(t, nodeB)
	idC := publicKeyHex(t, nodeC)

	if code := node.AllowPeer(idB); code != MOSS_OK {
		t.Fatalf("valid AllowPeer failed: %d", code)
	}
	if !node.IsPeerAllowed(idB) {
		t.Fatal("allowed peer must pass the list")
	}
	if node.IsPeerAllowed(idC) {
		t.Fatal("unlisted peer must fail strict list")
	}
	if code := node.DisallowPeer(idB); code != MOSS_OK {
		t.Fatalf("DisallowPeer failed: %d", code)
	}
	if node.IsPeerAllowed(idB) {
		t.Fatal("disallowed peer must not pass")
	}
	// Empty-but-present list stays strict: an operator removing the last
	// entry asked for "allow nobody", not "re-open to the substrate".
	if node.IsPeerAllowed(idB) {
		t.Fatal("empty allowlist must reject, not re-open")
	}
}

// A strict node registers ONLY listed peers: the allowed one connects, the
// unlisted one is dropped with a counted rejection and never enters n.peers.
func TestAllowlistRejectsUnlistedPeer(t *testing.T) {
	nodeA := startIsolatedNode(t, "allowlist-suite", "mesh-allowlist", nil)
	nodeB := startIsolatedNode(t, "allowlist-suite", "mesh-allowlist", nil)
	nodeC := startIsolatedNode(t, "allowlist-suite", "mesh-allowlist", nil)

	if code := nodeA.AllowPeer(publicKeyHex(t, nodeB)); code != MOSS_OK {
		t.Fatalf("AllowPeer failed: %d", code)
	}

	// Listed peer connects normally.
	if code := nodeB.Connect(net.JoinHostPort("127.0.0.1", strconv.Itoa(nodeA.ListenPort()))); code != MOSS_OK {
		t.Fatalf("listed peer Connect failed: %d", code)
	}
	waitForPeerCount(t, nodeA, 1)
	waitForPeerCount(t, nodeB, 1)

	// Unlisted peer is rejected at registration: not in n.peers, counted once.
	before := inboundCount(nodeA, "__allowlist_rejected__")
	_ = nodeC.Connect(net.JoinHostPort("127.0.0.1", strconv.Itoa(nodeA.ListenPort())))
	waitForPeerCountAtMost(t, nodeA, 1, 3*time.Second)
	nodeA.mu.RLock()
	_, registered := nodeA.peers[publicKeyHex(t, nodeC)]
	nodeA.mu.RUnlock()
	if registered {
		t.Fatal("unlisted peer was registered despite the allowlist")
	}
	// Every dial attempt legitimately counts (the caller may redial), so
	// the contract is "counted at least once", never "exactly once".
	if got := inboundCount(nodeA, "__allowlist_rejected__"); got <= before {
		t.Fatalf("allowlist rejection was never counted (before=%d after=%d)", before, got)
	}
}
