package mesh

import (
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
)

// DisallowPeer is a revocation, not just list maintenance: it must tear down
// the live session so the disallowed peer actually leaves, not merely fail to
// be admitted next time.
func TestDisallowPeerKicksLiveSession(t *testing.T) {
	nodeA := startIsolatedNode(t, "allowlist-kick", "mesh-allowlist-kick", nil)
	nodeB := startIsolatedNode(t, "allowlist-kick", "mesh-allowlist-kick", nil)

	if code := nodeA.AllowPeer(publicKeyHex(t, nodeB)); code != MOSS_OK {
		t.Fatalf("AllowPeer failed: %d", code)
	}
	if code := nodeB.Connect(net.JoinHostPort("127.0.0.1", strconv.Itoa(nodeA.ListenPort()))); code != MOSS_OK {
		t.Fatalf("listed peer Connect failed: %d", code)
	}
	waitForPeerCount(t, nodeA, 1)
	waitForPeerCount(t, nodeB, 1)

	if code := nodeA.DisallowPeer(publicKeyHex(t, nodeB)); code != MOSS_OK {
		t.Fatalf("DisallowPeer failed: %d", code)
	}
	// removePeer runs synchronously inside DisallowPeer, so A's bookkeeping
	// must already be clean — no grace period, no background worker to wait on.
	nodeA.mu.RLock()
	_, still := nodeA.peers[publicKeyHex(t, nodeB)]
	nodeA.mu.RUnlock()
	if still {
		t.Fatal("DisallowPeer returned but the peer is still registered on A")
	}

	// The transport close makes B's reader exit with EOF, so B tears its own
	// side down naturally. A must also stay at 0: B's redials are now gated.
	waitForPeerCountEventuallyAtMost(t, nodeB, 0, 8*time.Second)
	waitForPeerCountAtMost(t, nodeA, 0, 3*time.Second)
}

// Strict-enable is admission control, not eviction: AllowPeer on an open node
// switches future registrations to strict but leaves the existing mesh alone.
func TestStrictEnableDoesNotKickExistingPeer(t *testing.T) {
	nodeA := startIsolatedNode(t, "allowlist-strict", "mesh-allowlist-strict", nil)
	nodeB := startIsolatedNode(t, "allowlist-strict", "mesh-allowlist-strict", nil)
	nodeC := startIsolatedNode(t, "allowlist-strict", "mesh-allowlist-strict", nil)

	if code := nodeB.Connect(net.JoinHostPort("127.0.0.1", strconv.Itoa(nodeA.ListenPort()))); code != MOSS_OK {
		t.Fatalf("Connect failed: %d", code)
	}
	waitForPeerCount(t, nodeA, 1)
	waitForPeerCount(t, nodeB, 1)

	// A goes strict listing only C. B is connected already; the strict
	// switch must not retroactively evict it.
	if code := nodeA.AllowPeer(publicKeyHex(t, nodeC)); code != MOSS_OK {
		t.Fatalf("AllowPeer failed: %d", code)
	}
	time.Sleep(300 * time.Millisecond)

	nodeA.mu.RLock()
	_, still := nodeA.peers[publicKeyHex(t, nodeB)]
	count := len(nodeA.peers)
	nodeA.mu.RUnlock()
	if !still || count != 1 {
		t.Fatalf("strict-enable must not kick the existing peer: present=%v count=%d", still, count)
	}
	if nodeA.currentPeerCount() != 1 {
		t.Fatalf("currentPeerCount after strict-enable: %d", nodeA.currentPeerCount())
	}
}

// DisallowPeer on a node that never created an allowlist is a no-op: there is
// no list entry to remove and revoking admission was never list-based.
func TestDisallowPeerWithoutAllowlistIsNoOp(t *testing.T) {
	nodeA := startIsolatedNode(t, "allowlist-open", "mesh-allowlist-open", nil)
	nodeB := startIsolatedNode(t, "allowlist-open", "mesh-allowlist-open", nil)

	if code := nodeB.Connect(net.JoinHostPort("127.0.0.1", strconv.Itoa(nodeA.ListenPort()))); code != MOSS_OK {
		t.Fatalf("Connect failed: %d", code)
	}
	waitForPeerCount(t, nodeA, 1)
	waitForPeerCount(t, nodeB, 1)

	if code := nodeA.DisallowPeer(publicKeyHex(t, nodeB)); code != MOSS_OK {
		t.Fatalf("DisallowPeer on an open node must be MOSS_OK, got %d", code)
	}
	waitForPeerCountAtLeast(t, nodeA, 1, 2*time.Second)
}

// The relayed registration gate must mirror the direct one: an unlisted NEW
// remote is refused with a counted rejection; a listed one registers; a
// session migration for an existing relayed remote is never policy-refused;
// an existing DIRECT remote takes the early return, not the policy path.
func TestRegisterRelayedPeerLockedAllowlistGate(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxPeers = 10
	node := &Node{
		peers: map[string]*peerConn{
			"via": {id: "via", outbound: true},
			// An existing relayed remote: a migration must replace it, not be
			// policy-refused even though the list does not name it.
			"relay-known": {id: "relay-known", relayed: true, relaySessionID: "sess-known"},
			// An existing direct remote: hit before the gate, never a policy answer.
			"direct-peer": {id: "direct-peer"},
		},
		knownPeers: map[string]knownPeer{
			"via":          {id: "via", direct: true, noiseStatic: make([]byte, 32)},
			"relay-known":  {id: "relay-known", noiseStatic: make([]byte, 32)},
			"relay-listed": {id: "relay-listed", noiseStatic: make([]byte, 32)},
			"relay-new":    {id: "relay-new", noiseStatic: make([]byte, 32)},
		},
		allowlist: map[string]struct{}{"relay-listed": {}},
		scoring:   gossip.NewEngine(),
		config:    cfg,
	}

	node.mu.Lock()
	defer node.mu.Unlock()

	// (a) Unlisted new remote: policy-refused, counted.
	peer, refused := node.registerRelayedPeerLocked(relayLocalSession{
		sessionID: "sess-new", viaPeerID: "via", remotePeerID: "relay-new",
	})
	if peer != nil || !refused {
		t.Fatalf("unlisted new remote must be policy-refused: peer=%v refused=%v", peer, refused)
	}
	if v, ok := node.inboundByType.Load("__allowlist_rejected__"); !ok {
		t.Fatal("__allowlist_rejected__ counter not recorded")
	} else if v.(*atomic.Uint64).Load() < 1 {
		t.Fatal("allowlist rejection was not counted")
	}

	// (b) Listed new remote: registers.
	peer, refused = node.registerRelayedPeerLocked(relayLocalSession{
		sessionID: "sess-listed", viaPeerID: "via", remotePeerID: "relay-listed",
	})
	if refused || peer == nil {
		t.Fatalf("listed new remote must register: peer=%v refused=%v", peer, refused)
	}

	// (c) Migration of an existing unlisted relayed remote: replacement, not
	// a policy refusal — the peer is already present by an earlier decision.
	peer, refused = node.registerRelayedPeerLocked(relayLocalSession{
		sessionID: "sess-migrated", viaPeerID: "via", remotePeerID: "relay-known",
	})
	if refused || peer == nil || peer.relaySessionID != "sess-migrated" {
		t.Fatalf("migration of an existing relayed remote must replace: peer=%v refused=%v", peer, refused)
	}

	// (d) Existing DIRECT remote: the early return (nil, false), never the
	// policy path — direct wins over relay exactly as before the gate.
	peer, refused = node.registerRelayedPeerLocked(relayLocalSession{
		sessionID: "sess-direct", viaPeerID: "via", remotePeerID: "direct-peer",
	})
	if peer != nil || refused {
		t.Fatalf("existing direct remote must take the early return: peer=%v refused=%v", peer, refused)
	}

	// The counter saw exactly the one genuine policy rejection.
	if v, _ := node.inboundByType.Load("__allowlist_rejected__"); v.(*atomic.Uint64).Load() != 1 {
		t.Fatal("expected exactly one counted allowlist rejection")
	}
}
