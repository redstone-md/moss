package mesh

import (
	"net"
	"strconv"
	"testing"
	"time"
)

func startExplicitTargetNode(t *testing.T, name string) *Node {
	t.Helper()
	cfg := isolatedTestConfig(name)
	cfg.ListenPort = 0
	cfg.Security.HandshakeTimeoutSec = 2
	node, err := NewNode("mesh-explicit-target", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	if code := node.Start(); code != MOSS_OK {
		t.Fatalf("Start failed: %d", code)
	}
	t.Cleanup(func() { node.Stop() })
	return node
}

func injectKnownPeer(node *Node, peerID string, port int) {
	node.mu.Lock()
	node.knownPeers[peerID] = knownPeer{
		id:              peerID,
		addr:            net.JoinHostPort("127.0.0.1", strconv.Itoa(port)),
		verified:        true,
		publicReachable: true,
		lastSeen:        time.Now(),
	}
	node.mu.Unlock()
}

func waitForConnectedPeer(t *testing.T, node *Node, peerID string, dur time.Duration) {
	t.Helper()
	deadline := time.Now().Add(dur)
	for time.Now().Before(deadline) {
		node.mu.RLock()
		_, connected := node.peers[peerID]
		node.mu.RUnlock()
		if connected {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("node never connected to explicit target %s; info=%s", peerID, node.MeshInfoJSON())
}

func TestConnectToPeerValidatesInput(t *testing.T) {
	cfg := isolatedTestConfig("explicit-validate")
	node, err := NewNode("mesh-explicit-target", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	if code := node.ConnectToPeer("not-a-peer-id"); code != MOSS_ERR_CONFIG_INVALID {
		t.Fatalf("malformed id: got %d, want MOSS_ERR_CONFIG_INVALID", code)
	}
	if code := node.ConnectToPeer(node.localPeerID()); code != MOSS_ERR_CONFIG_INVALID {
		t.Fatalf("self id: got %d, want MOSS_ERR_CONFIG_INVALID", code)
	}
	otherID := node.localPeerID()
	flipped := []byte(otherID)
	if flipped[0] == 'a' {
		flipped[0] = 'b'
	} else {
		flipped[0] = 'a'
	}
	if code := node.ConnectToPeer(string(flipped)); code != MOSS_ERR_NOT_STARTED {
		t.Fatalf("valid id on stopped node: got %d, want MOSS_ERR_NOT_STARTED", code)
	}
}

// The glare rule forbids the higher-id public node from dialing a public peer
// (it waits for the inbound dial), and the discovery ranking may never select
// the counterpart at all. An explicit target must bypass both: whichever node
// calls ConnectToPeer connects, regardless of id ordering.
func TestConnectToPeerReachesKnownPeerDespiteGlareOrdering(t *testing.T) {
	a := startExplicitTargetNode(t, "explicit-target")
	b := startExplicitTargetNode(t, "explicit-target")

	caller, target := a, b
	if caller.localPeerID() < target.localPeerID() {
		caller, target = target, caller
	}
	injectKnownPeer(caller, target.localPeerID(), target.ListenPort())

	if code := caller.ConnectToPeer(target.localPeerID()); code != MOSS_OK {
		t.Fatalf("ConnectToPeer failed: %d", code)
	}
	waitForConnectedPeer(t, caller, target.localPeerID(), 8*time.Second)
}

// A target registered before its announce is known must be retried by the
// maintenance loop and connect once the peer appears in knownPeers.
func TestConnectToPeerRetriesUntilPeerBecomesKnown(t *testing.T) {
	a := startExplicitTargetNode(t, "explicit-retry")
	b := startExplicitTargetNode(t, "explicit-retry")

	if code := a.ConnectToPeer(b.localPeerID()); code != MOSS_OK {
		t.Fatalf("ConnectToPeer failed: %d", code)
	}
	time.Sleep(300 * time.Millisecond)
	injectKnownPeer(a, b.localPeerID(), b.ListenPort())
	waitForConnectedPeer(t, a, b.localPeerID(), 12*time.Second)
}
