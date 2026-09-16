package mesh

import (
	"testing"
)

// net.Dialer.DialContext panics with "nil context" before it can refuse the
// call itself (src/net/dial.go: dial.go:526). Every internal dial path leads
// to connectPeerOnce — a panic there, in a goroutine kicked off by
// kickBootstrapPeers or the maintenance loop, ends the process. The dial
// path's contract is errors, never panics: these tests pin the nil-context
// refusal at each entry point that spawns dials off caller-supplied
// contexts, so a caller that hands the mesh a nil context gets an error it
// can log, not a dead node.
func TestDialPathRefusesNilContext(t *testing.T) {
	node, err := NewNode("mesh-dial-nil-ctx", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	defer node.Stop()

	// The leaf itself.
	if err := node.connectPeerOnce(nil, "127.0.0.1:1", nil); err == nil {
		t.Fatal("connectPeerOnce accepted a nil context")
	}

	// The hint chain down into the leaf.
	if err := node.connectPeerWithHint(nil, "127.0.0.1:1", ""); err == nil {
		t.Fatal("connectPeerWithHint accepted a nil context")
	}

	// The parallel TCP/UDP bootstrap dial: a panic in either spawned half
	// takes the whole node down, so the refusal must happen before the
	// goroutines launch.
	if err := node.connectBootstrapPeer(nil, "127.0.0.1:1"); err == nil {
		t.Fatal("connectBootstrapPeer accepted a nil context")
	}
}
