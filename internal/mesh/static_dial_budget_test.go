package mesh

// The static dial at Start used to run on the root context — an unbounded
// handshake budget. A static peer that is down at that moment held the
// transport's client slot for its address forever: every later seed retry to
// the same address got "udp handshake is already in progress" and never even
// sent a packet, so a temporarily dead static peer could never recover, and
// with the UDP fallback it also starved the address for every other dialer.
// The initial dial needs the same bounded budget the kick and seed paths use.

import (
	"context"
	"testing"
	"time"
)

func TestStaticDialHasBoundedBudget(t *testing.T) {
	cfg := isolatedTestConfig("static-budget")
	cfg.Security.HandshakeTimeoutSec = 1
	cfg.StaticPeers = []string{"127.0.0.1:1"} // dead port: nothing answers
	node, err := NewNode("mesh-udp-confirm", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	if code := node.Start(); code != MOSS_OK {
		t.Fatalf("Start failed: %d", code)
	}
	t.Cleanup(func() { _ = node.Stop() })

	// Wait past the dial's budget, then poll: the seed pass may itself be
	// mid-handshake with the same dead address, and THAT is a bounded,
	// healthy in-flight marker — the bug is a slot that NEVER frees. The
	// check passes as soon as one dial attempt actually starts.
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(250 * time.Millisecond)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := node.connectPeerUDPWithHint(ctx, "", "127.0.0.1:1")
		cancel()
		if err == nil {
			t.Fatal("dial to a dead port succeeded; test setup is wrong")
		}
		if err.Error() != "udp handshake is already in progress" {
			return // the slot freed: a fresh handshake actually started
		}
	}
	t.Fatal("the initial static dial still holds the client slot after its budget: every later retry for this address starves forever")
}
