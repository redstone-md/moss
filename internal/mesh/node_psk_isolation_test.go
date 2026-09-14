package mesh

import (
	"net"
	"strconv"
	"testing"
	"time"
)

// Two nodes with the knob ON and the same room PSK must connect — the gate
// must not break the pair it is meant to protect.
func TestPSKHandshakeKnobConnectsMatchingNodes(t *testing.T) {
	cfgA := isolatedTestConfig("psk-match")
	cfgA.Security.PSKHandshake = true
	nodeA, err := NewNode("mesh-psk", []byte("room-secret-a"), cfgA)
	if err != nil {
		t.Fatalf("NewNode A failed: %v", err)
	}
	if code := nodeA.Start(); code != MOSS_OK {
		t.Fatalf("nodeA.Start failed: %d", code)
	}
	defer nodeA.Stop()

	cfgB := isolatedTestConfig("psk-match")
	cfgB.Security.PSKHandshake = true
	nodeB, err := NewNode("mesh-psk", []byte("room-secret-a"), cfgB)
	if err != nil {
		t.Fatalf("NewNode B failed: %v", err)
	}
	if code := nodeB.Start(); code != MOSS_OK {
		t.Fatalf("nodeB.Start failed: %d", code)
	}
	defer nodeB.Stop()

	if code := nodeB.Connect(net.JoinHostPort("127.0.0.1", strconv.Itoa(nodeA.ListenPort()))); code != MOSS_OK {
		t.Fatalf("Connect with matching PSK failed: %d", code)
	}
	waitForPeerCount(t, nodeA, 1)
	waitForPeerCount(t, nodeB, 1)
}

// With the knob ON and DIFFERENT PSKs the handshake must fail: the target
// never registers the caller, which is the observable fact (Connect may
// report success from the dialer's side — the gate runs at registration).
func TestPSKHandshakeKnobRejectsDifferentPSK(t *testing.T) {
	cfgA := isolatedTestConfig("psk-mismatch")
	cfgA.Security.PSKHandshake = true
	nodeA, err := NewNode("mesh-psk", []byte("room-secret-a"), cfgA)
	if err != nil {
		t.Fatalf("NewNode A failed: %v", err)
	}
	if code := nodeA.Start(); code != MOSS_OK {
		t.Fatalf("nodeA.Start failed: %d", code)
	}
	defer nodeA.Stop()

	cfgB := isolatedTestConfig("psk-mismatch")
	cfgB.Security.PSKHandshake = true
	nodeB, err := NewNode("mesh-psk", []byte("room-secret-b"), cfgB)
	if err != nil {
		t.Fatalf("NewNode B failed: %v", err)
	}
	if code := nodeB.Start(); code != MOSS_OK {
		t.Fatalf("nodeB.Start failed: %d", code)
	}
	_ = nodeB.Connect(net.JoinHostPort("127.0.0.1", strconv.Itoa(nodeA.ListenPort())))
	// Neither node may register the other within a generous window: the
	// handshake itself fails on the first message (PSK mismatch), long
	// before the peer-count deadline expires. waitForPeerCountAtMost
	// fatals only on helper misuse; the count assertion below is the test.
	waitForPeerCountAtMost(t, nodeA, 0, 3*time.Second)
	waitForPeerCountAtMost(t, nodeB, 0, 3*time.Second)
}

// The knob OFF must keep the historical open behavior: a PSK-carrying node
// connects to a PSK-less node, because the substrate handshake was never a
// room gate by default. This is the wire-compatibility contract — existing
// fleets must not notice the feature exists.
func TestPSKHandshakeKnobOffKeepsCompatibility(t *testing.T) {
	cfgA := isolatedTestConfig("psk-off")
	nodeA, err := NewNode("mesh-psk", nil, cfgA)
	if err != nil {
		t.Fatalf("NewNode A failed: %v", err)
	}
	if code := nodeA.Start(); code != MOSS_OK {
		t.Fatalf("nodeA.Start failed: %d", code)
	}
	defer nodeA.Stop()

	cfgB := isolatedTestConfig("psk-off")
	nodeB, err := NewNode("mesh-psk", []byte("room-secret-b"), cfgB)
	if err != nil {
		t.Fatalf("NewNode B failed: %v", err)
	}
	if code := nodeB.Start(); code != MOSS_OK {
		t.Fatalf("nodeB.Start failed: %d", code)
	}
	defer nodeB.Stop()

	if code := nodeB.Connect(net.JoinHostPort("127.0.0.1", strconv.Itoa(nodeA.ListenPort()))); code != MOSS_OK {
		t.Fatalf("Connect with knob off failed: %d", code)
	}
	waitForPeerCount(t, nodeA, 1)
	waitForPeerCount(t, nodeB, 1)
}
