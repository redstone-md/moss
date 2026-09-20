package mesh

import (
	"net"
	"strconv"
	"testing"
	"time"
)

// masqTestConfig builds an isolated config with the uTLS masquerade on:
// every TCP leg (dial and listen) rides inside a Chrome-shaped TLS stream
func masqTestConfig(name string) Config {
	c := isolatedTestConfig(name)
	c.MasqConfig = MasqConfig{Enabled: true, CoverSNI: "www.example.com"}
	// Fast gossip heartbeats so the mesh grafts in test time, matching the
	// plain-TCP integration tests' settle.
	c.GossipSub.HeartbeatMS = 50
	return c
}

// startMasqNode brings a masq-masked node up and fails loudly on any Start
// error, so a wiring regression (e.g. the masquerade binding the port the
// UDP half needs) surfaces as a test failure, not a silently degraded mesh.
func startMasqNode(t *testing.T, meshID string, cfg Config) *Node {
	t.Helper()
	n, err := NewNode(meshID, nil, cfg)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	if code := n.Start(); code != MOSS_OK {
		t.Fatalf("Start: %d (%s)", code, n.LastError())
	}
	t.Cleanup(func() { n.Stop() })
	return n
}

// TestMasqNodesFormAMesh proves the wiring end to end: two nodes configured
// with Masq come up with the masquerade bound (no plain TCP listener), dial
// each other through transport.MasqDialer, complete the Noise handshake
func TestMasqNodesFormAMesh(t *testing.T) {
	// One substrate for both sides: the Noise handshake binds to NetworkID,
	// so two different names would refuse each other's sessions (which is
	// exactly what the transport's PSK/network binding is for).
	substrate := masqTestConfig("masq-mesh")
	a := startMasqNode(t, "room", substrate)
	b := startMasqNode(t, "room", substrate)

	// The masked ear is a masq listener, and no plain TCP listener exists.
	a.mu.RLock()
	if a.masqListener == nil {
		a.mu.RUnlock()
		t.Fatal("masq node has no masq listener")
	}
	if a.listener != nil {
		a.mu.RUnlock()
		t.Fatal("masq node opened a plain TCP listener — the masquerade must replace it")
	}
	if d := a.masqDialer.Load(); d == nil || d.CoverSNI != a.config.MasqConfig.CoverSNI {
		a.mu.RUnlock()
		t.Fatal("masq node has no dialer or its cover SNI is wrong")
	}
	a.mu.RUnlock()

	// Dial b through the masquerade and wait for both sides to converge.
	bAddr := b.advertisedListenAddr()
	ctx := a.rootCtx
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		_ = a.connectPeerWithHint(ctx, bAddr, b.localPeerID())
		if a.directPeerConnected(b.localPeerID()) && b.directPeerConnected(a.localPeerID()) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !a.directPeerConnected(b.localPeerID()) || !b.directPeerConnected(a.localPeerID()) {
		t.Fatalf("masq dial did not converge: a->b=%v b->a=%v", a.directPeerConnected(b.localPeerID()), b.directPeerConnected(a.localPeerID()))
	}
	// Gossip needs the session grafted before a publish has a target, the
	// same settle the plain-TCP integration tests wait for.
	waitForPeerCount(t, a, 1)

	// The accepting side must have labelled the session as masq inbound —
	// the one observable that distinguishes the masked ear from a plain one.
	b.mu.RLock()
	inbound := b.peerByID(a.localPeerID())
	origin := ""
	if inbound != nil {
		origin = inbound.origin
	}
	b.mu.RUnlock()
	if origin != originInboundMasq {
		t.Fatalf("accepted session origin = %q, want %q", origin, originInboundMasq)
	}
	got := make(chan string, 1)
	b.SetMessageCallback(func(channel string, senderID [32]byte, data []byte) {
		got <- string(data)
	})
	// BOTH sides subscribe — matching the plain-TCP publish tests: the
	// publisher's mesh grafts a subscriber on receiving its SUBSCRIBE
	// envelope (node_envelope.go), and without a's own subscription there
	// is no mesh for the flood publish to ride.
	if code := b.Subscribe("chan"); code != MOSS_OK {
		t.Fatalf("Subscribe: %d", code)
	}
	if code := a.Subscribe("chan"); code != MOSS_OK {
		t.Fatalf("Subscribe: %d", code)
	}
	// Settle for the SUBSCRIBE envelopes to cross and the heartbeat to
	// graft — the same 150ms the plain-TCP pubsub integration test gives.
	time.Sleep(150 * time.Millisecond)
	if code := a.Publish("chan", []byte("hello-masq")); code != MOSS_OK {
		t.Fatalf("Publish: %d", code)
	}
	select {
	case msg := <-got:
		if msg != "hello-masq" {
			t.Fatalf("published payload = %q", msg)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("publish over the masqueraded session never arrived")
	}
}

// TestMasqDisabledKeepsPlainTCP pins the opt-out: an EXPLICIT MasqConfig zero
// must produce exactly the pre-masq topology — ListenPair's plain TCP listener
// present, no masq listener, no masq dialer. Since DefaultConfig (and therefore
// every helper built on it) now masks by default, the disabled path is only
// reachable through the explicit opt-out this test sets — which is also the
// contract a plain-Noise deployment relies on.
func TestMasqDisabledKeepsPlainTCP(t *testing.T) {
	cfg := isolatedTestConfig("masq-off")
	cfg.MasqConfig = MasqConfig{} // explicit opt-out; the default is now ON
	n := startMasqNode(t, "room", cfg)
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.listener == nil {
		t.Fatal("plain node lost its plain TCP listener")
	}
	if n.masqListener != nil || n.masqDialer.Load() != nil {
		t.Fatal("masq bearer created although MasqConfig is disabled")
	}
}

// TestMasqStartFailureSurfacesLastError verifies a masq bind failure returns
// the coarse listen error with the OS reason in LastError, matching the
// plain-TCP failure contract of Start.
func TestMasqStartFailureSurfacesLastError(t *testing.T) {
	blocker := startMasqNode(t, "room", masqTestConfig("masq-occupied"))
	_, portStr, err := net.SplitHostPort(blocker.advertisedListenAddr())
	if err != nil {
		t.Fatalf("split blocker addr: %v", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("parse blocker port: %v", err)
	}
	cfg := masqTestConfig("masq-collide")
	cfg.ListenPort = port
	n, err := NewNode("room", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	if code := n.Start(); code != MOSS_ERR_LISTEN_FAILED {
		t.Fatalf("Start on an occupied port = %d, want %d", code, MOSS_ERR_LISTEN_FAILED)
	}
	if n.LastError() == "" {
		t.Fatal("Start failure left no LastError text")
	}
}
