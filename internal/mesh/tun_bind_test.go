package mesh

import (
	"encoding/json"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/transport"
	"github.com/redstone-md/moss/internal/tun"
)

// The intranet binding's end-to-end contract: a packet injected into an
// attached interface is routed over the node's directed-packet transport
// (TypeDirect envelope), and a directed payload arriving from a peer is
// demultiplexed by the chained packet callback into the intranet and
// recorded as an interface write. The node is deliberately UNSTARTED where
// possible: SendToPeer's direct path is then synchronous, so a forwarded
// packet is observable the moment the pump runs — no dispatch-loop race.

// tunSpy is the PacketIface the tests attach: outbound packets (what the
// pump reads) flow through an inner loopback the test injects into, while
// inbound deliveries (what the router writes) are recorded on the spy —
// never on the inner loopback, where the pump would race the test for them
// and bounce the packet back out.
type tunSpy struct {
	inner   *tun.Loopback
	mu      sync.Mutex
	inbound [][]byte
}

func (s *tunSpy) Inject(packet []byte) error { return s.inner.WritePacket(packet) }

func (s *tunSpy) ReadPacket() ([]byte, error) { return s.inner.ReadPacket() }

func (s *tunSpy) WritePacket(packet []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inbound = append(s.inbound, append([]byte(nil), packet...))
	return nil
}

func (s *tunSpy) Close() error { return s.inner.Close() }

// Inbound returns a snapshot of the router's interface writes.
func (s *tunSpy) Inbound() [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([][]byte, len(s.inbound))
	copy(out, s.inbound)
	return out
}

// tunWaitFor polls cond until true or the deadline, failing the test. cond
// must never block.
func tunWaitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// tunIP4Packet builds a minimal IPv4 packet addressed to dst.
func tunIP4Packet(dst string, size int) []byte {
	addr := net.ParseIP(dst).To4()
	p := make([]byte, size)
	p[0] = 0x45
	p[2] = byte(size >> 8)
	p[3] = byte(size)
	copy(p[16:20], addr)
	return p
}

// tunAttachedNode builds an unstarted node with an attached spy intranet
// over 10.66.0.0/24, plus cleanup that detaches (which closes the spy).
func tunAttachedNode(t *testing.T, name string) (*Node, *tunSpy, *tunBinding) {
	t.Helper()
	node, err := NewNode("mesh-tun-"+name, nil, isolatedTestConfig("tun-"+name))
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	t.Cleanup(func() { _ = node.Stop() })
	spy := &tunSpy{inner: tun.NewLoopback()}
	if err := AttachTun(node, spy, "10.66.0.0/24"); err != nil {
		t.Fatalf("AttachTun: %v", err)
	}
	t.Cleanup(func() { _ = DetachTun(node) })
	v, _ := tunBinds.Load(node)
	b := v.(*tunBinding)
	return node, spy, b
}

// tunInjectPeer gives the node a direct session over a capturing carrier
// and returns the carrier — the far end of the intranet.
func tunInjectPeer(t *testing.T, node *Node, peerID string) *capturingCarrier {
	t.Helper()
	carrier := newCapturingCarrier()
	t.Cleanup(func() { _ = carrier.Close() })
	sess := mustCipherSession(carrier)
	t.Cleanup(func() { _ = sess.Close() })
	node.mu.Lock()
	node.peers[peerID] = &peerConn{id: peerID, session: sess, outbound: true, connectedAt: time.Now()}
	node.mu.Unlock()
	return carrier
}

// TestTunLoopbackEndToEnd walks the full data plane: injected packet →
// the binding's pump → SendToPeer (TypeDirect over the peer's session) →
// the wire (captured as ciphertext, decrypted through a cipher-matched far
// session) → the far peer's chained packet callback with the sender's ID →
// its router → its interface, arriving byte-identical.
func TestTunLoopbackEndToEnd(t *testing.T) {
	node, spy, b := tunAttachedNode(t, "e2e")
	carrier := tunInjectPeer(t, node, "peer-far")

	// The far peer's virtual IP is assigned on first use.
	farAddr, err := PeerAddr(node, "peer-far")
	if err != nil {
		t.Fatalf("PeerAddr: %v", err)
	}
	if !farAddr.Equal(net.ParseIP("10.66.0.1")) {
		t.Fatalf("first virtual IP should be 10.66.0.1, got %s", farAddr)
	}

	// One packet into the intranet, addressed to the far peer's virtual IP.
	packet := tunIP4Packet("10.66.0.1", 64)
	if err := spy.Inject(packet); err != nil {
		t.Fatalf("inject: %v", err)
	}

	// The pump routes it out over the direct session: exactly one carrier
	// write whose plaintext is a TypeDirect envelope carrying the packet.
	tunWaitFor(t, "the router to forward the packet", func() bool {
		return b.router.Counters().Forwarded.Load() == 1
	})
	if got := directCaptureCount(carrier); got != 1 {
		t.Fatalf("expected exactly one carrier write, got %d", got)
	}
	farEnd := newCapturingCarrier()
	t.Cleanup(func() { _ = farEnd.Close() })
	farSess := cipherMatchedSession(t, farEnd)
	farEnd.reads <- carrier.lastWrite()
	plain, err := farSess.ReadPacket()
	if err != nil {
		t.Fatalf("far-end read failed: %v", err)
	}
	var env gossip.Envelope
	if err := json.Unmarshal(plain, &env); err != nil {
		t.Fatalf("far-end plaintext is not an envelope: %v", err)
	}
	if env.Type != gossip.TypeDirect {
		t.Fatalf("expected a %s envelope on the wire, got %s", gossip.TypeDirect, env.Type)
	}
	if string(env.Payload) != string(packet) {
		t.Fatalf("payload mismatch: %x != %x", env.Payload, packet)
	}

	// Now the far peer's side: its chained packet callback receives the
	// envelope's sender ID with the packet and must classify it as IP,
	// route it through the router, and deliver it onto its interface
	// byte-identical. (Driving the far node's callback directly models the
	// dispatch loop's dispatchPacket case without the loop's timing.)
	var sender [32]byte
	copy(sender[:], node.identity.PublicKeyBytes())
	node.mu.RLock()
	cb := node.packetCB
	node.mu.RUnlock()
	if cb == nil {
		t.Fatal("chained packet callback missing")
	}
	back := tunIP4Packet("10.66.0.2", 48)
	cb(sender, back)
	tunWaitFor(t, "the inbound packet on the interface", func() bool {
		got := spy.Inbound()
		return len(got) == 1 && string(got[0]) == string(back)
	})
	if b.router.Counters().InboundDelivered.Load() != 1 {
		t.Fatalf("InboundDelivered = %d, want 1", b.router.Counters().InboundDelivered.Load())
	}
}

// TestTunUnknownDstCountedDrop pins the outbound miss path: a packet whose
// destination virtual IP belongs to no peer is dropped and counted, with
// nothing on the wire.
func TestTunUnknownDstCountedDrop(t *testing.T) {
	node, spy, b := tunAttachedNode(t, "unknown-dst")
	carrier := tunInjectPeer(t, node, "peer-far")
	_ = node

	if err := spy.Inject(tunIP4Packet("10.66.0.77", 40)); err != nil {
		t.Fatalf("inject: %v", err)
	}
	tunWaitFor(t, "the unknown-dst drop to be counted", func() bool {
		return b.router.Counters().DroppedUnknownDst.Load() == 1
	})
	if got := b.router.Counters().Forwarded.Load(); got != 0 {
		t.Fatalf("nothing may be forwarded for an unknown dst, got %d", got)
	}
	if got := directCaptureCount(carrier); got != 0 {
		t.Fatalf("unknown dst must not reach the wire, got %d writes", got)
	}
}

// TestTunDetachStopsGoroutine pins the detach lifecycle: the pump is
// proven alive (it processes one injected packet), then DetachTun stops
// the binding's goroutines (done closes within the bound), restores the
// pre-attach callback, and leaves the node re-attachable.
func TestTunDetachStopsGoroutine(t *testing.T) {
	node, spy, b := tunAttachedNode(t, "detach")

	// The pump is alive: one injected packet is processed (dropped as an
	// unknown destination — the observable proof the loop runs).
	if err := spy.Inject(tunIP4Packet("10.66.0.9", 40)); err != nil {
		t.Fatalf("inject: %v", err)
	}
	tunWaitFor(t, "the pump to process one packet", func() bool {
		return b.router.Counters().DroppedUnknownDst.Load() == 1
	})

	if err := DetachTun(node); err != nil {
		t.Fatalf("DetachTun: %v", err)
	}
	select {
	case <-b.done:
	case <-time.After(5 * time.Second):
		t.Fatal("binding goroutine did not stop after DetachTun")
	}
	// The callback is restored to the pre-attach value (nil here).
	node.mu.RLock()
	cb := node.packetCB
	node.mu.RUnlock()
	if cb != nil {
		t.Fatal("DetachTun must restore the pre-attach packet callback")
	}
	// Second detach is an error, not a panic.
	if err := DetachTun(node); err == nil {
		t.Fatal("double DetachTun should fail")
	}
	// Registry is clean: PeerAddr fails, and a fresh attach succeeds.
	if _, err := PeerAddr(node, "peer-x"); err == nil {
		t.Fatal("PeerAddr on a detached node should fail")
	}
	spy2 := &tunSpy{inner: tun.NewLoopback()}
	if err := AttachTun(node, spy2, "10.66.1.0/24"); err != nil {
		t.Fatalf("re-attach after detach: %v", err)
	}
}

// TestTunStopTerminatesBinding pins the Stop half of the lifecycle: on a
// STARTED node, node.Stop() alone must terminate the binding goroutine —
// the binding captured the node's root context at attach time.
func TestTunStopTerminatesBinding(t *testing.T) {
	if !transport.RunningGoTest() {
		t.Fatal("test build flag not set: the node would dial real discovery")
	}
	node, err := NewNode("mesh-tun-stop", nil, isolatedTestConfig("tun-stop"))
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	if code := node.Start(); code != MOSS_OK {
		t.Fatalf("Start: %d", code)
	}
	spy := &tunSpy{inner: tun.NewLoopback()}
	if err := AttachTun(node, spy, "10.66.0.0/24"); err != nil {
		t.Fatalf("AttachTun: %v", err)
	}
	v, _ := tunBinds.Load(node)
	b := v.(*tunBinding)

	// Prove the loop is alive before killing it: one forwarded packet.
	carrier := tunInjectPeer(t, node, "peer-stop")
	if _, err := PeerAddr(node, "peer-stop"); err != nil {
		t.Fatalf("PeerAddr: %v", err)
	}
	if err := spy.Inject(tunIP4Packet("10.66.0.1", 40)); err != nil {
		t.Fatalf("inject: %v", err)
	}
	tunWaitFor(t, "the binding to forward one packet", func() bool {
		return b.router.Counters().Forwarded.Load() == 1
	})

	// Stop alone: the captured root context must terminate the binding.
	node.Stop()
	select {
	case <-b.done:
	case <-time.After(5 * time.Second):
		t.Fatal("binding goroutine did not stop on node.Stop()")
	}
	// DetachTun after Stop is a clean reap, not an error path that hangs.
	if err := DetachTun(node); err != nil {
		t.Fatalf("DetachTun after Stop should reap cleanly: %v", err)
	}
	_ = carrier
}

// TestTunCallbackChaining pins the demultiplexer: an application callback
// registered BEFORE AttachTun keeps receiving non-IP payloads; IP packets
// route into the intranet instead; DetachTun restores the original.
func TestTunCallbackChaining(t *testing.T) {
	node, err := NewNode("mesh-tun-chain", nil, isolatedTestConfig("tun-chain"))
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	t.Cleanup(func() { _ = node.Stop() })

	received := make(chan []byte, 4)
	node.SetPacketCallback(func(senderID [32]byte, data []byte) {
		received <- append([]byte(nil), data...)
	})

	spy := &tunSpy{inner: tun.NewLoopback()}
	if err := AttachTun(node, spy, "10.66.0.0/24"); err != nil {
		t.Fatalf("AttachTun: %v", err)
	}
	t.Cleanup(func() { _ = DetachTun(node) })

	// Non-IP payload still reaches the application callback verbatim.
	node.mu.RLock()
	cb := node.packetCB
	node.mu.RUnlock()
	if cb == nil {
		t.Fatal("chained packet callback missing")
	}
	cb([32]byte{1}, []byte("hello"))
	select {
	case got := <-received:
		if string(got) != "hello" {
			t.Fatalf("app callback payload mismatch: %q", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("non-IP payload never reached the pre-registered app callback")
	}

	// IP payload goes to the intranet, never to the app callback.
	ip := tunIP4Packet("10.66.0.1", 40)
	cb([32]byte{1}, ip)
	select {
	case <-received:
		t.Fatal("IP payload leaked to the application callback")
	case <-time.After(200 * time.Millisecond):
	}
	v, _ := tunBinds.Load(node)
	b := v.(*tunBinding)
	if got := b.router.Counters().InboundDelivered.Load(); got != 1 {
		t.Fatalf("IP payload not delivered into the intranet: %d", got)
	}
	tunWaitFor(t, "the IP packet on the interface", func() bool {
		got := spy.Inbound()
		return len(got) == 1 && string(got[0]) == string(ip)
	})

	// Detach restores the original callback.
	if err := DetachTun(node); err != nil {
		t.Fatalf("DetachTun: %v", err)
	}
	node.mu.RLock()
	cb = node.packetCB
	node.mu.RUnlock()
	if cb == nil {
		t.Fatal("original app callback not restored")
	}
	cb([32]byte{1}, []byte("after-detach"))
	select {
	case got := <-received:
		if string(got) != "after-detach" {
			t.Fatalf("restored callback payload mismatch: %q", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("restored callback not invoked after detach")
	}
}

// TestTunAttachValidation pins the argument gates: nil node, nil interface,
// bad CIDR, a double attach, and double detach.
func TestTunAttachValidation(t *testing.T) {
	if err := AttachTun(nil, tun.NewLoopback(), "10.66.0.0/24"); err == nil {
		t.Fatal("nil node must fail")
	}
	node, err := NewNode("mesh-tun-validate", nil, isolatedTestConfig("tun-validate"))
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	t.Cleanup(func() { _ = node.Stop() })
	if err := AttachTun(node, nil, "10.66.0.0/24"); err == nil {
		t.Fatal("nil interface must fail")
	}
	if err := AttachTun(node, tun.NewLoopback(), "bogus"); err == nil {
		t.Fatal("bad CIDR must fail")
	}
	spy := &tunSpy{inner: tun.NewLoopback()}
	if err := AttachTun(node, spy, "10.66.0.0/24"); err != nil {
		t.Fatalf("attach: %v", err)
	}
	spy2 := &tunSpy{inner: tun.NewLoopback()}
	if err := AttachTun(node, spy2, "10.66.0.0/24"); err == nil {
		t.Fatal("double attach must fail")
	}
	if err := DetachTun(node); err != nil {
		t.Fatalf("detach: %v", err)
	}
	if err := DetachTun(node); err == nil {
		t.Fatal("detach with nothing attached must fail")
	}
	if _, err := PeerAddr(nil, "x"); err == nil {
		t.Fatal("PeerAddr on nil node must fail")
	}
}

// TestTunFragmentationEndToEnd walks the v2 fragmentation loop end to end:
// a 4400-byte packet (over the 1500 MTU) is injected, forwarded as THREE
// TypeDirect frames on the wire, decrypted at the far end, and reassembled
// out of order through the chained callback into one byte-identical
// interface write.
func TestTunFragmentationEndToEnd(t *testing.T) {
	node, spy, b := tunAttachedNode(t, "frag-e2e")
	carrier := tunInjectPeer(t, node, "peer-far")
	if _, err := PeerAddr(node, "peer-far"); err != nil {
		t.Fatalf("PeerAddr: %v", err)
	}

	packet := tunIP4Packet("10.66.0.1", 4400)
	if err := spy.Inject(packet); err != nil {
		t.Fatalf("inject: %v", err)
	}
	tunWaitFor(t, "the fragmented packet to be forwarded", func() bool {
		return b.router.Counters().Forwarded.Load() == 1
	})
	if got := b.router.Counters().FragSent.Load(); got != 3 {
		t.Fatalf("FragSent = %d, want 3", got)
	}
	if got := directCaptureCount(carrier); got != 3 {
		t.Fatalf("expected 3 carrier writes, got %d", got)
	}
	if got := b.router.Counters().DroppedOversize.Load(); got != 0 {
		t.Fatalf("a fragmented packet must not count as oversize, got %d", got)
	}

	// Decrypt all three frames through ONE far session (nonce lockstep)
	// and assert their headers: same fragID, indices 0-2, total 3.
	carrier.mu.Lock()
	frames := append([][]byte(nil), carrier.capture...)
	carrier.mu.Unlock()
	if len(frames) != 3 {
		t.Fatalf("captured %d frames, want 3", len(frames))
	}
	farEnd := newCapturingCarrier()
	t.Cleanup(func() { _ = farEnd.Close() })
	farSess := cipherMatchedSession(t, farEnd)
	var plains [][]byte
	for _, ct := range frames {
		farEnd.reads <- ct
		plain, err := farSess.ReadPacket()
		if err != nil {
			t.Fatalf("far-end read failed: %v", err)
		}
		var env gossip.Envelope
		if err := json.Unmarshal(plain, &env); err != nil {
			t.Fatalf("frame is not an envelope: %v", err)
		}
		if env.Type != gossip.TypeDirect {
			t.Fatalf("expected %s, got %s", gossip.TypeDirect, env.Type)
		}
		if !tun.IsFragFrame(env.Payload) {
			t.Fatalf("payload is not a TFRG frame: %x", env.Payload[:4])
		}
		plains = append(plains, env.Payload)
	}

	// Feed the frames back out of order (2, 0, 1): the reassembly must
	// produce exactly one byte-identical interface write.
	node.mu.RLock()
	cb := node.packetCB
	node.mu.RUnlock()
	for _, idx := range []int{2, 0, 1} {
		cb([32]byte{}, plains[idx])
	}
	tunWaitFor(t, "the reassembled packet on the interface", func() bool {
		got := spy.Inbound()
		return len(got) == 1 && string(got[0]) == string(packet)
	})
	if got := b.router.Counters().FragReassembled.Load(); got != 1 {
		t.Fatalf("FragReassembled = %d, want 1", got)
	}
	if got := b.router.Counters().InboundDelivered.Load(); got != 1 {
		t.Fatalf("InboundDelivered = %d, want 1", got)
	}
}

// TestTunRouteRTTSelection pins the beyond-the-pool routing: a destination
// inside a registered prefix routes to the lowest-RTT candidate, over the
// node's real directed transport.
func TestTunRouteRTTSelection(t *testing.T) {
	node, spy, b := tunAttachedNode(t, "route-rtt")
	carrierA := tunInjectPeer(t, node, "peer-a")
	carrierB := tunInjectPeer(t, node, "peer-b")

	if err := AddTunRoute(node, "192.168.50.0/24", "peer-a"); err != nil {
		t.Fatalf("AddTunRoute a: %v", err)
	}
	if err := AddTunRoute(node, "192.168.50.0/24", "peer-b"); err != nil {
		t.Fatalf("AddTunRoute b: %v", err)
	}
	node.mu.Lock()
	node.peers["peer-a"].lastRTT = 200 * time.Millisecond
	node.peers["peer-b"].lastRTT = 20 * time.Millisecond
	node.mu.Unlock()

	if err := spy.Inject(tunIP4Packet("192.168.50.1", 40)); err != nil {
		t.Fatalf("inject: %v", err)
	}
	tunWaitFor(t, "the routed packet to be forwarded", func() bool {
		return b.router.Counters().Forwarded.Load() == 1
	})
	if got := directCaptureCount(carrierB); got != 1 {
		t.Fatalf("lowest-RTT peer-b should receive the packet, got %d writes", got)
	}
	if got := directCaptureCount(carrierA); got != 0 {
		t.Fatalf("higher-RTT peer-a must not receive the packet, got %d writes", got)
	}
}

// TestTunRouteCacheInvalidationOnPeerDeath pins the cache liveness check:
// a cached choice whose peer left the mesh re-decides to the backup on the
// very next packet — no 10-second wait.
func TestTunRouteCacheInvalidationOnPeerDeath(t *testing.T) {
	node, spy, b := tunAttachedNode(t, "route-invalidate")
	carrierA := tunInjectPeer(t, node, "peer-a")
	carrierB := tunInjectPeer(t, node, "peer-b")

	_ = AddTunRoute(node, "192.168.50.0/24", "peer-a")
	_ = AddTunRoute(node, "192.168.50.0/24", "peer-b")
	node.mu.Lock()
	node.peers["peer-a"].lastRTT = 200 * time.Millisecond
	node.peers["peer-b"].lastRTT = 20 * time.Millisecond
	node.mu.Unlock()

	// First packet: peer-b wins and the choice is cached.
	if err := spy.Inject(tunIP4Packet("192.168.50.1", 40)); err != nil {
		t.Fatalf("inject 1: %v", err)
	}
	tunWaitFor(t, "the first routed packet", func() bool {
		return b.router.Counters().Forwarded.Load() == 1
	})
	if got := directCaptureCount(carrierB); got != 1 {
		t.Fatalf("first packet should reach peer-b, got %d writes", got)
	}

	// peer-b dies; the cached choice must be re-decided immediately.
	node.mu.Lock()
	delete(node.peers, "peer-b")
	node.mu.Unlock()
	if err := spy.Inject(tunIP4Packet("192.168.50.2", 40)); err != nil {
		t.Fatalf("inject 2: %v", err)
	}
	tunWaitFor(t, "the second routed packet", func() bool {
		return b.router.Counters().Forwarded.Load() == 2
	})
	if got := directCaptureCount(carrierA); got != 1 {
		t.Fatalf("peer-b's death must re-decide to peer-a, got %d writes on A", got)
	}
	if got := directCaptureCount(carrierB); got != 1 {
		t.Fatalf("peer-b is gone; its carrier must see no new writes, got %d", got)
	}
}
