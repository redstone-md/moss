package meshbridge

import (
	"bytes"
	"encoding/binary"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/mesh"
	"github.com/redstone-md/moss/internal/tun"
)

// e2eNewNode builds a started, isolated node in the bridge room with
// the given PSK — the TestPrivateRoomPSKIsolation node shape (nil
// trackers, no LAN discovery, fast heartbeat), NOT newBridgeNode,
// which builds a no-PSK node.
func e2eNewNode(t *testing.T, psk []byte) *mesh.Node {
	t.Helper()
	cfg := mesh.DefaultConfig()
	cfg.Trackers = nil
	cfg.LANDiscoveryEnabled = false
	cfg.GossipSub.HeartbeatMS = 50
	node, err := mesh.NewNode("bridge-room", psk, cfg)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	if code := node.Start(); code != mesh.MOSS_OK {
		t.Fatalf("node.Start: error code %d", code)
	}
	t.Cleanup(func() { node.Stop() })
	return node
}

// e2eIP4Packet builds a plausible-looking IPv4 packet of the given
// size: version/IHL 0x45, a total-length field that matches, and a
// recognizable payload pattern. Nothing on the bridge path parses it
// — the byte pattern is only for identity checks.
func e2eIP4Packet(size int) []byte {
	pkt := make([]byte, size)
	pkt[0] = 0x45
	binary.BigEndian.PutUint16(pkt[2:4], uint16(size))
	for i := 20; i < size; i++ {
		pkt[i] = byte(i)
	}
	return pkt
}

// e2eWaitFor polls cond every 10ms until it holds or the 5s deadline
// lapses, then fails the test — the tunWaitFor shape, local to these
// tests so pump_test's helpers stay untouched.
func e2eWaitFor(t *testing.T, what string, cond func() bool) {
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

// TestTunBridgeE2ERoomDelivery walks the whole sealed path: TUN packet
// → glue seal → PublishBridge → FakeLink (ciphertext frames) → far
// pump reassembles → far node Publishes on the room topic → the
// receiving host's mesh delivers through the mesh's own room seal →
// glue open → receiving iface, byte-identical. The wire assertion is
// the acceptance proof: what the Link carries is NOT the plaintext.
//
// Topology: nodeB (near gateway, pump + glue egress, no mesh peers)
// and nodeB2 (far gateway pump on the same FakeLink, mesh-connected
// to nodeR); nodeR (receiving host, pump-less glue, subscribed to
// DefaultTopic); nodeE (eavesdropper with the WRONG PSK, connected on
// the shared substrate — proof the wrong-PSK node receives nothing).
func TestTunBridgeE2ERoomDelivery(t *testing.T) {
	psk := []byte("e2e-room-secret")
	wrongPSK := []byte("e2e-wrong-secret")

	nodeB := e2eNewNode(t, psk)
	nodeB2 := e2eNewNode(t, psk)
	nodeR := e2eNewNode(t, psk)
	nodeE := e2eNewNode(t, wrongPSK)

	link := NewFakeLink()
	pumpB, err := AttachPump(nodeB, link, nil, nil)
	if err != nil {
		t.Fatalf("AttachPump B: %v", err)
	}
	defer pumpB.Detach()
	pumpB2, err := AttachPump(nodeB2, link, nil, nil)
	if err != nil {
		t.Fatalf("AttachPump B2: %v", err)
	}
	defer pumpB2.Detach()

	ifaceB := tun.NewLoopback()
	glueB, err := AttachTunBridge(nodeB, pumpB, ifaceB, "bridge-room", psk, nil)
	if err != nil {
		t.Fatalf("AttachTunBridge B: %v", err)
	}
	defer glueB.Detach()

	ifaceR := tun.NewLoopback()
	glueR, err := AttachTunBridge(nodeR, nil, ifaceR, "bridge-room", psk, nil)
	if err != nil {
		t.Fatalf("AttachTunBridge R: %v", err)
	}
	defer glueR.Detach()

	// Mesh substrate: R and E both connect to the far gateway's node.
	// The near gateway B is deliberately mesh-isolated — its only leg
	// out is the Link.
	if code := nodeR.Connect("127.0.0.1:" + strconv.Itoa(nodeB2.ListenPort())); code != mesh.MOSS_OK {
		t.Fatalf("nodeR.Connect: error code %d", code)
	}
	if code := nodeE.Connect("127.0.0.1:" + strconv.Itoa(nodeB2.ListenPort())); code != mesh.MOSS_OK {
		t.Fatalf("nodeE.Connect: error code %d", code)
	}
	waitConnected(t, nodeR, nodeB2)
	waitConnected(t, nodeE, nodeB2)

	// Eavesdropper listens on the same bare channel name; its topic is
	// an HMAC under the WRONG room key, so nothing can ever arrive.
	eavesdropped := make(chan []byte, 4)
	nodeE.SetMessageCallback(func(channel string, _ [32]byte, data []byte) {
		if channel == DefaultTopic {
			eavesdropped <- append([]byte(nil), data...)
		}
	})
	nodeE.Subscribe(DefaultTopic)
	nodeR.Subscribe(DefaultTopic)

	// Capture EVERY frame the Link carries — the wire proof. FakeLink
	// stacks handlers, so this rides alongside both pumps' ingresses.
	var wireFrames atomic.Int64
	firstFrame := make(chan []byte, 8)
	link.Subscribe(DefaultTopic, func(topic string, payload []byte) {
		wireFrames.Add(1)
		select {
		case firstFrame <- append([]byte(nil), payload...):
		default:
		}
	})

	// Let the subscription grafts settle before injecting.
	time.Sleep(1 * time.Second)

	pkt := e2eIP4Packet(64)
	if err := ifaceB.WritePacket(pkt); err != nil {
		t.Fatalf("ifaceB.WritePacket: %v", err)
	}

	got := make(chan []byte, 1)
	go func() {
		p, err := ifaceR.ReadPacket()
		if err != nil {
			return
		}
		got <- p
	}()
	select {
	case p := <-got:
		if !bytes.Equal(p, pkt) {
			t.Fatalf("received packet differs: got %d bytes, want %d", len(p), len(pkt))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the sealed packet to arrive at the receiving iface")
	}

	// The wire never carried the plaintext.
	select {
	case frame := <-firstFrame:
		f, err := Decode(frame)
		if err != nil {
			t.Fatalf("Decode wire frame: %v", err)
		}
		if bytes.Equal(f.Payload, pkt) {
			t.Fatal("ACCEPTANCE FAILURE: Link wire frame payload equals the plaintext packet")
		}
		if len(f.Payload) != len(pkt)+28 {
			t.Fatalf("sealed frame payload len %d, want %d (packet %d + 28 AEAD overhead)", len(f.Payload), len(pkt)+28, len(pkt))
		}
		if bytes.Equal(f.Payload[:12], pkt[:12]) {
			t.Fatal("frame payload begins with the plaintext packet bytes — not sealed")
		}
	default:
		t.Fatal("no wire frame captured — nothing was published on the Link")
	}

	e2eWaitFor(t, "egress packet counted", func() bool { return glueB.PacketsEgress() >= 1 })
	e2eWaitFor(t, "far pump data counted", func() bool { return pumpB2.FramesData() >= 1 })
	e2eWaitFor(t, "ingress packet counted", func() bool { return glueR.PacketsIngress() == 1 })

	select {
	case leaked := <-eavesdropped:
		t.Fatalf("PRIVACY BREACH: wrong-PSK eavesdropper received %d bytes", len(leaked))
	case <-time.After(500 * time.Millisecond):
		// Expected: the wrong-PSK node never gets anything.
	}
}

// TestTunBridgeMTUFragmentation proves the MTU case: a 1500-byte
// DefaultMTU packet seals to 1528 bytes — exactly 8 MBRIDGE frames of
// MBRIPayloadMax — and reassembles at the far pump into a mesh publish
// that delivers the original 1500 bytes byte-identical.
func TestTunBridgeMTUFragmentation(t *testing.T) {
	psk := []byte("e2e-room-secret")

	nodeB := e2eNewNode(t, psk)
	nodeB2 := e2eNewNode(t, psk)
	nodeR := e2eNewNode(t, psk)

	link := NewFakeLink()
	pumpB, err := AttachPump(nodeB, link, nil, nil)
	if err != nil {
		t.Fatalf("AttachPump B: %v", err)
	}
	defer pumpB.Detach()
	pumpB2, err := AttachPump(nodeB2, link, nil, nil)
	if err != nil {
		t.Fatalf("AttachPump B2: %v", err)
	}
	defer pumpB2.Detach()

	ifaceB := tun.NewLoopback()
	glueB, err := AttachTunBridge(nodeB, pumpB, ifaceB, "bridge-room", psk, nil)
	if err != nil {
		t.Fatalf("AttachTunBridge B: %v", err)
	}
	defer glueB.Detach()

	ifaceR := tun.NewLoopback()
	glueR, err := AttachTunBridge(nodeR, nil, ifaceR, "bridge-room", psk, nil)
	if err != nil {
		t.Fatalf("AttachTunBridge R: %v", err)
	}
	defer glueR.Detach()

	if code := nodeR.Connect("127.0.0.1:" + strconv.Itoa(nodeB2.ListenPort())); code != mesh.MOSS_OK {
		t.Fatalf("nodeR.Connect: error code %d", code)
	}
	waitConnected(t, nodeR, nodeB2)
	nodeR.Subscribe(DefaultTopic)

	// Count the wire frames this packet produces.
	var framesOnWire atomic.Int64
	link.Subscribe(DefaultTopic, func(topic string, payload []byte) {
		if IsMBridge(payload) {
			framesOnWire.Add(1)
		}
	})

	time.Sleep(1 * time.Second)

	pkt := e2eIP4Packet(tun.DefaultMTU)
	if err := ifaceB.WritePacket(pkt); err != nil {
		t.Fatalf("ifaceB.WritePacket: %v", err)
	}

	got := make(chan []byte, 1)
	go func() {
		p, err := ifaceR.ReadPacket()
		if err != nil {
			return
		}
		got <- p
	}()
	select {
	case p := <-got:
		if !bytes.Equal(p, pkt) {
			t.Fatalf("fragmented packet mismatch: got %d bytes, want %d", len(p), len(pkt))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the MTU packet to arrive")
	}

	// 1500 bytes + 28 AEAD overhead = 1528 = exactly 8 × 191.
	e2eWaitFor(t, "8 wire frames for the MTU packet", func() bool { return framesOnWire.Load() == 8 })
	if sealed := tun.DefaultMTU + 28; sealed%MBRIPayloadMax != 0 || sealed/MBRIPayloadMax != 8 {
		t.Fatalf("MTU arithmetic broken: %d is not 8 frames of %d", sealed, MBRIPayloadMax)
	}
	e2eWaitFor(t, "ingress MTU packet counted", func() bool { return glueR.PacketsIngress() == 1 })
}

// TestTunBridgeRoomKeyDerivation pins the glue key to mesh's
// derivation: deterministic per (room, PSK), 32 bytes, and different
// PSKs or rooms produce different keys.
func TestTunBridgeRoomKeyDerivation(t *testing.T) {
	pskA := []byte("alpha-secret")
	pskB := []byte("bravo-secret")

	kA1, err := bridgeRoomKey("bridge-room", pskA)
	if err != nil {
		t.Fatalf("bridgeRoomKey: %v", err)
	}
	kA2, err := bridgeRoomKey("bridge-room", pskA)
	if err != nil {
		t.Fatalf("bridgeRoomKey (repeat): %v", err)
	}
	if !bytes.Equal(kA1, kA2) {
		t.Fatal("room key derivation is not deterministic")
	}
	if len(kA1) != 32 {
		t.Fatalf("room key len %d, want 32", len(kA1))
	}

	kB, err := bridgeRoomKey("bridge-room", pskB)
	if err != nil {
		t.Fatalf("bridgeRoomKey (other psk): %v", err)
	}
	if bytes.Equal(kA1, kB) {
		t.Fatal("different PSKs derived the same room key")
	}

	kOther, err := bridgeRoomKey("other-room", pskA)
	if err != nil {
		t.Fatalf("bridgeRoomKey (other room): %v", err)
	}
	if bytes.Equal(kA1, kOther) {
		t.Fatal("different rooms derived the same key from one PSK")
	}

	if _, err := bridgeRoomKey("", pskA); err == nil {
		t.Fatal("empty room accepted")
	}
	if _, err := bridgeRoomKey("bridge-room", nil); err == nil {
		t.Fatal("empty PSK accepted")
	}
}

// TestTunBridgeDetachLifecycle proves the teardown contract: while
// attached, packets flow; Detach stops the egress goroutine (its iface
// read is unblocked and reaped), restores the previous message
// callback verbatim, and a second Detach is a no-op.
func TestTunBridgeDetachLifecycle(t *testing.T) {
	psk := []byte("e2e-room-secret")
	nodeB := e2eNewNode(t, psk)
	nodeB2 := e2eNewNode(t, psk)
	nodeR := e2eNewNode(t, psk)

	link := NewFakeLink()
	pumpB, err := AttachPump(nodeB, link, nil, nil)
	if err != nil {
		t.Fatalf("AttachPump B: %v", err)
	}
	defer pumpB.Detach()
	pumpB2, err := AttachPump(nodeB2, link, nil, nil)
	if err != nil {
		t.Fatalf("AttachPump B2: %v", err)
	}
	defer pumpB2.Detach()

	var prevCalls atomic.Int64
	prevCB := func(channel string, _ [32]byte, _ []byte) { prevCalls.Add(1) }
	nodeB.SetMessageCallback(prevCB)

	ifaceB := tun.NewLoopback()
	glueB, err := AttachTunBridge(nodeB, pumpB, ifaceB, "bridge-room", psk, prevCB)
	if err != nil {
		t.Fatalf("AttachTunBridge B: %v", err)
	}

	ifaceR := tun.NewLoopback()
	glueR, err := AttachTunBridge(nodeR, nil, ifaceR, "bridge-room", psk, nil)
	if err != nil {
		t.Fatalf("AttachTunBridge R: %v", err)
	}
	defer glueR.Detach()

	if code := nodeR.Connect("127.0.0.1:" + strconv.Itoa(nodeB2.ListenPort())); code != mesh.MOSS_OK {
		t.Fatalf("nodeR.Connect: error code %d", code)
	}
	waitConnected(t, nodeR, nodeB2)
	nodeR.Subscribe(DefaultTopic)
	time.Sleep(1 * time.Second)

	// One packet flows while attached.
	pkt := e2eIP4Packet(64)
	if err := ifaceB.WritePacket(pkt); err != nil {
		t.Fatalf("ifaceB.WritePacket: %v", err)
	}
	got := make(chan []byte, 1)
	go func() {
		p, err := ifaceR.ReadPacket()
		if err != nil {
			return
		}
		got <- p
	}()
	select {
	case p := <-got:
		if !bytes.Equal(p, pkt) {
			t.Fatal("lifecycle packet mismatch")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the while-attached packet")
	}

	egressBefore := glueB.PacketsEgress()

	// Detach B's glue: the egress goroutine must die, prev must come
	// back, and a second Detach must do nothing.
	glueB.Detach()
	glueB.Detach()

	// The egress side is dead: a queued packet is never picked up (the
	// iface was closed by Detach, so it cannot even be queued — write
	// fails), and egress stays frozen at its pre-Detach value.
	if err := ifaceB.WritePacket(pkt); err == nil {
		t.Fatal("write to a detached bridge's iface unexpectedly succeeded")
	}
	time.Sleep(300 * time.Millisecond)
	if got := glueB.PacketsEgress(); got != egressBefore {
		t.Fatalf("egress counter moved after Detach: %d → %d", egressBefore, got)
	}

	// prev is restored: a publish on a FOREIGN channel reaches it.
	nodeB.Subscribe("foreign-chat")
	time.Sleep(200 * time.Millisecond)
	nodeB.Publish("foreign-chat", []byte("hello"))
	e2eWaitFor(t, "prev callback restored", func() bool { return prevCalls.Load() >= 1 })
}
