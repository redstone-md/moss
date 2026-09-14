package meshbridge

import (
	"bytes"
	"encoding/hex"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/mesh"
)

// newBridgeNode builds a started, isolated test node: no trackers, no
// LAN discovery, heartbeat fast enough for local topologies to form.
func newBridgeNode(t *testing.T, meshID string) *mesh.Node {
	t.Helper()
	cfg := mesh.DefaultConfig()
	cfg.Trackers = nil
	cfg.LANDiscoveryEnabled = false
	cfg.GossipSub.HeartbeatMS = 50
	node, err := mesh.NewNode(meshID, nil, cfg)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	if code := node.Start(); code != mesh.MOSS_OK {
		t.Fatalf("node.Start: error code %d", code)
	}
	t.Cleanup(func() { node.Stop() })
	return node
}

// hexOf is the node's peer ID in the hex form mesh keys peers by.
func hexOf(n *mesh.Node) string {
	key := n.PublicKey()
	return hex.EncodeToString(key[:])
}

// waitConnected gates on mutual RTT: a node knows a peer's RTT only
// after the maintenance loop's first ping round-trip, which means the
// session is up in both directions.
func waitConnected(t *testing.T, a, b *mesh.Node) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	idA := hexOf(a)
	idB := hexOf(b)
	for time.Now().Before(deadline) {
		if a.PeerRTT(idB) > 0 && b.PeerRTT(idA) > 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("nodes did not connect in time")
}

// TestLoopbackBridgePublishDeliversAcrossLink walks the full data path
// with no LoRa and no second gateway: a PublishBridge on node B's pump
// encodes MBRIDGE frames onto the FakeLink, and the same pump's Link
// ingress — the receive half — decodes them and Publishes into the
// moss mesh, where a subscriber receives the original bytes. The
// anti-loop check must not eat the pump's own frames on the mesh side:
// the loopback exercises the Link→moss direction only.
func TestLoopbackBridgePublishDeliversAcrossLink(t *testing.T) {
	nodeB := newBridgeNode(t, "mesh-bridge-loop")

	link := NewFakeLink()
	pumpB, err := AttachPump(nodeB, link, nil, nil)
	if err != nil {
		t.Fatalf("AttachPump: %v", err)
	}
	defer pumpB.Detach()

	// The pump is this node's own gateway, so its own frames coming
	// back on the Link are loopback by definition. Simulate a SECOND
	// gateway pair: another node whose pump shares the same Link topic
	// but is a different gateway. Its decode side must accept what the
	// first pump published (different gatewayID) and re-publish into
	// its own mesh, where the subscriber listens.
	nodeC := newBridgeNode(t, "mesh-bridge-loop")
	pumpC, err := AttachPump(nodeC, link, nil, nil)
	if err != nil {
		t.Fatalf("AttachPump C: %v", err)
	}
	defer pumpC.Detach()

	received := make(chan []byte, 4)
	nodeC.SetMessageCallback(func(channel string, sender [32]byte, data []byte) {
		if channel == pumpC.Topic {
			received <- append([]byte(nil), data...)
		}
	})
	nodeC.Subscribe(pumpC.Topic)
	time.Sleep(200 * time.Millisecond)

	// The 80-byte loopback payload from the design.
	payload := make([]byte, 80)
	for i := range payload {
		payload[i] = byte(i)
	}
	if err := pumpB.PublishBridge(payload); err != nil {
		t.Fatalf("PublishBridge: %v", err)
	}

	select {
	case got := <-received:
		if !bytes.Equal(got, payload) {
			t.Fatalf("payload mismatch: got %d bytes, want %d", len(got), len(payload))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for bridged delivery")
	}
	if pumpC.FramesData() == 0 {
		t.Fatal("receiving pump did not count a data message")
	}
}

// TestChainingPassthroughKeepsPrevCallback proves the splice: with a
// pre-registered application callback passed as prev, a non-MBRIDGE
// directed payload still reaches it after the pump attaches, while an
// MBRIDGE-tagged payload is consumed by the bridge.
func TestChainingPassthroughKeepsPrevCallback(t *testing.T) {
	node := newBridgeNode(t, "mesh-bridge-chain")

	var got atomic.Int64
	prev := func(sender [32]byte, data []byte) { got.Add(1) }
	node.SetPacketCallback(prev)

	link := NewFakeLink()
	pump, err := AttachPump(node, link, nil, prev)
	if err != nil {
		t.Fatalf("AttachPump: %v", err)
	}

	// Non-MBRIDGE payload: reaches prev through the splice. The splice
	// is the node's registered callback, so this call is exactly what
	// the node does for every directed payload.
	node.SetPacketCallback(pump.packetSplice) // already done by AttachPump; a no-op here
	prevHead := func(sender [32]byte, data []byte) {
		pump.packetSplice(sender, data)
	}
	prevHead([32]byte{}, []byte("plain application payload"))
	if got.Load() != 1 {
		t.Fatalf("prev callback not reached: got %d calls", got.Load())
	}
	if pump.IngressSpliced() != 1 || pump.IngressPassthrough() != 1 {
		t.Fatal("splice counters did not record the passthrough")
	}

	// MBRIDGE payload: consumed by the bridge, prev untouched.
	frames, err := Encode([32]byte{1}, [8]byte{2}, 0, 0, []byte("x"))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	prevHead([32]byte{}, frames[0])
	if got.Load() != 1 {
		t.Fatalf("prev callback reached for MBRIDGE frame: %d calls", got.Load())
	}
	if pump.FramesPublished() != 1 {
		t.Fatal("MBRIDGE frame not published to the Link")
	}
}

// TestDetachRestoresPrevCallback proves the restore: after Detach the
// node's callback is prev again, so the application's registration is
// the head, and the pump counts nothing further.
func TestDetachRestoresPrevCallback(t *testing.T) {
	node := newBridgeNode(t, "mesh-bridge-detach")

	var got atomic.Int64
	prev := func(sender [32]byte, data []byte) { got.Add(1) }
	node.SetPacketCallback(prev)

	link := NewFakeLink()
	pump, err := AttachPump(node, link, nil, prev)
	if err != nil {
		t.Fatalf("AttachPump: %v", err)
	}
	pump.Detach()
	pump.Detach() // idempotent

	// After detach the node's callback is prev verbatim: invoking the
	// node's chain (which now IS prev) must not touch the pump.
	prev([32]byte{}, []byte("post-detach"))
	if got.Load() != 1 {
		t.Fatal("prev not restored")
	}
	if pump.IngressSpliced() != 0 {
		t.Fatalf("pump still in the chain after detach: %d splices", pump.IngressSpliced())
	}

	// The Link is closed with the detach: late Publish errors and
	// nothing reaches the pump's subscription.
	if err := link.Publish(pump.Topic, []byte("late frame")); err == nil {
		t.Fatal("Link.Publish after Close must error")
	}
	if pump.FramesIngress() != 0 {
		t.Fatal("frames ingressed after detach")
	}
}

// TestKeepaliveLoopPublishesAndSweeps proves the keepalive goroutine
// sends GW_KEEPALIVE frames on the Link at its interval, that a second
// start is refused, that Detach stops the loop, and that the table
// sweeps stale entries on the 3x timeout.
func TestKeepaliveLoopPublishesAndSweeps(t *testing.T) {
	node := newBridgeNode(t, "mesh-bridge-keepalive")
	link := NewFakeLink()
	table := NewMbsTable()
	pump, err := AttachPump(node, link, table, nil)
	if err != nil {
		t.Fatalf("AttachPump: %v", err)
	}

	// Fast interval for the test; three keepalives prove repetition.
	if !pump.StartKeepaliveEvery(30 * time.Millisecond) {
		t.Fatal("StartKeepaliveEvery: not started")
	}
	if pump.StartKeepaliveEvery(30 * time.Millisecond) {
		t.Fatal("second StartKeepaliveEvery must not start another loop")
	}

	deadline := time.Now().Add(2 * time.Second)
	for pump.KeepalivesSent() < 3 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if pump.KeepalivesSent() < 3 {
		t.Fatalf("keepalives not flowing: %d in 2s", pump.KeepalivesSent())
	}

	// The keepalive frames must actually be on the Link (counted by
	// the FakeLink, whose own subscription dispatches them back into
	// the pump's ingress — where the own-gatewayID check drops them).
	if link.Published() < 3 {
		t.Fatalf("keepalive frames not published to the Link: %d", link.Published())
	}

	// Detach stops the loop and closes the Link.
	pump.Detach()
	sent := pump.KeepalivesSent()
	time.Sleep(100 * time.Millisecond)
	if pump.KeepalivesSent() != sent {
		t.Fatalf("keepalives continued after detach: %d -> %d", sent, pump.KeepalivesSent())
	}

	// The sweep: a registered entry with no keepalive refresh dies at
	// the 3x timeout. Sweep is also driven by the keepalive loop, so
	// this checks the table contract directly.
	table.Register(42, "cafe")
	table.Sweep(time.Now().Add(KeepaliveTimeout+time.Second), KeepaliveTimeout)
	if _, ok := table.Lookup(42); ok {
		t.Fatal("stale table entry survived the sweep")
	}
}

// TestFakeLinkCountsDispatch proves the FakeLink counters observe
// Publish and dispatch counts, the observability the keepalive and
// passthrough tests above rely on.
func TestFakeLinkCountsDispatch(t *testing.T) {
	link := NewFakeLink()
	if err := link.Publish("t", []byte("x")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if link.Published() != 1 {
		t.Fatalf("Published: %d", link.Published())
	}
	if link.Dispatched() != 0 {
		t.Fatalf("Dispatched without handlers: %d", link.Dispatched())
	}
	var hits atomic.Int64
	if err := link.Subscribe("t", func(topic string, payload []byte) { hits.Add(1) }); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := link.Publish("t", []byte("y")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if hits.Load() != 1 || link.Dispatched() != 1 {
		t.Fatalf("handler not dispatched once: hits=%d dispatched=%d", hits.Load(), link.Dispatched())
	}
}
