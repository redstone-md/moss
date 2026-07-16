package mesh

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/nat"
	"github.com/redstone-md/moss/internal/overlay"
)

// startOverlayNode builds a node with discovery disabled, so the only topology
// is the one the test wires by hand.
func startOverlayNode(t *testing.T, meshID string) *Node {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Trackers = nil
	cfg.LANDiscoveryEnabled = false
	cfg.DHTEnabled = false
	n, err := NewNode(meshID, nil, cfg)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	if code := n.Start(); code != MOSS_OK {
		t.Fatalf("Start: %d", code)
	}
	t.Cleanup(func() { n.Stop() })
	return n
}

// makeCore forces the node to look publicly reachable. Only such a node holds
// overlay records and answers lookups — a query cannot reach a node nobody can
// dial — and on loopback nothing would classify as public on its own.
func makeCore(n *Node) {
	n.natProfile.Store(nat.Profile{Type: nat.TypePublic, PublicReachable: true})
}

func nodeAddr(n *Node) string {
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(n.ListenPort()))
}

// attachLeaf connects a leaf to a core node and records the core as a routable
// overlay contact, which is what the substrate's peer exchange does in the real
// network once the core advertises its reachability.
func attachLeaf(t *testing.T, leaf, core *Node) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := leaf.connectPeer(ctx, nodeAddr(core)); err != nil {
		t.Fatalf("leaf could not attach to core: %v", err)
	}
	leaf.mu.Lock()
	info := leaf.knownPeers[core.localPeerID()]
	info.id = core.localPeerID()
	info.addr = nodeAddr(core)
	info.publicReachable = true
	leaf.knownPeers[core.localPeerID()] = info
	leaf.mu.Unlock()
	leaf.overlaySeedFromKnownPeers()
}

// This is the gap the shared substrate opened: subscription state only ever
// travels one hop, so two subscribers to a sparse channel — scattered among
// hundreds of unrelated substrate peers — never learn about each other, and the
// publisher's messages go nowhere. The overlay must make it deterministic:
// A and B here are NOT connected to each other and never will be; they share
// only a core node.
func TestOverlayLeavesFindEachOtherThroughCore(t *testing.T) {
	core := startOverlayNode(t, "room")
	makeCore(core)
	a := startOverlayNode(t, "room")
	b := startOverlayNode(t, "room")

	attachLeaf(t, a, core)
	attachLeaf(t, b, core)

	if code := a.Subscribe("sparse-channel"); code != MOSS_OK {
		t.Fatalf("a.Subscribe: %d", code)
	}
	if code := b.Subscribe("sparse-channel"); code != MOSS_OK {
		t.Fatalf("b.Subscribe: %d", code)
	}

	// A and B must not be direct peers: the whole point is finding a peer you
	// have no connection to.
	if a.peerByID(b.localPeerID()) != nil {
		t.Fatal("test precondition broken: A and B are directly connected")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	a.republishOverlayRecords(ctx)
	b.republishOverlayRecords(ctx)

	topic := a.roomTopic("sparse-channel")
	var found map[string]reachabilityHint
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		found = a.findChannelPeers(ctx, topic)
		if _, ok := found[b.localPeerID()]; ok {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	hint, ok := found[b.localPeerID()]
	if !ok {
		t.Fatalf("A did not find B on the channel via the overlay; found %d peers: %v", len(found), found)
	}
	// The record must carry a way to reach B, which is the other half: B is a
	// leaf and undialable, so the finder needs the core node to relay through.
	if len(hint.Attachments) == 0 {
		t.Fatal("B's record carries no attachment, so A has no way to reach an undialable leaf")
	}
	if hint.Attachments[0] != nodeAddr(core) {
		t.Fatalf("attachment = %q, want the core node %q", hint.Attachments[0], nodeAddr(core))
	}
}

// The end-to-end case, and the one that was broken in production: two players
// on a channel nobody else shares, connected only to a relay, publish and see
// nothing. Nothing here wires A to B — the mesh must find the path itself:
// overlay rendezvous → relay through the attachment → graft → delivery.
func TestOverlayDeliversBetweenLeavesWithNoDirectPath(t *testing.T) {
	core := startOverlayNode(t, "room")
	makeCore(core)
	a := startOverlayNode(t, "room")
	b := startOverlayNode(t, "room")

	received := make(chan []byte, 4)
	b.SetMessageCallback(func(channel string, _ [32]byte, data []byte) {
		if channel == "sparse-channel" {
			received <- append([]byte(nil), data...)
		}
	})

	attachLeaf(t, a, core)
	attachLeaf(t, b, core)

	if code := a.Subscribe("sparse-channel"); code != MOSS_OK {
		t.Fatalf("a.Subscribe: %d", code)
	}
	if code := b.Subscribe("sparse-channel"); code != MOSS_OK {
		t.Fatalf("b.Subscribe: %d", code)
	}
	if a.peerByID(b.localPeerID()) != nil {
		t.Fatal("test precondition broken: A and B start out directly connected")
	}

	// Records are refreshed on a 30s timer in production; publish them now so
	// the test does not wait on it. Everything after this is the mesh's own doing.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	a.republishOverlayRecords(ctx)
	b.republishOverlayRecords(ctx)

	deadline := time.Now().Add(25 * time.Second)
	for time.Now().Before(deadline) {
		a.Publish("sparse-channel", []byte("lobby"))
		select {
		case got := <-received:
			if string(got) != "lobby" {
				t.Fatalf("payload = %q, want \"lobby\"", got)
			}
			return // the mesh found the path on its own
		case <-time.After(500 * time.Millisecond):
		}
	}
	t.Fatalf("B never received A's publish: the topic mesh did not form (a mesh peers=%d, a subscribers known=%d)",
		len(a.pubsub.MeshPeers(a.roomTopic("sparse-channel"))),
		len(a.pubsub.NonMeshSubscribers(a.roomTopic("sparse-channel"))))
}

// A leaf must never answer lookups: nobody can dial it, so records parked there
// would be unreachable and the lookup would dead-end.
func TestOverlayLeafIsNotCore(t *testing.T) {
	leaf := startOverlayNode(t, "room")
	if leaf.overlayIsCore() {
		t.Fatal("a node on loopback with no confirmed inbound reach must not serve as core")
	}
	core := startOverlayNode(t, "room")
	makeCore(core)
	if !core.overlayIsCore() {
		t.Fatal("a publicly reachable node must serve as core")
	}
}

// A store from a peer must be attributed to that peer, never to a claim in the
// payload — the cheapest forgery is publishing a record about someone else.
func TestOverlayStoreAttributesToSender(t *testing.T) {
	core := startOverlayNode(t, "room")
	makeCore(core)
	leaf := startOverlayNode(t, "room")
	attachLeaf(t, leaf, core)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	leaf.republishOverlayRecords(ctx)

	self, ok := leaf.localOverlayID()
	if !ok {
		t.Fatal("leaf has no overlay id")
	}
	// STORE is unacknowledged and therefore asynchronous; wait for the core to
	// process it rather than racing the envelope.
	var entries []overlay.Entry
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if entries = core.overlayStore.Get(self, time.Now()); len(entries) > 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if len(entries) != 1 {
		t.Fatalf("core holds %d providers for the leaf's own key, want 1", len(entries))
	}
	if entries[0].Peer != self {
		t.Fatalf("record attributed to %s, want the sending leaf %s", entries[0].Peer, self)
	}
}

// The channel key must not be the bare name: the substrate is room-blind by
// design, and a core node holding a record should not learn which game it is.
func TestOverlayChannelKeyUsesOpaqueTopic(t *testing.T) {
	n := startOverlayNode(t, "room")
	topic := n.roomTopic("gse-app-2767030")
	if topic == "gse-app-2767030" {
		t.Fatal("room topic must not be the bare channel name")
	}
	if overlay.ChannelKey(topic) == overlay.ChannelKey("gse-app-2767030") {
		t.Fatal("the overlay key must derive from the opaque topic, not the bare channel")
	}
}
