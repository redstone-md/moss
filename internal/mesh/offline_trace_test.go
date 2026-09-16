package mesh

import (
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/inspect"
)

func TestDefaultOfflineConfigIsOffline(t *testing.T) {
	cfg := DefaultOfflineConfig()
	if !cfg.IsOffline() {
		t.Fatal("DefaultOfflineConfig should be offline")
	}
	if len(cfg.Trackers) != 0 {
		t.Fatalf("trackers not empty: %v", cfg.Trackers)
	}
	if cfg.DHTEnabled {
		t.Fatal("DHT should be off")
	}
	if !cfg.LANDiscoveryEnabled {
		t.Fatal("LAN discovery should stay on for isolated-site use")
	}
}

func TestDefaultConfigIsNotOffline(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.IsOffline() {
		t.Fatal("DefaultConfig should not be offline")
	}
}

// TestOfflinePresetThreeNodesStaticLoopback proves the isolated preset works
// end to end: no trackers, no DHT, static-peer bootstrap only. If the offline
// path ever reaches for public bootstrap — or stops gossiping among nodes that
// already know each other — this never converges.
func TestOfflinePresetThreeNodesStaticLoopback(t *testing.T) {
	offlineConfig := func() Config {
		c := DefaultOfflineConfig()
		c.GossipSub.HeartbeatMS = 50
		return c
	}

	root, err := NewNode("offline-test", nil, offlineConfig())
	if err != nil {
		t.Fatalf("NewNode root: %v", err)
	}
	if code := root.Start(); code != MOSS_OK {
		t.Fatalf("root.Start: %d", code)
	}
	defer root.Stop()

	rootAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(root.ListenPort()))
	nodes := []*Node{root}
	for range 2 {
		cfg := offlineConfig()
		cfg.StaticPeers = []string{rootAddr}
		leaf, nodeErr := NewNode("offline-test", nil, cfg)
		if nodeErr != nil {
			t.Fatalf("NewNode leaf: %v", nodeErr)
		}
		if code := leaf.Start(); code != MOSS_OK {
			t.Fatalf("leaf.Start: %d", code)
		}
		defer leaf.Stop()
		nodes = append(nodes, leaf)
	}

	waitForPeerCount(t, root, 2)
	for _, leaf := range nodes[1:] {
		waitForPeerCount(t, leaf, 1)
	}
	for _, node := range nodes {
		if code := node.Subscribe("offline"); code != MOSS_OK {
			t.Fatalf("Subscribe: %d", code)
		}
	}
	waitForMeshCountAtLeast(t, root, "offline", 1)

	if code := root.Publish("offline", []byte("airgap")); code != MOSS_OK {
		t.Fatalf("Publish: %d", code)
	}
	payloads := []string{"airgap"}
	deadline := time.Now().Add(10 * time.Second)
	for {
		if everyNodeHasPayloads(nodes, "offline", payloads) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("offline static mesh did not converge; root=%s", root.MeshInfoJSON())
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestTraceHopsRecordedAcrossForward(t *testing.T) {
	cfgA := DefaultConfig()
	cfgA.Trackers = nil
	cfgA.GossipSub.HeartbeatMS = 50
	a, err := NewNode("trace-test", nil, cfgA)
	if err != nil {
		t.Fatalf("NewNode A: %v", err)
	}
	if code := a.Start(); code != MOSS_OK {
		t.Fatalf("A.Start: %d", code)
	}
	defer a.Stop()

	cfgB := DefaultConfig()
	cfgB.Trackers = nil
	cfgB.GossipSub.HeartbeatMS = 50
	cfgB.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(a.ListenPort()))}
	b, err := NewNode("trace-test", nil, cfgB)
	if err != nil {
		t.Fatalf("NewNode B: %v", err)
	}
	if code := b.Start(); code != MOSS_OK {
		t.Fatalf("B.Start: %d", code)
	}
	defer b.Stop()
	b.debugBus.SetRecording(true)

	waitForPeerCount(t, a, 1)
	waitForPeerCount(t, b, 1)
	if code := a.Subscribe("traced"); code != MOSS_OK {
		t.Fatalf("A.Subscribe: %d", code)
	}
	if code := b.Subscribe("traced"); code != MOSS_OK {
		t.Fatalf("B.Subscribe: %d", code)
	}
	// The publish must see a formed mesh — otherwise NO_PEERS is legitimate.
	waitForMeshCountAtLeast(t, a, "traced", 1)

	traceFilter := &inspect.Filter{Kinds: []inspect.Kind{inspect.KindTrace}}

	if code := a.PublishTrace("", "traced", []byte("hello-trace"), "trace-1"); code != MOSS_OK {
		t.Fatalf("PublishTrace: %d", code)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		events := b.debugBus.History(0, traceFilter)
		if len(events) > 0 {
			hops, _ := events[len(events)-1].Fields["hops"].([]string)
			if len(hops) == 0 {
				t.Fatalf("trace event carried no hops: %+v", events[len(events)-1].Fields)
			}
			if hops[0] != a.localPeerID() {
				t.Fatalf("first hop = %s, want publisher %s", hops[0], a.localPeerID())
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("B never emitted a trace event for the traced publish")
		}
		time.Sleep(50 * time.Millisecond)
	}

	// An untagged publish pays nothing: no trace events appear.
	before := len(b.debugBus.History(0, traceFilter))
	if code := a.Publish("traced", []byte("plain")); code != MOSS_OK {
		t.Fatalf("plain Publish: %d", code)
	}
	deadline = time.Now().Add(5 * time.Second)
	for {
		if nodeHasCachedPayload(b, "traced", "plain") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("B never received the plain payload")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if after := len(b.debugBus.History(0, traceFilter)); after != before {
		t.Fatalf("untagged publish emitted %d extra trace events (want 0)", after-before)
	}
}

func TestAppendTraceHopCaps(t *testing.T) {
	node, err := NewNode("trace-cap-test", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	env := gossip.Envelope{TraceID: "t", TraceHops: make([]string, 0, traceHopCap)}
	for range traceHopCap {
		node.appendTraceHop(&env)
	}
	if got := counterValue(node, "__trace_hops_capped__"); got != 0 {
		t.Fatalf("cap hit early: %d", got)
	}
	node.appendTraceHop(&env)
	if got := counterValue(node, "__trace_hops_capped__"); got != 1 {
		t.Fatalf("cap not counted: %d", got)
	}
	if len(env.TraceHops) != traceHopCap {
		t.Fatalf("hops grew past cap: %d", len(env.TraceHops))
	}
	untagged := gossip.Envelope{}
	node.appendTraceHop(&untagged)
	if len(untagged.TraceHops) != 0 {
		t.Fatal("untagged envelope gained hops")
	}
}
