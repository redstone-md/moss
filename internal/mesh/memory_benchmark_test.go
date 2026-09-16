package mesh

import (
	"fmt"
	"net"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"
)

// steadyStateHeapBaselineMB is the default heap ceiling for the 201-node
// steady state: a recent `heap_mb` measurement on the reference CI runner,
// padded so ordinary allocator noise cannot flake the gate. The observable
// contract is per-node cost (~0.42 MB * 201 here); the baseline lets CI fail
// when a change grows steady-state memory by more than the allowed margin.
//
// Override with MOSS_HEAP_BASELINE_MB (a float). The benchmark fails when
// heap_mb exceeds baseline * (1 + MOSS_HEAP_ALLOWED_GROWTH, default 0.20).
const steadyStateHeapBaselineMB = 80.0

// heapBudgetMB returns the ceiling the measured heap_mb must not exceed.
func heapBudgetMB(b *testing.B) float64 {
	b.Helper()
	baseline := steadyStateHeapBaselineMB
	if raw := os.Getenv("MOSS_HEAP_BASELINE_MB"); raw != "" {
		v, err := strconv.ParseFloat(raw, 64)
		if err != nil || v <= 0 {
			b.Fatalf("invalid MOSS_HEAP_BASELINE_MB %q: %v", raw, err)
		}
		baseline = v
	}
	growth := 0.20
	if raw := os.Getenv("MOSS_HEAP_ALLOWED_GROWTH"); raw != "" {
		v, err := strconv.ParseFloat(raw, 64)
		if err != nil || v < 0 {
			b.Fatalf("invalid MOSS_HEAP_ALLOWED_GROWTH %q: %v", raw, err)
		}
		growth = v
	}
	return baseline * (1 + growth)
}

// BenchmarkTwoHundredPeerSteadyStateMemory measures the heap a node holds at
// steady state with 200 connected peers, plus the cost of one MeshInfoJSON
// snapshot pass. The reported ns/op covers the full build/measure/teardown
// cycle — the numbers this gate exists for are the heap_* metrics, checked
// against the baseline budget below.
func BenchmarkTwoHundredPeerSteadyStateMemory(b *testing.B) {
	budget := heapBudgetMB(b)
	for b.Loop() {
		runtime.GC()
		runtime.GC()
		var before runtime.MemStats
		runtime.ReadMemStats(&before)

		cfgRoot := DefaultConfig()
		cfgRoot.MasqConfig = MasqConfig{}
		cfgRoot.Trackers = nil
		cfgRoot.GossipSub.HeartbeatMS = 250
		cfgRoot.MaxPeers = 256
		root, err := NewNode("mesh-memory-200", nil, cfgRoot)
		if err != nil {
			b.Fatalf("NewNode root failed: %v", err)
		}
		if code := root.Start(); code != MOSS_OK {
			b.Fatalf("root.Start failed: %d", code)
		}

		nodes := make([]*Node, 0, 201)
		nodes = append(nodes, root)
		for peerIndex := range 200 {
			cfg := DefaultConfig()
			cfg.MasqConfig = MasqConfig{}
			cfg.Trackers = nil
			cfg.GossipSub.HeartbeatMS = 250
			cfg.MaxPeers = 1
			cfg.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(root.ListenPort()))}
			node, err := NewNode("mesh-memory-200", nil, cfg)
			if err != nil {
				b.Fatalf("NewNode peer %d failed: %v", peerIndex, err)
			}
			if code := node.Start(); code != MOSS_OK {
				b.Fatalf("peer %d Start failed: %d", peerIndex, code)
			}
			nodes = append(nodes, node)
		}
		for _, node := range nodes[1:] {
			waitForPeerCountBench(b, node, 1)
		}
		waitForPeerCountBench(b, root, 200)

		runtime.GC()
		runtime.GC()
		var after runtime.MemStats
		runtime.ReadMemStats(&after)
		heapBytes := after.HeapAlloc - before.HeapAlloc
		b.ReportMetric(float64(heapBytes), "heap_bytes")
		heapMB := float64(heapBytes) / 1024.0 / 1024.0
		b.ReportMetric(heapMB, "heap_mb")
		b.ReportMetric(float64(heapBytes)/201.0/1024.0, "kb_per_node")
		if heapMB > budget {
			b.Fatalf("steady-state heap %.2f MB exceeds budget %.2f MB (baseline +20%%); a change grew per-node memory", heapMB, budget)
		}

		_ = root.MeshInfoJSON()

		for idx := len(nodes) - 1; idx >= 0; idx-- {
			nodes[idx].Stop()
		}
	}
}

// loadSoakWindow bounds how long TestTwentyFiveNodeLoadSoakSustainsPublishing
// drives its topology. The default keeps `go test ./internal/mesh` fast
// (~30s of sustained load); CI sets MOSS_SOAK_WINDOW_SEC to run the same
// scenario for minutes. Values <= 0 get the default.
func loadSoakWindow() time.Duration {
	secs := 30
	if raw := os.Getenv("MOSS_SOAK_WINDOW_SEC"); raw != "" {
		if v, err := strconv.Atoi(raw); err == nil && v > 0 {
			secs = v
		}
	}
	return time.Duration(secs) * time.Second
}

// TestTwentyFiveNodeLoadSoakSustainsPublishing drives the 25-node star for a
// sustained window — the same topology the burst test proves in one shot
// (TestTwentyFiveNodePublishBurstPropagatesToAllSubscribers), held under
// rotating publish churn long enough for timer-driven behavior (maintenance
// passes, heartbeat re-announce, prune cycles) to fire repeatedly. CI runs
// this with a multi-minute MOSS_SOAK_WINDOW_SEC; locally the 30s default
// keeps the package green without a long wait.
func TestTwentyFiveNodeLoadSoakSustainsPublishing(t *testing.T) {
	window := loadSoakWindow()

	cfgRoot := DefaultConfig()
	cfgRoot.MasqConfig = MasqConfig{}
	cfgRoot.Trackers = nil
	cfgRoot.GossipSub.HeartbeatMS = 50
	cfgRoot.MaxPeers = 32
	root, err := NewNode("mesh-load-soak-25", nil, cfgRoot)
	if err != nil {
		t.Fatalf("NewNode root failed: %v", err)
	}
	if code := root.Start(); code != MOSS_OK {
		t.Fatalf("root.Start failed: %d", code)
	}
	defer root.Stop()

	nodes := []*Node{root}
	defer func() {
		for _, node := range nodes[1:] {
			node.Stop()
		}
	}()
	for range 24 {
		cfg := DefaultConfig()
		cfg.MasqConfig = MasqConfig{}
		cfg.Trackers = nil
		cfg.GossipSub.HeartbeatMS = 50
		cfg.MaxPeers = 1
		cfg.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(root.ListenPort()))}
		node, err := NewNode("mesh-load-soak-25", nil, cfg)
		if err != nil {
			t.Fatalf("NewNode peer failed: %v", err)
		}
		if code := node.Start(); code != MOSS_OK {
			t.Fatalf("peer Start failed: %d", code)
		}
		nodes = append(nodes, node)
	}

	waitForPeerCount(t, root, 24)
	for _, node := range nodes[1:] {
		waitForPeerCount(t, node, 1)
	}
	for _, node := range nodes {
		if code := node.Subscribe("soak"); code != MOSS_OK {
			t.Fatalf("Subscribe failed: %d", code)
		}
	}
	time.Sleep(150 * time.Millisecond)

	// Rotate publishers across the whole fleet (root + leaves): every node
	// both publishes and receives, so a degradation on any one path shows
	// up as a missing payload at some subscriber.
	publishInterval := 125 * time.Millisecond
	published := 0
	start := time.Now()
	for i := 0; time.Since(start) < window; i++ {
		payload := fmt.Sprintf("load-soak-%06d", i)
		publisher := nodes[i%len(nodes)]
		if code := publisher.Publish("soak", []byte(payload)); code != MOSS_OK {
			t.Fatalf("publish %d from node %d failed: %d (window %s)", i, i%len(nodes), code, window)
		}
		published++

		// Connectivity must hold for the whole window, not just at the end.
		if root.currentPeerCount() < 24 {
			t.Fatalf("root peer count dropped to %d during soak after %s; info=%s",
				root.currentPeerCount(), time.Since(start), root.MeshInfoJSON())
		}
		time.Sleep(publishInterval)
	}

	// Convergence check: the last burst of payloads must have reached every
	// node. Earlier payloads may have aged out of the 16-entry recent cache;
	// the last 8 are the freshest evidence.
	if published < 8 {
		t.Fatalf("soak window %s too short to produce %d payloads", window, 8)
	}
	recent := make([]string, 0, 8)
	for i := published - 8; i < published; i++ {
		recent = append(recent, fmt.Sprintf("load-soak-%06d", i))
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if everyNodeHasPayloads(nodes, "soak", recent) {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("25-node soak did not converge after %s of publishing; root=%s", window, root.MeshInfoJSON())
}
