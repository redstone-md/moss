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

// TestHundredNodeStarDeliversBurst scales the proven 25-node burst pattern
// (TestTwentyFiveNodePublishBurstPropagatesToAllSubscribers) to a 100-node
// star: one root (MaxPeers=128) and 99 leaves pinned to it (MaxPeers=1,
// StaticPeers=[root]), all on loopback, no trackers, 50ms gossip heartbeat.
// Scenario: converge (waitForPeerCount), subscribe every node, then a burst
// of 10 publishes from the root must reach every node within 60s
// (everyNodeHasPayloads).
//
// Cost: ~40 MB steady-state heap for the 100-node fleet (measured on every
// run and logged as the HeapAlloc delta after convergence) and roughly 1-2
// minutes wall clock, dominated by 99 concurrent Noise handshakes and the
// GRAFT/PRUNE join choreography. Two skip gates keep it off quick paths:
// `go test -short` skips it, and MOSS_LOAD100=0 skips it explicitly (any
// other value — or unset — runs it).
//
// Deliberately NOT wired into .github/workflows/ci-dev.yml: shared CI
// runners time out on multi-minute load scenarios. Whether (and where) it
// runs in CI is the orchestrator's call.
func TestHundredNodeStarDeliversBurst(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 100-node star burst in -short mode")
	}
	if raw := os.Getenv("MOSS_LOAD100"); raw == "0" {
		t.Skip("MOSS_LOAD100=0: 100-node star burst disabled")
	}

	const (
		leaves        = 99
		burstSize     = 10
		deliveryCheck = 60 * time.Second
	)
	started := time.Now()

	runtime.GC()
	runtime.GC()
	var heapBefore runtime.MemStats
	runtime.ReadMemStats(&heapBefore)

	cfgRoot := DefaultConfig()
	cfgRoot.Trackers = nil
	cfgRoot.GossipSub.HeartbeatMS = 50
	cfgRoot.MaxPeers = 128
	root, err := NewNode("mesh-burst-100", nil, cfgRoot)
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
	for range leaves {
		cfg := DefaultConfig()
		cfg.Trackers = nil
		cfg.GossipSub.HeartbeatMS = 50
		cfg.MaxPeers = 1
		cfg.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(root.ListenPort()))}
		node, err := NewNode("mesh-burst-100", nil, cfg)
		if err != nil {
			t.Fatalf("NewNode peer failed: %v", err)
		}
		if code := node.Start(); code != MOSS_OK {
			t.Fatalf("peer Start failed: %d", code)
		}
		nodes = append(nodes, node)
	}

	waitForPeerCount(t, root, leaves)
	for _, node := range nodes[1:] {
		waitForPeerCount(t, node, 1)
	}
	t.Logf("star converged: root holds %d peers after %s", leaves, time.Since(started))

	runtime.GC()
	runtime.GC()
	var heapAfter runtime.MemStats
	runtime.ReadMemStats(&heapAfter)
	fleetHeapMB := float64(heapAfter.HeapAlloc-heapBefore.HeapAlloc) / 1024.0 / 1024.0
	t.Logf("100-node fleet steady-state heap: %.1f MB", fleetHeapMB)

	for idx, node := range nodes {
		if code := node.Subscribe("alpha"); code != MOSS_OK {
			t.Fatalf("node %d Subscribe failed: %d", idx, code)
		}
	}
	time.Sleep(150 * time.Millisecond)

	payloads := make([]string, 0, burstSize)
	for i := range burstSize {
		payload := fmt.Sprintf("burst100-%02d", i)
		payloads = append(payloads, payload)
		if code := root.Publish("alpha", []byte(payload)); code != MOSS_OK {
			t.Fatalf("root Publish %s failed: %d", payload, code)
		}
	}

	deadline := time.Now().Add(deliveryCheck)
	for time.Now().Before(deadline) {
		if everyNodeHasPayloads(nodes, "alpha", payloads) {
			t.Logf("burst of %d payloads reached all %d nodes; total elapsed %s",
				burstSize, len(nodes), time.Since(started))
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("100-node burst did not converge within %s; root=%s", deliveryCheck, root.MeshInfoJSON())
}
