package mesh

import (
	"net"
	"runtime"
	"strconv"
	"testing"
)

func BenchmarkTwoHundredPeerSteadyStateMemory(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		runtime.GC()
		runtime.GC()
		var before runtime.MemStats
		runtime.ReadMemStats(&before)

		cfgRoot := DefaultConfig()
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
		for peerIndex := 0; peerIndex < 200; peerIndex++ {
			cfg := DefaultConfig()
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
		b.ReportMetric(float64(heapBytes)/1024.0/1024.0, "heap_mb")
		b.ReportMetric(float64(heapBytes)/201.0/1024.0, "kb_per_node")

		b.StartTimer()
		_ = root.MeshInfoJSON()
		b.StopTimer()

		for idx := len(nodes) - 1; idx >= 0; idx-- {
			nodes[idx].Stop()
		}
	}
}
