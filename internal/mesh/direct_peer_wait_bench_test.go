//go:build windows || unix

package mesh

import (
	"testing"
	"time"
)

// Eight absent recipients reproduce the local readiness work without public
// topology, handshakes or application polling. Every result must remain false.
func BenchmarkDirectPeerWaitOffline(b *testing.B) {
	node := &Node{peers: make(map[string]*peerConn)}
	done := make(chan bool, 8)
	ids := [8]string{"offline-0", "offline-1", "offline-2", "offline-3",
		"offline-4", "offline-5", "offline-6", "offline-7"}
	b.ReportAllocs()
	b.ResetTimer()
	start := directWaitBenchmarkCPU()
	results := 0
	for range b.N {
		for _, id := range ids {
			go func() { done <- node.waitForDirectPeer(id, time.Second) }()
		}
		for range ids {
			if <-done {
				b.Fatal("absent recipient reported ready")
			}
			results++
		}
	}
	cpu := directWaitBenchmarkCPU() - start
	b.StopTimer()
	b.ReportMetric(float64(cpu)/float64(b.N), "cpu-ns/batch")
	b.ReportMetric(float64(results)/float64(b.N), "false-results/batch")
}
