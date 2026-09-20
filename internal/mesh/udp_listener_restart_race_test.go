package mesh

// Bug №7 harness: the restart race the census caught on
// TestPublishIsDeliveredAfterRestart under -race.
//
// Stop() does not join probePortMapping: it is deliberately wg-untracked,
// bounded only by its own STUN/mapping timeouts (its contexts are
// Background-derived, so cancelling rootCtx does not interrupt it either).
// Its STUN helpers read n.udpListener for several seconds after Stop has
// returned — and a restart's Start() then assigns a fresh n.udpListener to
// the same field. Unsynchronized read-while-write on a plain field: the
// previous run's probe goroutine against the next run's Start.
//
// The repro is deterministic without network luck: the probe's first
// statement chain dereferences the field (requestSTUNBindingObservation's
// nil gate) the instant Start returns, so an immediate Stop+Start pairs
// that read with the next Start's write under no ordering at all.

import (
	"testing"
	"time"
)

func TestUDPListenerRestartDoesNotRaceProbeGoroutine(t *testing.T) {
	cfg := isolatedTestConfig("restart-race")
	// A public-shaped tracker (TEST-NET-3) turns the STUN bootstrap on —
	// the production shape the field-reading probe path takes. The STUN
	// servers themselves are never assumed to answer: the race is on the
	// field read, not on any observation.
	cfg.Trackers = []string{"http://203.0.113.7:4001/announce"}

	node, err := NewNode("mesh-restart-race", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	t.Cleanup(func() { _ = node.Stop() })
	if code := node.Start(); code != MOSS_OK {
		t.Fatalf("first Start: %d", code)
	}
	// Let the probe take its first field read and park inside its STUN
	// window — alive, unjoined, and about to read the field again.
	time.Sleep(50 * time.Millisecond)

	for round := 0; round < 3; round++ {
		if code := node.Stop(); code != MOSS_OK {
			t.Fatalf("round %d Stop: %d", round, code)
		}
		// The write the race is about: a fresh listener assigned while the
		// previous run's probe goroutine is still inside its STUN windows.
		if code := node.Start(); code != MOSS_OK {
			t.Fatalf("round %d Start: %d", round, code)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
