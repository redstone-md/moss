package gossip

import (
	"fmt"
	"testing"
	"time"
)

// memoPeerID builds a valid 64-char hex peer id, the shape real peer keys
// have on the wire, so the memo tests exercise the actual decode path
// rather than the zero-array fallback for unparseable ids.
func memoPeerID(i int) string {
	return fmt.Sprintf("%064x", i)
}

// AdjustedScore is the path every scoring gate in the mesh funnels through
// when an application callback is registered. The memo exists so the dozens
// of per-envelope reads — threshold gates, sort comparators — stop at the
// map instead of re-invoking a callback that is, behind the FFI boundary, a
// C function call. A stable base must evaluate the callback exactly once.
func TestAdjustedScoreInvokesCallbackOncePerStableBase(t *testing.T) {
	engine := NewEngine()
	peer := memoPeerID(1)
	engine.Ensure(peer)
	engine.SetApplicationScore(peer, 5)

	calls := 0
	cb := func(peerID [32]byte, baseScore float64) float64 {
		calls++
		if peerID != DecodePeerKey(peer) {
			t.Errorf("callback saw key %x, want %x", peerID, DecodePeerKey(peer))
		}
		return baseScore + 1
	}

	for range 100 {
		if got := engine.AdjustedScore(peer, cb); got != 6 {
			t.Fatalf("AdjustedScore = %f, want 6", got)
		}
	}
	if calls != 1 {
		t.Fatalf("stable base: callback invoked %d times, want 1", calls)
	}
}

// The memo is keyed by the base it was computed from: any base change —
// Tick's TimeInMesh recompute, a reward, a penalty, a new application
// score — must re-evaluate the callback, otherwise the mesh would keep
// gating on a value the application no longer stands behind.
func TestAdjustedScoreReinvokesCallbackWhenBaseChanges(t *testing.T) {
	engine := NewEngine()
	peer := memoPeerID(2)
	engine.Ensure(peer)
	engine.SetApplicationScore(peer, 5)

	calls := 0
	cb := func(peerID [32]byte, baseScore float64) float64 {
		calls++
		return baseScore + 1
	}

	if got := engine.AdjustedScore(peer, cb); got != 6 {
		t.Fatalf("first evaluation = %f, want 6", got)
	}
	engine.SetApplicationScore(peer, 7)
	if got := engine.AdjustedScore(peer, cb); got != 8 {
		t.Fatalf("after base change = %f, want 8", got)
	}
	if calls != 2 {
		t.Fatalf("base change: callback invoked %d times, want 2", calls)
	}
	// The refreshed memo serves subsequent reads of the new base.
	for range 50 {
		if got := engine.AdjustedScore(peer, cb); got != 8 {
			t.Fatalf("memoized re-read = %f, want 8", got)
		}
	}
	if calls != 2 {
		t.Fatalf("after refresh: callback invoked %d times, want 2", calls)
	}
}

// Remove must evict the memo along with the peer entry: a stale memo
// surviving a disconnect would both leak one entry per peer the node ever
// connected to (the exact leak Remove exists to prevent) and serve a
// pre-disconnect adjusted score to a returning peer.
func TestAdjustedScoreRemoveEvictsMemo(t *testing.T) {
	engine := NewEngine()
	peer := memoPeerID(3)
	engine.Ensure(peer)
	engine.SetApplicationScore(peer, 5)

	calls := 0
	cb := func(peerID [32]byte, baseScore float64) float64 {
		calls++
		return baseScore + 1
	}

	engine.AdjustedScore(peer, cb)
	engine.mu.Lock()
	_, memoized := engine.appMemo[peer]
	engine.mu.Unlock()
	if !memoized {
		t.Fatal("expected a memo entry after the first evaluation")
	}

	if !engine.Remove(peer) {
		t.Fatal("expected Remove to report an existing peer")
	}
	engine.mu.Lock()
	_, memoized = engine.appMemo[peer]
	engine.mu.Unlock()
	if memoized {
		t.Fatal("expected Remove to evict the memo entry")
	}

	// The evicted peer now reads as unknown: live evaluation at base 0,
	// and it must not re-memoize.
	if got := engine.AdjustedScore(peer, cb); got != 1 {
		t.Fatalf("unknown re-read = %f, want 1", got)
	}
	if calls != 2 {
		t.Fatalf("callback invoked %d times, want 2", calls)
	}
	engine.mu.Lock()
	_, memoized = engine.appMemo[peer]
	engine.mu.Unlock()
	if memoized {
		t.Fatal("an evicted peer must not be re-memoized on read")
	}
}

// An unknown peer must keep the live evaluation and never touch the memo
// map — mirroring Score's contract that a lookup of a stranger does not
// track the stranger. Without this, a flood of AdjustedScore calls for
// strangers would grow the memo map without bound.
func TestAdjustedScoreOfUnknownPeerIsNeverMemoized(t *testing.T) {
	engine := NewEngine()
	stranger := memoPeerID(4)

	calls := 0
	cb := func(peerID [32]byte, baseScore float64) float64 {
		calls++
		if baseScore != 0 {
			t.Errorf("callback saw base %f for a stranger, want 0", baseScore)
		}
		return baseScore + 1
	}

	for range 50 {
		if got := engine.AdjustedScore(stranger, cb); got != 1 {
			t.Fatalf("unknown peer = %f, want 1", got)
		}
	}
	if calls != 50 {
		t.Fatalf("unknown peer: callback invoked %d times, want 50", calls)
	}
	engine.mu.Lock()
	tracked := len(engine.peers)
	memoized := len(engine.appMemo)
	engine.mu.Unlock()
	if tracked != 0 || memoized != 0 {
		t.Fatalf("stranger tracked (%d) or memoized (%d), want 0/0", tracked, memoized)
	}
}

// The callback runs outside every engine lock. A registered callback may
// itself touch the engine (the mesh's registered hooks reach back into
// node state, and test callbacks take the node mutex), so invoking it
// under e.mu would invert the acquisition order and deadlock. Ensure
// write-locks the engine: if AdjustedScore held any lock across the
// callback, this test deadlocks instead of passing.
func TestAdjustedScoreRunsCallbackOutsideEngineLock(t *testing.T) {
	engine := NewEngine()
	peer := memoPeerID(5)
	engine.Ensure(peer)

	cb := func(peerID [32]byte, baseScore float64) float64 {
		engine.Ensure(peer)
		return baseScore
	}

	done := make(chan struct{})
	go func() {
		engine.AdjustedScore(peer, cb)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("callback deadlocked against the engine lock")
	}
}

// BenchmarkAdjustedScoreMemoHit measures the memoized hot path: the score
// read every mesh threshold gate and sort comparator performs per
// envelope. With a registered callback and a stable base, the hit path is
// one RLock, one map lookup and one struct copy — no callback invocation,
// no hex decode, no heap allocation.
//
// Baseline (Ryzen 5 3600X, linux, go1.25.9, 2026-09-15, single scored
// peer, memo pre-warmed): ~27 ns/op, 0 B/op, 0 allocs/op. Before the
// memo, the same read paid a callback invocation and a hex decode
// (64-char hex → 32-byte slice) on every gate.
func BenchmarkAdjustedScoreMemoHit(b *testing.B) {
	engine := NewEngine()
	peer := memoPeerID(1)
	engine.Ensure(peer)
	engine.SetApplicationScore(peer, 5)
	cb := func(peerID [32]byte, baseScore float64) float64 {
		return baseScore + 1
	}
	// Warm the memo so the benchmark measures the hit path, not the miss.
	engine.AdjustedScore(peer, cb)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		sink += engine.AdjustedScore(peer, cb)
	}
}

var sink float64
