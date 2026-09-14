package stat

import (
	"testing"
)

// The finalized-epoch digest map must be bounded: without the window every
// finalized epoch kept its digest forever. Advancing well past
// chainKeepEpochs epochs prunes the old digests while keeping the head and
// its predecessor — Snapshot's prev-digest link and the next finalize both
// read epoch head-1, so it must survive the prune.
func TestChainPrunesToWindow(t *testing.T) {
	agg, err := NewAggregator(Config{EpochSec: 1, DPEpsilon: -1, KAnon: 1}, []byte("me"))
	if err != nil {
		t.Fatal(err)
	}
	total := uint64(chainKeepEpochs + 64)
	for e := uint64(0); e < total; e++ {
		if _, err := agg.ContributeLocal(e, 1, 1, 1, "public"); err != nil {
			t.Fatalf("contribute epoch %d: %v", e, err)
		}
	}
	agg.mu.Lock()
	held := len(agg.chain)
	head := agg.chainHead
	agg.mu.Unlock()
	if held > chainKeepEpochs {
		t.Fatalf("chain exceeded window: %d > %d", held, chainKeepEpochs)
	}
	// Contributing epoch e finalizes epoch e-2, so the head sits two behind
	// the last contribution — that is the window's newest entry.
	if head != total-3 {
		t.Fatalf("unexpected chain head %d (contributed through %d)", head, total-1)
	}
	r := agg.Snapshot()
	if r.ChainHead != head {
		t.Fatalf("Snapshot head %d != chain head %d", r.ChainHead, head)
	}
	// The head's predecessor must have survived the prune: both Snapshot
	// (prev-digest link) and the next finalize read epoch head-1.
	agg.mu.Lock()
	_, headMinusOneAlive := agg.chain[head-1]
	agg.mu.Unlock()
	if !headMinusOneAlive {
		t.Fatalf("epoch %d (head-1) did not survive the prune", head-1)
	}
	// The newest retained link must resolve: RecentChain must return the
	// head entry with a prev-digest link.
	chain := agg.RecentChain(2)
	if len(chain) != 2 {
		t.Fatalf("expected 2 retained chain entries, got %d", len(chain))
	}
	newest := chain[len(chain)-1]
	if newest.Epoch != head {
		t.Fatalf("newest retained entry %d != head %d", newest.Epoch, head)
	}
	if newest.Prev == "" {
		t.Fatal("head entry lost its prev-digest link")
	}
	// One more epoch advance must finalize against the retained prev
	// digest without error and keep the window bounded.
	if _, err := agg.ContributeLocal(total, 1, 1, 1, "public"); err != nil {
		t.Fatalf("finalize after prune failed: %v", err)
	}
	agg.mu.Lock()
	held = len(agg.chain)
	agg.mu.Unlock()
	if held > chainKeepEpochs {
		t.Fatalf("chain exceeded window after further advance: %d > %d", held, chainKeepEpochs)
	}
}
