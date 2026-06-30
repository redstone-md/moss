package stat

import (
	"bytes"
	"fmt"
	"testing"
)

func TestHLLEstimateWithinError(t *testing.T) {
	h, err := NewHLL(14)
	if err != nil {
		t.Fatal(err)
	}
	const n = 50000
	for i := 0; i < n; i++ {
		eid := EID(uint64(i%7), []byte(fmt.Sprintf("node-%d", i)))
		h.Add(hash64(eid))
	}
	est := h.Estimate()
	diff := float64(int64(est)-n) / float64(n)
	if diff < 0 {
		diff = -diff
	}
	if diff > 0.03 {
		t.Fatalf("HLL estimate %d off by %.2f%% (want <3%%)", est, diff*100)
	}
}

func TestHLLMergeIsUnion(t *testing.T) {
	a, _ := NewHLL(12)
	b, _ := NewHLL(12)
	for i := 0; i < 1000; i++ {
		a.Add(hash64(EID(0, []byte(fmt.Sprintf("a%d", i)))))
	}
	for i := 0; i < 1000; i++ {
		b.Add(hash64(EID(0, []byte(fmt.Sprintf("b%d", i)))))
	}
	if err := a.Merge(b); err != nil {
		t.Fatal(err)
	}
	est := a.Estimate()
	if est < 1800 || est > 2200 {
		t.Fatalf("merged estimate %d not near 2000", est)
	}
}

func TestEIDUnlinkableAcrossEpochs(t *testing.T) {
	pk := []byte("stable-public-key-32-bytes-long!")
	e0 := EID(0, pk)
	e1 := EID(1, pk)
	if e0 == e1 {
		t.Fatal("eid did not rotate between epochs")
	}
	// Same epoch + same key is stable (so re-contribution is idempotent).
	if EID(5, pk) != EID(5, pk) {
		t.Fatal("eid not deterministic within an epoch")
	}
}

// TestSnapshotMergeConvergence is the core CRDT property: nodes that receive the
// same contributions in different orders compute byte-identical digests.
func TestSnapshotMergeConvergence(t *testing.T) {
	mk := func() (*Snapshot, []Delta) {
		s, _ := NewSnapshot(3, 12)
		var deltas []Delta
		for i := 0; i < 20; i++ {
			eid := EID(3, []byte(fmt.Sprintf("peer-%d", i)))
			m := Metrics{BandwidthIn: uint64(i * 100), BandwidthOut: uint64(i * 50), Degree: uint32(i % 8), NATType: "public", Seq: 1}
			deltas = append(deltas, Delta{Epoch: 3, EID: eid[:], Metrics: m})
		}
		return s, deltas
	}

	s1, deltas := mk()
	for _, d := range deltas {
		var eid [32]byte
		copy(eid[:], d.EID)
		s1.Contribute(eid, d.Metrics)
	}

	// Apply in reverse order, with a duplicate to exercise idempotency.
	s2, _ := NewSnapshot(3, 12)
	for i := len(deltas) - 1; i >= 0; i-- {
		var eid [32]byte
		copy(eid[:], deltas[i].EID)
		s2.Contribute(eid, deltas[i].Metrics)
	}
	var dup [32]byte
	copy(dup[:], deltas[0].EID)
	s2.Contribute(dup, deltas[0].Metrics)

	var prev [32]byte
	if s1.Digest(prev) != s2.Digest(prev) {
		t.Fatal("digests diverged for same contribution set (CRDT not convergent)")
	}
	if s1.Contributors() != 20 || s2.Contributors() != 20 {
		t.Fatalf("contributor count wrong: %d / %d", s1.Contributors(), s2.Contributors())
	}
}

func TestLWWDeterministicTiebreak(t *testing.T) {
	hi := Metrics{BandwidthIn: 999, Seq: 2}
	lo := Metrics{BandwidthIn: 1, Seq: 1}
	if mergeMetrics(hi, lo) != mergeMetrics(lo, hi) {
		t.Fatal("merge not commutative")
	}
	if mergeMetrics(hi, lo).Seq != 2 {
		t.Fatal("higher seq did not win")
	}
}

func TestDeltaRoundTrip(t *testing.T) {
	eid := EID(7, []byte("somekey"))
	d := Delta{Epoch: 7, EID: eid[:], Metrics: Metrics{BandwidthIn: 5, Degree: 3, NATType: "cgnat", Seq: 1}}
	b, err := d.Encode()
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeDelta(b)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.EID, d.EID) || got.Metrics != d.Metrics || got.Epoch != d.Epoch {
		t.Fatalf("round trip mismatch: %+v vs %+v", got, d)
	}
}

func TestDecodeDeltaRejectsBadEID(t *testing.T) {
	if _, err := DecodeDelta([]byte(`{"epoch":1,"eid":"AAAA","metrics":{}}`)); err == nil {
		t.Fatal("expected error for short eid")
	}
}

func TestKAnonGate(t *testing.T) {
	agg, err := NewAggregator(Config{KAnon: 5, EpochSec: 100, DPEpsilon: -1}, []byte("me"))
	if err != nil {
		t.Fatal(err)
	}
	// Three contributors < k=5 → detailed metrics suppressed.
	for i := 0; i < 3; i++ {
		eid := EID(1, []byte(fmt.Sprintf("n%d", i)))
		_ = agg.ApplyDelta(Delta{Epoch: 1, EID: eid[:], Metrics: Metrics{BandwidthIn: 100, NATType: "public", Seq: 1}})
	}
	r := agg.Snapshot()
	if r.KAnonOK {
		t.Fatal("k-anon gate should be closed with 3 < 5 contributors")
	}
	if r.BandwidthIn != 0 || r.NATHistogram != nil {
		t.Fatal("detailed metrics leaked below k-anon threshold")
	}
	// Add more to cross the threshold.
	for i := 3; i < 8; i++ {
		eid := EID(1, []byte(fmt.Sprintf("n%d", i)))
		_ = agg.ApplyDelta(Delta{Epoch: 1, EID: eid[:], Metrics: Metrics{BandwidthIn: 100, NATType: "public", Seq: 1}})
	}
	r = agg.Snapshot()
	if !r.KAnonOK {
		t.Fatalf("k-anon gate should open with 8 contributors, got %d", r.Contributors)
	}
	if r.BandwidthIn != 800 {
		t.Fatalf("expected summed bandwidth 800, got %d", r.BandwidthIn)
	}
}

func TestDPNoisePerturbs(t *testing.T) {
	agg, _ := NewAggregator(Config{DPEpsilon: 0.5, BandwidthCap: 1 << 20, EpochSec: 100}, []byte("me"))
	// With noise on, the contributed value should usually differ from the raw.
	differing := 0
	for i := 0; i < 50; i++ {
		d, _ := agg.ContributeLocal(uint64(i), 1000, 1000, 4, "public")
		if d.Metrics.BandwidthIn != 1000 {
			differing++
		}
	}
	if differing == 0 {
		t.Fatal("DP noise never perturbed the value")
	}
}

func TestChainLinksEpochs(t *testing.T) {
	agg, _ := NewAggregator(Config{EpochSec: 100, DPEpsilon: -1, KAnon: 1}, []byte("me"))
	// Contribute across three epochs; advancing finalizes the older ones.
	for e := uint64(0); e < 3; e++ {
		eid := EID(e, []byte("x"))
		_ = agg.ApplyDelta(Delta{Epoch: e, EID: eid[:], Metrics: Metrics{Seq: 1}})
		// Force window advance by contributing local at the next epoch.
		_, _ = agg.ContributeLocal(e+1, 0, 0, 0, "public")
	}
	r := agg.Snapshot()
	if r.ChainHead == 0 {
		t.Fatalf("expected finalized epochs in chain, head=%d", r.ChainHead)
	}
	if r.PrevDigest == "" {
		t.Fatal("current epoch missing prev digest link")
	}
}
