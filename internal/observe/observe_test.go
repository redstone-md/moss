package observe

import "testing"

func TestVerifyContinuityAcceptsUnbrokenChain(t *testing.T) {
	points := []EpochPoint{
		{Epoch: 10, Digest: "a", Prev: "g"},
		{Epoch: 11, Digest: "b", Prev: "a"},
		{Epoch: 12, Digest: "c", Prev: "b"},
	}
	if err := VerifyContinuity(points); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestVerifyContinuityRejectsBrokenLink(t *testing.T) {
	points := []EpochPoint{
		{Epoch: 10, Digest: "a", Prev: "g"},
		{Epoch: 11, Digest: "b", Prev: "TAMPERED"},
	}
	if err := VerifyContinuity(points); err == nil {
		t.Fatal("expected broken-link error")
	}
}

func TestVerifyContinuityRejectsGap(t *testing.T) {
	points := []EpochPoint{
		{Epoch: 10, Digest: "a", Prev: "g"},
		{Epoch: 12, Digest: "c", Prev: "a"},
	}
	if err := VerifyContinuity(points); err == nil {
		t.Fatal("expected gap error")
	}
}

func TestCrossCheckDetectsDisagreement(t *testing.T) {
	byGateway := map[string][]EpochPoint{
		"gw1": {{Epoch: 5, Digest: "x"}, {Epoch: 6, Digest: "y"}},
		"gw2": {{Epoch: 5, Digest: "x"}, {Epoch: 6, Digest: "DIFFERENT"}},
	}
	agree := CrossCheck(byGateway)
	if !agree[5] {
		t.Fatal("epoch 5 should agree")
	}
	if agree[6] {
		t.Fatal("epoch 6 should be flagged as disagreement")
	}
}

func TestSimulateTreeDeterministic(t *testing.T) {
	params := TreeParams{
		Seed:       "deadbeefdigest",
		NodeCount:  120,
		NATHist:    map[string]int{"public": 10, "symmetric_nat": 30, "cgnat": 20},
		DegreeHist: map[string]int{"1-2": 10, "3-5": 20},
	}
	a := SimulateTree(params)
	b := SimulateTree(params)
	if len(a) != len(b) {
		t.Fatalf("non-deterministic length: %d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("non-deterministic node %d: %+v vs %+v", i, a[i], b[i])
		}
	}
}

func TestSimulateTreeStructure(t *testing.T) {
	nodes := SimulateTree(TreeParams{
		Seed:      "seed1",
		NodeCount: 100,
		NATHist:   map[string]int{"public": 20, "cgnat": 80},
	})
	if len(nodes) != 100 {
		t.Fatalf("want 100 nodes, got %d", len(nodes))
	}
	roots, supers := 0, 0
	for _, n := range nodes {
		if n.Parent == -1 {
			roots++
			if n.Kind != "supernode" {
				t.Fatalf("root must be supernode, got %q", n.Kind)
			}
		}
		if n.Kind == "supernode" {
			supers++
		}
		// Every non-root parent must reference an earlier, valid node id.
		if n.Parent != -1 && (n.Parent < 0 || n.Parent >= n.ID) {
			t.Fatalf("node %d has invalid parent %d", n.ID, n.Parent)
		}
	}
	if roots == 0 || roots != supers {
		t.Fatalf("expected all supernodes to be roots: roots=%d supers=%d", roots, supers)
	}
}

func TestSimulateTreeCapsRendering(t *testing.T) {
	nodes := SimulateTree(TreeParams{Seed: "s", NodeCount: 100000, MaxRender: 50, NATHist: map[string]int{"public": 1}})
	if len(nodes) != 50 {
		t.Fatalf("expected render cap of 50, got %d", len(nodes))
	}
}

func TestSimulateTreeEmpty(t *testing.T) {
	if got := SimulateTree(TreeParams{Seed: "s", NodeCount: 0}); len(got) != 0 {
		t.Fatalf("expected empty tree, got %d", len(got))
	}
}
