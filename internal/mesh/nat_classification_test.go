package mesh

import (
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/nat"
)

// The bug the fleet telemetry exposed: every event carried observations=1, so
// the profiler was always handed a single vantage point. Symmetric NAT is
// defined by the mapped port differing per destination — with one sample there
// is nothing to differ from, so the profile could only ever stay "unknown", and
// a node that never learns it is symmetric keeps punching at peers it can never
// reach. These pin the profiler's contract that the fix depends on.
func TestProfilerNeedsTwoVantagePointsToClassify(t *testing.T) {
	p := nat.NewProfiler()
	base := nat.Profile{Type: nat.TypeUnknown}

	// One observation: nothing to compare, so nothing may be concluded.
	got := p.WithBindingObservations(base, []string{"203.0.113.7:41666"})
	if got.Type != nat.TypeUnknown {
		t.Fatalf("Type = %v from a single observation; one vantage point cannot classify a NAT", got.Type)
	}

	// Two destinations, different mapped ports: that IS symmetric NAT.
	got = p.WithBindingObservations(base, []string{"203.0.113.7:43364", "203.0.113.7:52010"})
	if got.Type != nat.TypeSymmetric {
		t.Fatalf("Type = %v, want symmetric_nat: a mapping that differs per destination is the definition", got.Type)
	}
	if got.PublicReachable {
		t.Error("a symmetric-NAT node must not be considered publicly reachable")
	}

	// Two destinations, same mapped port: not symmetric, and now knowable.
	got = p.WithBindingObservations(base, []string{"203.0.113.7:41666", "203.0.113.7:41666"})
	if got.Type == nat.TypeSymmetric {
		t.Fatal("a stable mapping across destinations must not be classified symmetric")
	}
	if got.Type == nat.TypeUnknown {
		t.Fatal("two agreeing vantage points are enough to leave unknown behind")
	}
}

// A public node must not be demoted by the comparison: its port is stable, so
// the classifier has to leave it alone. Getting this wrong would strip the
// relay fleet of its role.
func TestClassificationDoesNotDemoteAPublicNode(t *testing.T) {
	p := nat.NewProfiler()
	public := nat.Profile{Type: nat.TypePublic, PublicReachable: true}
	got := p.WithBindingObservations(public, []string{"203.0.113.7:4001", "203.0.113.7:4001"})
	if got.Type != nat.TypePublic || !got.PublicReachable {
		t.Fatalf("public node became %v (reachable=%v); a stable mapping must not demote it",
			got.Type, got.PublicReachable)
	}
}

// refreshNATClassification must be inert rather than wrong when it cannot get a
// second look — silence is correct here, a guess is not.
func TestRefreshNATClassificationNeedsASecondSample(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil // shouldUseSTUNBootstrap is false without a public tracker
	cfg.LANDiscoveryEnabled = false
	cfg.DHTEnabled = false
	n, err := NewNode("room", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	if n.refreshNATClassification(time.Millisecond) {
		t.Fatal("classification must not claim a result it could not sample")
	}
	if got := n.NATType(); got != string(nat.TypeUnknown) {
		t.Fatalf("nat_type = %q; with no samples it must stay unknown rather than guess", got)
	}
}

// The gossip binding reply is built from the observed host plus the port the
// asker advertised about itself — the right answer to "what is my dialable
// address", and a non-answer to "what is my mapping". It is the same value no
// matter who replies, so letting it reach the classifier pinned the fleet at
// observations=1 and nat_type=unknown: appendObservation collapsed every
// identical echo into one entry and left nothing to compare.
func TestSyntheticObservationNeverInformsClassification(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	cfg.LANDiscoveryEnabled = false
	cfg.DHTEnabled = false
	n, err := NewNode("room", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	deadline := time.Now().Add(time.Second)

	// Two peers echo our own port back at us — as the gossip path always does.
	n.applySyntheticObservation("203.0.113.7:41666", deadline)
	n.applySyntheticObservation("203.0.113.7:41666", deadline)

	n.mu.RLock()
	history := len(n.bindingHistory)
	n.mu.RUnlock()
	if history != 0 {
		t.Fatalf("bindingHistory = %d entries; an echo of our own port is not evidence about our NAT and must never reach the classifier", history)
	}
}

// A genuine observation — STUN or a peer's UDP observe — must still inform it,
// or classification loses its only real input.
func TestGenuineObservationDoesInformClassification(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	cfg.LANDiscoveryEnabled = false
	cfg.DHTEnabled = false
	n, err := NewNode("room", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	deadline := time.Now().Add(time.Second)

	n.applyExternalObservation("203.0.113.7:43364", deadline)
	n.applyExternalObservation("203.0.113.7:52010", deadline)

	n.mu.RLock()
	history := append([]string(nil), n.bindingHistory...)
	n.mu.RUnlock()
	if len(history) != 2 {
		t.Fatalf("bindingHistory = %v, want both genuine observations kept so the ports can be compared", history)
	}
	if got := n.NATType(); got != string(nat.TypeSymmetric) {
		t.Fatalf("nat_type = %q, want symmetric_nat: two destinations saw different mapped ports", got)
	}
}
