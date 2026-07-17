package mesh

import "testing"

// Capacity is a ratio, not a count. 8 peers is half-idle at MaxPeers=16 and
// wedged at MaxPeers=8, so the raw counts the fleet already ships cannot answer
// whether the network has room or where it is bottlenecked.
func TestCapacityIsReportedAgainstTheNodesOwnCeilings(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	cfg.MaxPeers = 8
	n, err := NewNode("mesh-capacity", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	fields := map[string]any{}
	n.addCapacityFields(fields, map[string]any{
		"direct_peer_count":   float64(4), // MeshInfoJSON is decoded json: always float64
		"relay_session_count": float64(0),
	})

	if got := fields["peer_capacity_pct"]; got != 50 {
		t.Fatalf("4 of 8 peers should read 50%%: got %v", got)
	}
	if got := fields["max_peers"]; got != 8 {
		t.Fatalf("the denominator must ship too, or the fleet cannot be rolled up: got %v", got)
	}
}

// A node can briefly hold more than its ceiling while an eviction is in flight.
// Reporting 112% reads as a broken metric rather than the truth it is telling.
func TestCapacityClampsAndSurvivesMissingFields(t *testing.T) {
	if got := percentOf(9, 8); got != 100 {
		t.Fatalf("over-ceiling must clamp to 100: got %d", got)
	}
	if got := percentOf(1, 0); got != 0 {
		t.Fatalf("a zero ceiling must not divide by zero: got %d", got)
	}

	cfg := DefaultConfig()
	cfg.Trackers = nil
	n, err := NewNode("mesh-capacity-missing", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	fields := map[string]any{}
	n.addCapacityFields(fields, map[string]any{}) // no counts at all
	if _, ok := fields["peer_capacity_pct"]; ok {
		t.Fatal("a percentage was invented from a count that was not there")
	}
}
