package meshbridge

import (
	"testing"
	"time"
)

// TestTableRegisterLookup proves the core mapping: a registered pair
// reads back exactly, and an unknown node number is a miss.
func TestTableRegisterLookup(t *testing.T) {
	table := NewMbsTable()
	if _, ok := table.Lookup(0x42); ok {
		t.Fatal("empty table: lookup hit")
	}
	table.Register(0x42, "cafe")
	peerID, ok := table.Lookup(0x42)
	if !ok || peerID != "cafe" {
		t.Fatalf("Lookup(0x42): got (%q, %v)", peerID, ok)
	}
	if _, ok := table.Lookup(0x43); ok {
		t.Fatal("unregistered node number hit")
	}
}

// TestTableRegisterEmptyPeerIDIgnored proves the no-op: an empty
// peerID carries no address claim, so it leaves the table unchanged.
func TestTableRegisterEmptyPeerIDIgnored(t *testing.T) {
	table := NewMbsTable()
	table.Register(1, "")
	if table.Len() != 0 {
		t.Fatalf("empty peerID created a row: len=%d", table.Len())
	}
	if _, ok := table.Lookup(1); ok {
		t.Fatal("empty peerID row lookup hit")
	}
}

// TestTableRegisterOverwrites proves the re-registration contract: an
// existing node number's peerID claim is replaced by the fresh one.
func TestTableRegisterOverwrites(t *testing.T) {
	table := NewMbsTable()
	table.Register(7, "old")
	table.Register(7, "new")
	peerID, ok := table.Lookup(7)
	if !ok || peerID != "new" {
		t.Fatalf("re-registration: got (%q, %v), want new", peerID, ok)
	}
	if table.Len() != 1 {
		t.Fatalf("re-registration grew the table: len=%d", table.Len())
	}
}

// TestTableUnregister proves removal and the missing no-op.
func TestTableUnregister(t *testing.T) {
	table := NewMbsTable()
	table.Unregister(99) // missing: no-op, no panic
	table.Register(99, "peer-99")
	table.Unregister(99)
	if _, ok := table.Lookup(99); ok {
		t.Fatal("unregistered row survived")
	}
	if table.Len() != 0 {
		t.Fatalf("unregister left rows: len=%d", table.Len())
	}
	table.Unregister(99) // second unregister: still a no-op
}

// TestTableTouchRefreshesLiveness proves the keepalive path: Touch on
// a registered row defers the sweep, Touch on an unknown node number
// changes nothing.
func TestTableTouchRefreshesLiveness(t *testing.T) {
	table := NewMbsTable()
	table.Register(5, "peer-5")

	ttl := time.Minute
	// Registered now: at now+ttl+1s the row is stale and swept.
	if dropped := table.Sweep(time.Now().Add(ttl+time.Second), ttl); dropped != 1 {
		t.Fatalf("stale row not swept: dropped=%d", dropped)
	}

	// Touch an unknown node: no row appears, nothing breaks.
	table.Touch(0xDEAD)
	if table.Len() != 0 {
		t.Fatalf("Touch on unknown created a row: len=%d", table.Len())
	}

	// The touch contract, with a control. Register/Touch take no clock
	// parameter (the pump contract is wall-clock liveness), so the gap
	// between the control's deadline and the touched row's is built
	// with a real sleep; the margins on both sides dwarf it.
	start := time.Now()
	table.Register(6, "peer-6")
	table.Register(7, "peer-7-control")
	gap := time.Second
	time.Sleep(gap)
	table.Touch(6)
	// Sweep between the two deadlines, anchored to the register time:
	// start+ttl+500ms is past the control's deadline by 500ms and
	// inside the touched row's by the same margin.
	sweepAt := start.Add(ttl + gap/2)
	if dropped := table.Sweep(sweepAt, ttl); dropped != 1 {
		t.Fatalf("sweep between deadlines: dropped=%d, want 1 (control only)", dropped)
	}
	if _, ok := table.Lookup(6); !ok {
		t.Fatal("touched row evicted by the sweep")
	}
	if _, ok := table.Lookup(7); ok {
		t.Fatal("untouched control survived the sweep")
	}
}

// TestTableSweepBoundary pins the strict-expiry edge: a row exactly at
// its budget survives the sweep; one moment past it does not.
func TestTableSweepBoundary(t *testing.T) {
	table := NewMbsTable()
	table.Register(11, "peer-11")
	ttl := time.Minute
	// Re-register to pin lastSeen to a known "now".
	start := time.Now()
	table.Register(11, "peer-11")

	// Exactly at the budget: lastSeen + ttl — strictly inside, survives.
	if dropped := table.Sweep(start.Add(ttl), ttl); dropped != 0 {
		t.Fatalf("row at its exact budget swept: dropped=%d", dropped)
	}
	// One moment past: swept.
	if dropped := table.Sweep(start.Add(ttl+50*time.Millisecond), ttl); dropped != 1 {
		t.Fatalf("row past its budget not swept: dropped=%d", dropped)
	}
	if table.Len() != 0 {
		t.Fatalf("swept row still present: len=%d", table.Len())
	}
}

// TestTableLen proves the count across the write paths: register adds,
// unregister and sweep remove.
func TestTableLen(t *testing.T) {
	table := NewMbsTable()
	if table.Len() != 0 {
		t.Fatalf("empty table: len=%d", table.Len())
	}
	for i := uint32(1); i <= 3; i++ {
		table.Register(i, "peer")
	}
	if table.Len() != 3 {
		t.Fatalf("after registers: len=%d, want 3", table.Len())
	}
	table.Unregister(2)
	if table.Len() != 2 {
		t.Fatalf("after unregister: len=%d, want 2", table.Len())
	}
	// Live rows: sweeping at a point inside the liveness budget evicts
	// nothing.
	if dropped := table.Sweep(time.Now().Add(30*time.Second), time.Minute); dropped != 0 {
		t.Fatalf("live rows swept: dropped=%d", dropped)
	}
	if table.Len() != 2 {
		t.Fatalf("live sweep changed len: %d", table.Len())
	}
}
