package overlay

import (
	"fmt"
	"testing"
	"time"
)

// Closest must return exactly the n nearest, in order, from a table far
// larger than n — the top-K selection must not lose or misplace anyone
// relative to a full sort.
func TestClosestTopKMatchesFullSort(t *testing.T) {
	self := id(0x01)
	table := NewTable(self, 4)
	// Spread contacts across many buckets: ids with high bytes counting up
	// differ from self in ever-later bits, landing in ever-lower buckets.
	const total = 500
	for i := 0; i < total; i++ {
		table.Add(Contact{
			ID:       id(byte(i%256), byte(i>>8), 0x5a, byte(i%7)),
			Addr:     fmt.Sprintf("10.0.0.%d:4001", i%256),
			LastSeen: time.Now(),
		})
	}
	target := id(0x02)
	got := table.Closest(target, 10)
	if len(got) != 10 {
		t.Fatalf("expected 10 closest, got %d", len(got))
	}
	// Reference: full sort of every contact (insertion sort keeps the
	// reference independent of the production comparator wiring).
	all := table.Contacts()
	sorted := append([]Contact(nil), all...)
	for i := 1; i < len(sorted); i++ {
		for j := i; j > 0 && Closer(target, sorted[j].ID, sorted[j-1].ID); j-- {
			sorted[j], sorted[j-1] = sorted[j-1], sorted[j]
		}
	}
	for i, c := range got {
		if c.ID != sorted[i].ID {
			t.Fatalf("position %d: got %v want %v", i, c.ID, sorted[i].ID)
		}
	}
}

// Closest with n covering the whole table returns everything, ordered.
func TestClosestLargeNReturnsWholeTableOrdered(t *testing.T) {
	table := NewTable(id(0x01), 20)
	for i := 0; i < 30; i++ {
		table.Add(Contact{ID: id(byte(i), 0, 0, byte(i)), Addr: "a", LastSeen: time.Now()})
	}
	target := id(0xff)
	got := table.Closest(target, 100)
	if len(got) != 30 {
		t.Fatalf("expected whole table (30), got %d", len(got))
	}
	for i := 1; i < len(got); i++ {
		if !Closer(target, got[i-1].ID, got[i].ID) {
			t.Fatalf("result not ordered by distance at %d", i)
		}
	}
}

// Closest must stay exact under bucket churn: Remove and re-Add of contacts
// between queries must not corrupt the bounded heap's view.
func TestClosestStaysExactAcrossChurn(t *testing.T) {
	table := NewTable(id(0x01), 8)
	ids := make([]NodeID, 0, 60)
	for i := 0; i < 60; i++ {
		cid := id(byte(i), 0x7f, byte(i>>4))
		ids = append(ids, cid)
		table.Add(Contact{ID: cid, Addr: "a", LastSeen: time.Now()})
	}
	target := id(0x80)
	before := table.Closest(target, 5)
	// Drop some, re-add a few new ones, re-add one dropped.
	table.Remove(ids[3])
	table.Remove(ids[10])
	table.Add(Contact{ID: id(0x80, 0x01, 0x02, 0x03), Addr: "b", LastSeen: time.Now()})
	table.Add(Contact{ID: id(0x80, 0x01, 0x02, 0x04), Addr: "b", LastSeen: time.Now()})
	table.Add(Contact{ID: ids[3], Addr: "a", LastSeen: time.Now()})
	after := table.Closest(target, 5)
	if len(after) != 5 {
		t.Fatalf("expected 5, got %d", len(after))
	}
	for i := 1; i < len(after); i++ {
		if !Closer(target, after[i-1].ID, after[i].ID) {
			t.Fatalf("churned result not ordered at %d", i)
		}
	}
	// The new near-target contact must beat the far field.
	if after[0].ID != id(0x80, 0x01, 0x02, 0x03) {
		t.Fatalf("expected nearest to be the added near-target contact, got %v", after[0].ID)
	}
	// A surviving pre-churn top contact must still be found; removed
	// id[10] must never come back.
	for _, c := range after {
		if c.ID == ids[10] {
			t.Fatal("removed contact returned by Closest after churn")
		}
	}
	beforeSet := make(map[NodeID]bool, len(before))
	for _, c := range before {
		beforeSet[c.ID] = true
	}
	present := 0
	for _, c := range after {
		if beforeSet[c.ID] {
			present++
		}
	}
	if present == 0 && len(before) > 0 {
		t.Fatal("expected at least one pre-churn contact to survive in the top-5")
	}
}
