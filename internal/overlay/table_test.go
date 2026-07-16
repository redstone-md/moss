package overlay

import (
	"testing"
	"time"
)

// id builds a NodeID from a big-endian prefix; the rest is zero.
func id(prefix ...byte) NodeID {
	var n NodeID
	copy(n[:], prefix)
	return n
}

func TestBucketIndex(t *testing.T) {
	self := id(0x00)
	cases := []struct {
		name  string
		other NodeID
		want  int
	}{
		// 0x80 differs in the very first bit => farthest bucket.
		{"top bit differs", id(0x80), IDBits - 1},
		// 0x40 differs in the second bit.
		{"second bit differs", id(0x40), IDBits - 2},
		// 0x01 differs in the 8th bit of the first byte.
		{"eighth bit differs", id(0x01), IDBits - 8},
		// Differing only in the last byte's last bit => bucket 0.
		{"last bit differs", func() NodeID { n := id(); n[IDLen-1] = 0x01; return n }(), 0},
		{"identical has no bucket", self, -1},
	}
	for _, tc := range cases {
		if got := BucketIndex(self, tc.other); got != tc.want {
			t.Errorf("%s: BucketIndex = %d, want %d", tc.name, got, tc.want)
		}
	}
}

func TestBucketIndexIsSymmetric(t *testing.T) {
	a, b := id(0x12, 0x34), id(0xab, 0xcd)
	if BucketIndex(a, b) != BucketIndex(b, a) {
		t.Fatal("XOR distance must be symmetric")
	}
}

func TestCloserOrdersByXor(t *testing.T) {
	target := id(0x00)
	near, far := id(0x01), id(0x80)
	if !Closer(target, near, far) {
		t.Fatal("0x01 must be closer to 0x00 than 0x80 is")
	}
	if Closer(target, far, near) {
		t.Fatal("Closer must not be true in both directions")
	}
}

func TestAddRefreshesAndDoesNotDuplicate(t *testing.T) {
	tbl := NewTable(id(0x00), 20)
	c := Contact{ID: id(0x80), Addr: "1.1.1.1:1"}
	tbl.Add(c)
	c.Addr = "2.2.2.2:2"
	tbl.Add(c)
	if tbl.Len() != 1 {
		t.Fatalf("Len = %d, want 1 (re-add must refresh, not duplicate)", tbl.Len())
	}
	if got := tbl.Closest(id(0x80), 1); got[0].Addr != "2.2.2.2:2" {
		t.Fatalf("Addr = %q, want the refreshed value", got[0].Addr)
	}
}

func TestAddIgnoresSelf(t *testing.T) {
	self := id(0x42)
	tbl := NewTable(self, 20)
	tbl.Add(Contact{ID: self, Addr: "1.1.1.1:1"})
	if tbl.Len() != 0 {
		t.Fatal("a node must never be its own contact")
	}
}

func TestBucketEvictsLeastRecentlySeenWhenFull(t *testing.T) {
	tbl := NewTable(id(0x00), 2)
	// All three share bucket IDBits-1 (top bit set) so they contend.
	a := Contact{ID: id(0x80, 0x01), Addr: "a", LastSeen: time.Now()}
	b := Contact{ID: id(0x80, 0x02), Addr: "b", LastSeen: time.Now()}
	c := Contact{ID: id(0x80, 0x03), Addr: "c", LastSeen: time.Now()}
	tbl.Add(a)
	tbl.Add(b)
	tbl.Add(c) // evicts a, the least recently seen
	if tbl.Len() != 2 {
		t.Fatalf("Len = %d, want 2 (k=2)", tbl.Len())
	}
	for _, got := range tbl.Contacts() {
		if got.ID == a.ID {
			t.Fatal("the least recently seen contact must be evicted when the bucket is full")
		}
	}
}

func TestRemove(t *testing.T) {
	tbl := NewTable(id(0x00), 20)
	tbl.Add(Contact{ID: id(0x80), Addr: "a"})
	tbl.Remove(id(0x80))
	if tbl.Len() != 0 {
		t.Fatal("Remove must drop the contact")
	}
	tbl.Remove(id(0x80)) // removing a missing contact must not panic
}

func TestClosestOrdersByDistanceToTarget(t *testing.T) {
	tbl := NewTable(id(0x00), 20)
	tbl.Add(Contact{ID: id(0x80), Addr: "far"})
	tbl.Add(Contact{ID: id(0x01), Addr: "near"})
	tbl.Add(Contact{ID: id(0x40), Addr: "mid"})
	got := tbl.Closest(id(0x00), 3)
	want := []string{"near", "mid", "far"}
	for i := range want {
		if got[i].Addr != want[i] {
			t.Fatalf("Closest[%d] = %q, want %q (order: %v)", i, got[i].Addr, want[i], addrs(got))
		}
	}
}

func TestClosestCapsAtN(t *testing.T) {
	tbl := NewTable(id(0x00), 20)
	for i := 1; i <= 5; i++ {
		tbl.Add(Contact{ID: id(byte(i)), Addr: "x"})
	}
	if got := tbl.Closest(id(0x00), 2); len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}
	if got := tbl.Closest(id(0x00), 0); got != nil {
		t.Fatal("Closest(_, 0) must return nil")
	}
}

// The whole point of using Kademlia at any scale: with a handful of core nodes
// the table simply returns all of them, which IS a full mesh — no special case,
// and the same code path serves a large network.
func TestSmallCoreDegradesToFullMesh(t *testing.T) {
	tbl := NewTable(id(0x00), 20)
	const core = 6
	for i := 1; i <= core; i++ {
		tbl.Add(Contact{ID: id(byte(i * 0x10)), Addr: "box"})
	}
	got := tbl.Closest(id(0xff), DefaultK)
	if len(got) != core {
		t.Fatalf("Closest returned %d of %d core nodes; a small core must resolve to every node (full mesh)", len(got), core)
	}
}

func TestIDFromHexRoundTrip(t *testing.T) {
	orig := id(0xde, 0xad, 0xbe, 0xef)
	got, ok := IDFromHex(orig.String())
	if !ok || got != orig {
		t.Fatalf("round trip failed: ok=%v got=%s want=%s", ok, got, orig)
	}
	if _, ok := IDFromHex("not-hex"); ok {
		t.Error("non-hex must be rejected")
	}
	if _, ok := IDFromHex("abcd"); ok {
		t.Error("wrong-length hex must be rejected")
	}
}

func addrs(cs []Contact) []string {
	out := make([]string, len(cs))
	for i, c := range cs {
		out[i] = c.Addr
	}
	return out
}
