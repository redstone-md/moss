package mesh

import (
	"strings"
	"testing"

	"github.com/redstone-md/moss/internal/overlay"
)

// A seed pass is the only thing that fills the routing table, and an empty
// table makes every layer above it report "nobody is there". These assert the
// tally names the gate that actually closed, because the whole point of the
// counters is to tell those cases apart from the outside.
func TestOverlaySeedTallyNamesTheGateThatRejected(t *testing.T) {
	self, ok := overlay.IDFromHex(strings.Repeat("11", overlay.IDLen))
	if !ok {
		t.Fatal("self id")
	}
	node := &Node{overlayTable: overlay.NewTable(self, 0)}

	routable := knownPeer{
		id:              strings.Repeat("ab", overlay.IDLen),
		addr:            "203.0.113.7:4001",
		publicReachable: true,
	}
	cases := []struct {
		name string
		peer knownPeer
		want overlaySeedTally
	}{
		{
			name: "routable contact is added",
			peer: routable,
			want: overlaySeedTally{considered: 1, added: 1},
		},
		{
			name: "a NAT'd peer is not a hop",
			peer: knownPeer{id: routable.id, addr: routable.addr},
			want: overlaySeedTally{considered: 1, notReachable: 1},
		},
		{
			name: "reachable but nowhere to dial",
			peer: knownPeer{id: routable.id, publicReachable: true},
			want: overlaySeedTally{considered: 1, noAddr: 1},
		},
		{
			name: "id is not a point in the keyspace",
			peer: knownPeer{id: "nothex", addr: routable.addr, publicReachable: true},
			want: overlaySeedTally{considered: 1, badID: 1},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var got overlaySeedTally
			node.noteOverlayContact(tc.peer, &got)
			if got != tc.want {
				t.Fatalf("tally = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// The counters have to survive the walk, not just a single call: the field we
// read in telemetry is the sum over every known peer.
func TestOverlaySeedTallyAccumulatesAcrossPeers(t *testing.T) {
	self, ok := overlay.IDFromHex(strings.Repeat("22", overlay.IDLen))
	if !ok {
		t.Fatal("self id")
	}
	node := &Node{
		overlayTable: overlay.NewTable(self, 0),
		knownPeers: map[string]knownPeer{
			"a": {id: strings.Repeat("aa", overlay.IDLen), addr: "203.0.113.7:4001", publicReachable: true},
			"b": {id: strings.Repeat("bb", overlay.IDLen), addr: "203.0.113.8:4001", publicReachable: true},
			"c": {id: strings.Repeat("cc", overlay.IDLen), addr: "203.0.113.9:4001"},
			"d": {id: strings.Repeat("dd", overlay.IDLen), publicReachable: true},
		},
	}

	got := node.overlaySeedFromKnownPeers()
	want := overlaySeedTally{considered: 4, added: 2, notReachable: 1, noAddr: 1}
	if got != want {
		t.Fatalf("tally = %+v, want %+v", got, want)
	}
	if n := node.overlayTable.Len(); n != 2 {
		t.Fatalf("table holds %d contacts, want 2 — added count and table must agree", n)
	}
}
