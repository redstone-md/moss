package mesh

import "testing"

// After a NAT rebind the peer reconnects in the same direction from a new
// source port; the fresh connection must win over a stale (ping-missing)
// entry, while a healthy duplicate still follows the direction rule.
func TestYieldsToNewConnection(t *testing.T) {
	const local = "aa"
	const remote = "bb" // local < remote: the direction rule wants our outbound

	cases := []struct {
		name        string
		existing    *peerConn
		newOutbound bool
		want        bool
	}{
		{"healthy same-direction duplicate is kept", &peerConn{id: remote, outbound: false}, false, false},
		{"stale same-direction duplicate yields", &peerConn{id: remote, outbound: false, pingMisses: 1}, false, true},
		{"stale opposite-direction duplicate yields regardless of rule", &peerConn{id: remote, outbound: true, pingMisses: 3}, false, true},
		{"healthy opposite-direction follows rule: favored new wins", &peerConn{id: remote, outbound: false}, true, true},
		{"healthy opposite-direction follows rule: disfavored new loses", &peerConn{id: remote, outbound: true}, false, false},
	}
	for _, tc := range cases {
		if got := yieldsToNewConnection(local, tc.existing, tc.newOutbound); got != tc.want {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
	}
}
