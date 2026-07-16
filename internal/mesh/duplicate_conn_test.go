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

// A datagram session to a symmetric-NAT peer has a dead return path, so it must
// never displace a healthy connection-oriented one; the reliability rule runs
// ahead of the direction rule.
func TestResolveReliabilityPreference(t *testing.T) {
	cases := []struct {
		name             string
		existingReliable bool
		existingHealthy  bool
		newReliable      bool
		wantRes          duplicateResolution
		wantDecided      bool
	}{
		{"healthy TCP is not displaced by UDP", true, true, false, keepExistingSession, true},
		{"stale TCP defers to direction rule", true, false, false, 0, false},
		{"UDP is upgraded to TCP", false, true, true, adoptNewSession, true},
		{"stale UDP is still upgraded to TCP", false, false, true, adoptNewSession, true},
		{"TCP vs TCP defers to direction rule", true, true, true, 0, false},
		{"UDP vs UDP defers to direction rule", false, true, false, 0, false},
	}
	for _, tc := range cases {
		res, decided := resolveReliabilityPreference(tc.existingReliable, tc.existingHealthy, tc.newReliable)
		if decided != tc.wantDecided || (decided && res != tc.wantRes) {
			t.Errorf("%s: got (res=%d, decided=%v), want (res=%d, decided=%v)",
				tc.name, res, decided, tc.wantRes, tc.wantDecided)
		}
	}
}

func TestIsReliableNetwork(t *testing.T) {
	for network, want := range map[string]bool{"tcp": true, "tcp4": true, "tcp6": true, "udp": false, "udp4": false, "": false} {
		if got := isReliableNetwork(network); got != want {
			t.Errorf("isReliableNetwork(%q) = %v, want %v", network, got, want)
		}
	}
}
