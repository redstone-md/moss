package mesh

import (
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/nat"
)

// The punch counters and the coordination retry exist to make punch outcomes
// observable per NAT layer, not just per node. These tests stub a node that
// never started (no listener, so every send is a silent false) and verify the
// counters advance exactly as the policy runs.

func newNatReliabilityTestNode(t *testing.T, room string) *Node {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Trackers = nil
	cfg.LANDiscoveryEnabled = false
	cfg.DHTEnabled = false
	n, err := NewNode(room, nil, cfg)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	return n
}

// addTrustedRelayCandidate registers a peer that passes both the relay-candidate
// trust gate and the reachability-probe gate on an unstarted node.
func addTrustedRelayCandidate(n *Node, peerID string) {
	n.mu.Lock()
	n.peers[peerID] = &peerConn{id: peerID, addr: "192.0.2.10:40000"}
	n.knownPeers[peerID] = knownPeer{
		id:              peerID,
		addr:            "192.0.2.10:40000",
		natTrusted:      true,
		relayCapable:    true,
		publicReachable: true,
	}
	n.mu.Unlock()
}

func TestPunchCountsAttemptAndTimeout(t *testing.T) {
	n := newNatReliabilityTestNode(t, "nat-punch-timeout")
	defer n.Stop()
	target := "target-peer"
	via := "relay-peer"
	addTrustedRelayCandidate(n, via)
	n.mu.Lock()
	n.knownPeers[target] = knownPeer{id: target, addr: "203.0.113.9:45000"}
	n.mu.Unlock()

	if n.attemptHolePunch(target, 600*time.Millisecond) {
		t.Fatal("expected punch to fail: target session is nil so no connection can land")
	}
	if got := counterValue(n, "__punch_attempt__"); got != 1 {
		t.Fatalf("__punch_attempt__ = %d, want 1", got)
	}
	if got := counterValue(n, "__punch_timeout__"); got != 1 {
		t.Fatalf("__punch_timeout__ = %d, want 1", got)
	}
	if got := counterValue(n, "__punch_success__"); got != 0 {
		t.Fatalf("__punch_success__ = %d, want 0", got)
	}
	// The first coordination retry is scheduled at start+1500ms, beyond the
	// 600ms deadline: no retry may fire.
	if got := counterValue(n, "__punch_coord_retry__"); got != 0 {
		t.Fatalf("__punch_coord_retry__ = %d, want 0 for a punch that ended before any grace expiry", got)
	}
}

func TestPunchCoordRetryFiresAfterGrace(t *testing.T) {
	n := newNatReliabilityTestNode(t, "nat-punch-coord-retry")
	defer n.Stop()
	target := "target-peer"
	via := "relay-peer"
	addTrustedRelayCandidate(n, via)
	n.mu.Lock()
	n.knownPeers[target] = knownPeer{id: target, addr: "203.0.113.9:45000"}
	n.mu.Unlock()

	if n.attemptHolePunch(target, 2500*time.Millisecond) {
		t.Fatal("expected punch to fail: target session is nil so no connection can land")
	}
	if got := counterValue(n, "__punch_coord_retry__"); got < 1 {
		t.Fatalf("__punch_coord_retry__ = %d, want >= 1: the offer must be re-sent once the coordination grace expired", got)
	}
	if got := counterValue(n, "__punch_coord_retry__"); got > 2 {
		t.Fatalf("__punch_coord_retry__ = %d, want <= 2 per punch attempt", got)
	}
}

func TestPunchCountsSuccess(t *testing.T) {
	n := newNatReliabilityTestNode(t, "nat-punch-success")
	defer n.Stop()
	target := "target-peer"
	via := "relay-peer"
	addTrustedRelayCandidate(n, via)
	n.mu.Lock()
	n.knownPeers[target] = knownPeer{id: target, addr: "203.0.113.9:45000"}
	// A direct, non-relayed peer counts as connected: the poll loop's first
	// iteration must report success.
	n.peers[target] = &peerConn{id: target, addr: "203.0.113.9:45000"}
	n.mu.Unlock()

	if !n.attemptHolePunch(target, 600*time.Millisecond) {
		t.Fatal("expected punch to succeed: the target counts as a direct peer")
	}
	if got := counterValue(n, "__punch_attempt__"); got != 1 {
		t.Fatalf("__punch_attempt__ = %d, want 1", got)
	}
	if got := counterValue(n, "__punch_success__"); got != 1 {
		t.Fatalf("__punch_success__ = %d, want 1", got)
	}
	if got := counterValue(n, "__punch_timeout__"); got != 0 {
		t.Fatalf("__punch_timeout__ = %d, want 0", got)
	}
}

// The parallel confirmation must not spend counters when it has nothing to
// probe with.
func TestConfirmReachabilityParallelWithoutPeers(t *testing.T) {
	n := newNatReliabilityTestNode(t, "nat-reach-no-peers")
	defer n.Stop()
	if n.confirmReachabilityParallel("203.0.113.7:41666", time.Now().Add(time.Second)) {
		t.Fatal("expected no probe peers to mean no confirmation")
	}
	if got := counterValue(n, "__reach_confirm_attempt__"); got != 0 {
		t.Fatalf("__reach_confirm_attempt__ = %d, want 0", got)
	}
}

// A bootstrap stub peer passes the probe gate; with no session its probe never
// gets a reply, so the parallel confirmation must time out and count it.
func TestConfirmReachabilityParallelTimesOutWithStubPeer(t *testing.T) {
	n := newNatReliabilityTestNode(t, "nat-reach-stub")
	defer n.Stop()
	peerID := "probe-stub-peer"
	n.mu.Lock()
	n.peers[peerID] = &peerConn{id: peerID, addr: "192.0.2.10:40000", bootstrap: true}
	n.mu.Unlock()

	if n.confirmReachabilityParallel("203.0.113.7:41666", time.Now().Add(1200*time.Millisecond)) {
		t.Fatal("expected stub peer with no session to leave reachability unconfirmed")
	}
	if got := counterValue(n, "__reach_confirm_attempt__"); got != 1 {
		t.Fatalf("__reach_confirm_attempt__ = %d, want 1", got)
	}
	if got := counterValue(n, "__reach_confirm_timeout__"); got != 1 {
		t.Fatalf("__reach_confirm_timeout__ = %d, want 1", got)
	}
}

// shouldRecheckPublicReachability is the anti-flap gate: only a symmetric
// verdict after an already-confirmed node may trigger a re-check. Everything
// else must keep the old behaviour.
func TestShouldRecheckPublicReachabilityMatrix(t *testing.T) {
	cases := []struct {
		name     string
		previous nat.Profile
		profile  nat.Profile
		want     bool
	}{
		{
			name:     "confirmed node goes symmetric",
			previous: nat.Profile{Type: nat.TypePublic, PublicReachable: true},
			profile:  nat.Profile{Type: nat.TypeSymmetric, PublicReachable: false},
			want:     true,
		},
		{
			name:     "confirmed node keeps cone verdict",
			previous: nat.Profile{Type: nat.TypePublic, PublicReachable: true},
			profile:  nat.Profile{Type: nat.TypePortRestricted, PublicReachable: false},
			want:     false,
		},
		{
			name:     "unconfirmed node goes symmetric",
			previous: nat.Profile{Type: nat.TypeUnknown, PublicReachable: false},
			profile:  nat.Profile{Type: nat.TypeSymmetric, PublicReachable: false},
			want:     false,
		},
		{
			name:     "symmetric verdict keeps reachability",
			previous: nat.Profile{Type: nat.TypeSymmetric, PublicReachable: true},
			profile:  nat.Profile{Type: nat.TypeSymmetric, PublicReachable: true},
			want:     false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := shouldRecheckPublicReachability(tc.previous, tc.profile); got != tc.want {
				t.Fatalf("shouldRecheckPublicReachability(%v, %v) = %t, want %t",
					tc.previous, tc.profile, got, tc.want)
			}
		})
	}
}

// The sticky path: a confirmed node that turns symmetric with no probe peers
// must keep its confirmed reachability rather than degrading to unreachable.
func TestReachabilityRecheckStickyWithoutProbePeers(t *testing.T) {
	n := newNatReliabilityTestNode(t, "nat-recheck-sticky")
	defer n.Stop()
	n.natProfile.Store(nat.Profile{Type: nat.TypePublic, PublicReachable: true})
	// One stable mapping already recorded; the new observation differs —
	// the classifier will call it symmetric on the window.
	n.mu.Lock()
	n.bindingHistory = appendBindingSample(n.bindingHistory, "203.0.113.7:41666")
	n.mu.Unlock()

	n.applyExternalObservation("203.0.113.7:52010", time.Now().Add(time.Second))

	profile := n.natProfile.Load().(nat.Profile)
	if got := counterValue(n, "__reach_recheck_attempt__"); got != 1 {
		t.Fatalf("__reach_recheck_attempt__ = %d, want 1", got)
	}
	if got := counterValue(n, "__reach_recheck_sticky__"); got != 1 {
		t.Fatalf("__reach_recheck_sticky__ = %d, want 1", got)
	}
	if !profile.PublicReachable {
		t.Fatal("a confirmed node must keep PublicReachable through symmetric jitter with no probe peers")
	}
	if profile.Type != nat.TypeSymmetric {
		t.Fatalf("nat_type = %v, want symmetric_nat: the evidence stays symmetric", profile.Type)
	}
}

// The raw binding history must keep consecutive duplicates. With the old
// collapsing writer a node whose mappings had stabilised could never produce
// "the last three mappings were identical", and that evidence is what lets
// the classifier rule symmetric OUT for a fresh window (three agreeing ports
// never classify symmetric), keeping a once-confirmed node from flapping on
// stale symmetric history.
func TestBindingHistoryKeepsRawDuplicates(t *testing.T) {
	history := []string{}
	history = appendBindingSample(history, "203.0.113.7:41000")
	history = appendBindingSample(history, "203.0.113.7:41000")
	history = appendBindingSample(history, "203.0.113.7:41000")
	if len(history) != 3 {
		t.Fatalf("history = %v, want raw duplicates kept so a settled cone can be seen", history)
	}
	p := nat.NewProfiler()
	// Three agreeing ports from a fresh window must not classify symmetric.
	got := p.WithBindingObservations(nat.Profile{Type: nat.TypeUnknown}, history)
	if got.Type == nat.TypeSymmetric {
		t.Fatalf("three identical mappings classified symmetric (%v); agreeing ports are not symmetric", got.Type)
	}
	// And the raw history reaching the classifier is the whole point: a
	// collapsed history of one entry could never show agreement at all.
	if got2 := p.WithBindingObservations(nat.Profile{Type: nat.TypeUnknown}, history[:1]); got2.Type != nat.TypeUnknown {
		t.Fatalf("a single sample classified %v; one vantage point can never classify", got2.Type)
	}
}

// appendBindingSample must refuse empty observations and cap the history.
func TestAppendBindingSampleCapsHistory(t *testing.T) {
	history := []string{}
	history = appendBindingSample(history, "")
	if len(history) != 0 {
		t.Fatal("empty observation must be ignored")
	}
	for i := 0; i < bindingSampleHistoryCap+4; i++ {
		history = appendBindingSample(history, netip.MustParseAddrPort("203.0.113.7:41000").String()+time.Duration(i).String())
	}
	if len(history) > bindingSampleHistoryCap {
		t.Fatalf("history len = %d, want <= %d", len(history), bindingSampleHistoryCap)
	}
}

// On a multi-homed box, the advertised host must be the interface that hosts
// the peers actually connecting to us.
func TestSelectAdvertiseHostForPeers(t *testing.T) {
	eth0 := net.Interface{Name: "eth0", Flags: net.FlagUp}
	eth1 := net.Interface{Name: "eth1", Flags: net.FlagUp}
	addrs := map[string][]net.Addr{
		"eth0": {&net.IPNet{IP: net.ParseIP("192.168.1.42"), Mask: net.CIDRMask(24, 32)}},
		"eth1": {&net.IPNet{IP: net.ParseIP("10.0.0.42"), Mask: net.CIDRMask(24, 32)}},
	}
	addrFn := func(iface net.Interface) ([]net.Addr, error) { return addrs[iface.Name], nil }

	// A peer on the 192.168.1.0/24 subnet: eth0 is the dialable interface.
	host, ok := selectAdvertiseHostForPeersFunc([]net.Interface{eth0, eth1}, addrFn,
		[]netip.Addr{netip.MustParseAddr("192.168.1.10")})
	if !ok {
		t.Fatal("expected the peer-subnet interface to win")
	}
	if got := host; got != "192.168.1.42" {
		t.Fatalf("host = %s, want 192.168.1.42", got)
	}

	// No peer on any local subnet: the evidence must not override the default.
	if _, ok := selectAdvertiseHostForPeersFunc([]net.Interface{eth0, eth1}, addrFn,
		[]netip.Addr{netip.MustParseAddr("198.51.100.10")}); ok {
		t.Fatal("a match set of zero must not produce an advertise host")
	}

	// Two interfaces tie at one peer each: ambiguous evidence must fall back.
	if _, ok := selectAdvertiseHostForPeersFunc([]net.Interface{eth0, eth1}, addrFn,
		[]netip.Addr{netip.MustParseAddr("192.168.1.10"), netip.MustParseAddr("10.0.0.10")}); ok {
		t.Fatal("a tie at the top must fall back to the generic preference")
	}
}

// The overlay blacklist must cover the modern VPN interfaces the fleet
// actually runs, and stay silent on physical names.
func TestModernVPNInterfaceNamesAreOverlay(t *testing.T) {
	for _, name := range []string{
		"netbird0",
		"nordlynx",
		"mullvad-vpn",
		"protonvpn0",
		"CloudflareWARP",
		"ppp0",
		"xfrm0",
		"ipsec0",
		"zt7abc123",
	} {
		if !isVirtualOverlayInterfaceName(name) {
			t.Fatalf("expected %q to be treated as virtual overlay interface", name)
		}
	}
	for _, name := range []string{"eth0", "en0", "wlan0", "Wi-Fi", "Ethernet"} {
		if isVirtualOverlayInterfaceName(name) {
			t.Fatalf("expected %q to remain eligible as a normal interface", name)
		}
	}
}
