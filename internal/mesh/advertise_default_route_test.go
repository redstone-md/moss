package mesh

// Bug №5: a public box advertised its docker bridge. The generic
// private-before-global preference (selectAdvertiseHost) is deliberate for
// LAN pairs, but a host with a default route out a public interface must not
// hand a container-bridge address to the fleet: nobody off-box can route to
// 172.26.0.1. The census caught exactly that on a public stand box —
// advertised_addr 172.26.0.1:43155 at t+0 — and the same address fed the NAT profile, so two
// directly public hosts classified as port_restricted_cone.
//
// Three gates fix it, each pinned here on the probe seam (fabricated routes,
// no real network):
//
//   - the source address of the default route leads the advertise for a
//     public host, ahead of any interface preference;
//   - a non-global source (private LAN behind a router, CGNAT, loopback)
//     is rejected, so the old local/LAN chain stays in charge;
//   - an observation whose host IS that egress address is no NAT evidence:
//     the far side saw the endpoint our socket sits on, so it must not
//     classify a mapping — that fold is what pinned public hosts at
//     port_restricted_cone forever.

import (
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/nat"
)

// fakeDefaultRouteEgress overrides the default-route probe for one test.
// The production probe is a real connected-UDP dial (see
// defaultRouteAdvertiseHostFn); the seam keeps these units hermetic.
func fakeDefaultRouteEgress(t *testing.T, host string, ok bool) {
	t.Helper()
	previous := defaultRouteAdvertiseHostFn
	defaultRouteAdvertiseHostFn = func(bindIfIndex int) (string, bool) {
		return host, ok
	}
	t.Cleanup(func() { defaultRouteAdvertiseHostFn = previous })
}

// advertiseTestNode is a started isolated node reset to the honest startup
// state of a production node: the wildcard listen leaves the NAT profile
// Unknown with no external address yet (in tests the loopback bind would
// otherwise pre-fill ExternalAddress=127.0.0.1 and short-circuit the chain
// these tests walk).
func advertiseTestNode(t *testing.T) *Node {
	t.Helper()
	node := startOverlayNode(t, "advertise-room")
	node.natProfile.Store(nat.Profile{Type: nat.TypeUnknown})
	return node
}

// A public host must advertise the address its default route leaves from —
// not a private bridge next to it, not the generic interface preference.
func TestAdvertisedAddrPrefersDefaultRouteEgress(t *testing.T) {
	node := advertiseTestNode(t)
	fakeDefaultRouteEgress(t, "192.0.2.20", true)

	host, port, err := splitAdvertised(node.advertisedListenAddr())
	if err != nil {
		t.Fatalf("advertised is not host:port: %v", err)
	}
	if host != "192.0.2.20" || port != portOf(t, node) {
		t.Fatalf("advertised = %s:%s, want the default-route egress 192.0.2.20:%s — a public box handing out a bridge or LAN address is bug №5", host, port, portOf(t, node))
	}
}

// The egress source must be validated, not trusted: the probe can return a
// CGNAT carrier address, a private LAN address, a loopback — none of those
// is an address the fleet can dial, and the old chain must answer instead.
func TestDefaultRouteAdvertiseHostRejectsNonGlobalEgress(t *testing.T) {
	for name, host := range map[string]string{
		"private rfc1918": "192.168.1.10",
		"private rfc4193": "fd00::10",
		"cgnat rfc6598":   "100.64.1.10",
		"loopback":        "127.0.0.1",
		"link-local":      "fe80::1",
		"multicast":       "224.0.0.1",
		"unspecified":     "0.0.0.0",
	} {
		t.Run(name, func(t *testing.T) {
			node := advertiseTestNode(t)
			fakeDefaultRouteEgress(t, host, true)

			if gotHost, ok := node.defaultRouteAdvertiseHost(); ok {
				t.Fatalf("egress %s advertised as %s: a non-global default-route source must fall back to the local chain", host, gotHost)
			}
		})
	}
}

// A public egress source passes the gate.
func TestDefaultRouteAdvertiseHostAcceptsPublicEgress(t *testing.T) {
	node := advertiseTestNode(t)
	fakeDefaultRouteEgress(t, "192.0.2.20", true)

	host, ok := node.defaultRouteAdvertiseHost()
	if !ok || host != "192.0.2.20" {
		t.Fatalf("egress = %q ok=%v, want the public source accepted", host, ok)
	}
}

// No egress at all (air-gapped box, failed probe): the advertise chain
// behaves exactly as before — this is the LAN case, and it must not change.
func TestAdvertisedAddrFallsBackWithoutEgress(t *testing.T) {
	node := advertiseTestNode(t)
	fakeDefaultRouteEgress(t, "", false)

	if got := node.advertisedListenAddr(); got == "" {
		t.Fatal("advertise chain produced nothing without an egress route")
	}
	if _, ok := node.defaultRouteAdvertiseHost(); ok {
		t.Fatal("a failed probe must report no egress")
	}
}

// The egress host leads the chain ahead of the peer-subnet preference: a
// peer pair arriving over the docker bridge's subnet must not drag the
// advertise onto it — the stand shape with peers on the bridge.
func TestAdvertisedAddrEgressLeadsPeerSubnet(t *testing.T) {
	node := advertiseTestNode(t)
	fakeDefaultRouteEgress(t, "192.0.2.20", true)

	node.mu.Lock()
	node.peers["docker-pair"] = &peerConn{id: "docker-pair", addr: "172.26.0.7:10001"}
	node.mu.Unlock()

	if got := node.advertisedListenAddr(); got != "192.0.2.20:"+portOf(t, node) {
		t.Fatalf("advertised = %s, want the egress host: a bridge-subnet peer must not win the advertise over the default route", got)
	}
}

// A profile external address (a confirmed port mapping or STUN observation)
// still wins over the egress probe: a working observed route is stronger
// evidence than a routing-table guess.
func TestAdvertisedAddrProfileLeadsEgress(t *testing.T) {
	node := advertiseTestNode(t)
	fakeDefaultRouteEgress(t, "192.0.2.20", true)

	node.natProfile.Store(nat.Profile{
		Type:            nat.TypePublic,
		PublicReachable: true,
		ExternalAddress: "203.0.113.9:40001",
	})

	if got := node.advertisedListenAddr(); got != "203.0.113.9:40001" {
		t.Fatalf("advertised = %s, want the profile's external address", got)
	}
}

// The NAT gate. An observation whose host is the node's own egress address
// is a third party confirming the endpoint the socket sits on — no NAT is
// in front. Feeding that to the binding classifier upgraded Unknown to
// port_restricted_cone and locked both directly public stand hosts out of
// TypePublic forever (the public upgrade only fires from Unknown). The
// observation must stay what it is: an external address, not NAT evidence.
func TestEgressObservationIsNotNATEvidence(t *testing.T) {
	node := advertiseTestNode(t)
	fakeDefaultRouteEgress(t, "192.0.2.9", true)
	// The observation a STUN vantage point reports for this node's socket:
	// its own egress address at its own listen port — the no-NAT shape.
	observed := "192.0.2.9:" + portOf(t, node)

	// Two agreeing vantage points on our own address — the fold that used
	// to mint the cone verdict.
	for i := 0; i < 2; i++ {
		node.applyExternalObservation(observed, time.Now().Add(time.Second))
	}

	profile := node.natProfile.Load().(nat.Profile)
	if profile.Type == nat.TypePortRestricted || profile.Type == nat.TypeSymmetric {
		t.Fatalf("our own egress endpoint classified as %v: a stable observation of the bound address means no NAT is in front and must not mint a mapping verdict", profile.Type)
	}
	if profile.ExternalAddress != observed {
		t.Fatalf("external address = %q, want %q — the observation is still the right thing to advertise", profile.ExternalAddress, observed)
	}
}

// The same fold against an address that is NOT ours is genuine NAT
// evidence — a home box behind a router seeing the router's stable mapping
// — and the classifier must keep its verdict. The gate may only fire on
// our own egress, or real NATs would go undetected again.
func TestNonEgressObservationKeepsNATVerdict(t *testing.T) {
	node := advertiseTestNode(t)
	fakeDefaultRouteEgress(t, "192.0.2.9", true)
	behindNAT := "192.0.2.27:40001"

	for i := 0; i < 2; i++ {
		node.applyExternalObservation(behindNAT, time.Now().Add(time.Second))
	}

	profile := node.natProfile.Load().(nat.Profile)
	if profile.Type != nat.TypePortRestricted {
		t.Fatalf("a genuine NAT observation classified as %v: agreeing ports behind a router IS a cone verdict, and only our own egress address is NAT-free evidence", profile.Type)
	}
}

// Without an egress route the NAT gate is inert: observations classify
// exactly as before (the air-gapped and pure-LAN cases change nothing).
func TestEgressNATGateInertWithoutEgress(t *testing.T) {
	node := advertiseTestNode(t)
	fakeDefaultRouteEgress(t, "", false)
	observed := "203.0.113.7:40001"

	for i := 0; i < 2; i++ {
		node.applyExternalObservation(observed, time.Now().Add(time.Second))
	}

	profile := node.natProfile.Load().(nat.Profile)
	if profile.Type != nat.TypePortRestricted {
		t.Fatalf("without an egress host the gate must be inert, got %v", profile.Type)
	}
}

// The production probe must exist and never block: a connected UDP socket
// sends no packet — the kernel resolves the route locally — so even a
// hostile network cannot make it hang past its dialer timeout.
func TestDefaultRouteAdvertiseHostFnBounded(t *testing.T) {
	done := make(chan struct{})
	go func() {
		defaultRouteAdvertiseHostFn(0)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the default-route probe blocked past every bound: it must stay a local syscall")
	}
}

func portOf(t *testing.T, node *Node) string {
	t.Helper()
	port := node.ListenPort()
	if port <= 0 {
		t.Fatal("node has no listen port")
	}
	return strconv.Itoa(port)
}

func splitAdvertised(addr string) (string, string, error) {
	return net.SplitHostPort(addr)
}
