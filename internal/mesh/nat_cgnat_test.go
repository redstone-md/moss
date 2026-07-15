package mesh

import (
	"testing"

	"github.com/redstone-md/moss/internal/nat"
)

func TestPublicReflexiveAddrClassification(t *testing.T) {
	cases := []struct {
		addr string
		want bool
	}{
		{"188.122.209.9:41000", true},
		{"203.0.113.7:1", true},
		{"10.16.17.121:41000", false}, // RFC1918 private
		{"100.64.0.1:41000", false},   // CGNAT shared range
		{"127.0.0.1:41000", false},    // loopback
		{"garbage", false},
	}
	for _, c := range cases {
		_, ok := publicReflexiveAddr(c.addr)
		if ok != c.want {
			t.Fatalf("publicReflexiveAddr(%q)=%v want %v", c.addr, ok, c.want)
		}
	}
}

// TestLabelExternalReachabilityRequiresProbe verifies that a public reflexive
// address only becomes TypePublic when inbound reachability is confirmed, and
// that an unconfirmed private-local host is treated as carrier NAT — never
// public, never supernode-eligible.
func TestLabelExternalReachabilityRequiresProbe(t *testing.T) {
	n := &Node{}

	// Confirmed inbound reachability promotes an Unknown profile to Public.
	reachable := nat.Profile{Type: nat.TypeUnknown, PublicReachable: true}
	got := n.labelExternalReachability(reachable, "188.122.209.9:41000")
	if got.Type != nat.TypePublic {
		t.Fatalf("reachable public reflexive should be TypePublic, got %q", got.Type)
	}

	// A private reflexive address is ignored (not public-shaped) and untouched.
	priv := nat.Profile{Type: nat.TypeFullCone}
	if got := n.labelExternalReachability(priv, "10.16.17.121:41000"); got.Type != nat.TypeFullCone {
		t.Fatalf("private reflexive should be untouched, got %q", got.Type)
	}

	// Unreachable + public reflexive: must never be labelled Public, regardless
	// of the host's interfaces (it stays Unknown or becomes CGNAT).
	unconfirmed := nat.Profile{Type: nat.TypeUnknown, PublicReachable: false}
	got = n.labelExternalReachability(unconfirmed, "188.122.209.9:41000")
	if got.Type == nat.TypePublic {
		t.Fatalf("unreachable node must not be labelled public, got %q", got.Type)
	}
	if got.PublicReachable {
		t.Fatal("labelExternalReachability must never set PublicReachable")
	}
}
