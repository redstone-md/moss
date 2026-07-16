package mesh

import (
	"testing"
	"time"
)

func natCtxNode(t *testing.T, observations ...string) *Node {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Trackers = nil
	cfg.LANDiscoveryEnabled = false
	cfg.DHTEnabled = false
	n, err := NewNode("room", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	n.bindingHistory = append(n.bindingHistory, observations...)
	return n
}

// The whole diagnostic lives in the port's behaviour: a mapping that differs by
// destination is symmetric NAT — which is what the classifier kept missing and
// what made two players unable to punch. Getting there needs no address, so
// none is shipped.
func TestNATContextDetectsPortVariationWithoutShippingAnAddress(t *testing.T) {
	n := natCtxNode(t, "203.0.113.7:43364", "203.0.113.7:52010")
	ctx := n.natContext()

	if differed, ok := ctx["ports_differed"].(bool); !ok || !differed {
		t.Fatalf("ports_differed = %v; two destinations seeing different mapped ports is symmetric NAT", ctx["ports_differed"])
	}
	if ctx["mapped_port"] != 52010 {
		t.Errorf("mapped_port = %v, want the latest observation 52010", ctx["mapped_port"])
	}
	if ctx["observations"] != 2 {
		t.Errorf("observations = %v, want 2", ctx["observations"])
	}
	if ctx["family"] != "ipv4" {
		t.Errorf("family = %v, want ipv4", ctx["family"])
	}
	// No field may carry the address itself.
	for k, v := range ctx {
		if s, ok := v.(string); ok && s == "203.0.113.7" {
			t.Fatalf("field %q leaks the node's address", k)
		}
	}
	if _, ok := ctx["observed_addr"]; ok {
		t.Fatal("the observed endpoint must never be shipped: the port behaviour is the diagnostic, the IP is only risk")
	}
}

func TestNATContextStablePortIsNotSymmetric(t *testing.T) {
	n := natCtxNode(t, "203.0.113.7:41666", "203.0.113.7:41666")
	ctx := n.natContext()
	if differed, ok := ctx["ports_differed"].(bool); !ok || differed {
		t.Fatalf("ports_differed = %v, want false: the same mapped port from two vantage points is a cone NAT", ctx["ports_differed"])
	}
}

// One observation cannot tell symmetric from cone, so the field must be absent
// rather than a guess — guessing here is exactly what went wrong before.
func TestNATContextWithdrawsTheClaimOnASingleObservation(t *testing.T) {
	n := natCtxNode(t, "203.0.113.7:41666")
	ctx := n.natContext()
	if _, ok := ctx["ports_differed"]; ok {
		t.Fatal("a single observation cannot distinguish symmetric from cone; the field must be omitted, not guessed")
	}
	if ctx["mapped_port"] != 41666 {
		t.Errorf("mapped_port = %v, want 41666", ctx["mapped_port"])
	}
}

func TestNATContextWithNoObservations(t *testing.T) {
	n := natCtxNode(t)
	ctx := n.natContext()
	if ctx["observations"] != 0 {
		t.Errorf("observations = %v, want 0", ctx["observations"])
	}
	if _, ok := ctx["mapped_port"]; ok {
		t.Error("no observations means no mapped port to report")
	}
	if ctx["nat_type"] == nil {
		t.Error("nat_type must always be present")
	}
}

func TestNATContextIPv6Family(t *testing.T) {
	n := natCtxNode(t, "[2001:db8::1]:41666", "[2001:db8::1]:41666")
	if got := n.natContext()["family"]; got != "ipv6" {
		t.Errorf("family = %v, want ipv6", got)
	}
}

// Reporting must be free when no sink is configured — this runs on connect
// paths, and the overwhelming majority of nodes ship nothing.
func TestReportingIsANoopWithoutASink(t *testing.T) {
	n := natCtxNode(t, "203.0.113.7:41666")
	if n.AxiomEnabled() {
		t.Fatal("a fresh node must not have a sink")
	}
	// Must not panic or block.
	n.reportConnectAttempt(outcomeDirect, reasonNone, time.Now(), false)
	n.reportRendezvous(1, 0, time.Now())
	n.reportSessionLifetime(false, time.Now(), 0, originDialTCP)
}
