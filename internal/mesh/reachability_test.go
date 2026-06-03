package mesh

import (
	"testing"
	"time"

	"moss/internal/nat"
)

// A single public reflexive address is only the NAT's WAN IP, not proof of
// inbound reachability. With no peer to run an inbound probe, the node must not
// self-report public reachability (regression for the "everyone shows open" NAT
// misdetection).
func TestApplyExternalObservationDoesNotClaimReachableWithoutProbe(t *testing.T) {
	node, err := NewNode("mesh-nat-reachability", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	node.natProfile.Store(nat.Profile{Type: nat.TypeUnknown})

	node.applyExternalObservation("203.0.113.50:41030", time.Now().Add(time.Second))

	profile := node.natProfile.Load().(nat.Profile)
	if profile.PublicReachable {
		t.Fatalf("must not self-report reachable without an inbound probe: %+v", profile)
	}
}

// The same node observed from two destinations gets two different mapped ports —
// the signature of a symmetric NAT — and must be classified as such (and not
// reachable) so Moss falls back to hole-punch/relay instead of futile direct
// dials.
func TestApplyExternalObservationDetectsSymmetricFromVaryingPorts(t *testing.T) {
	node, err := NewNode("mesh-nat-symmetric", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	node.natProfile.Store(nat.Profile{Type: nat.TypeUnknown})

	deadline := time.Now().Add(time.Second)
	node.applyExternalObservation("203.0.113.50:41030", deadline)
	node.applyExternalObservation("203.0.113.50:41031", deadline)

	profile := node.natProfile.Load().(nat.Profile)
	if profile.Type != nat.TypeSymmetric {
		t.Fatalf("expected symmetric NAT from varying mapped ports, got %q", profile.Type)
	}
	if profile.PublicReachable {
		t.Fatalf("symmetric NAT must not be publicly reachable: %+v", profile)
	}
}

func TestSameAdvertisedEndpoint(t *testing.T) {
	tests := []struct {
		name string
		a    string
		b    string
		want bool
	}{
		{name: "same IPv4 endpoint", a: "198.51.100.10:41030", b: "198.51.100.10:41030", want: true},
		{name: "IPv4 mapped IPv6 endpoint", a: "[::ffff:198.51.100.10]:41030", b: "198.51.100.10:41030", want: true},
		{name: "different port", a: "198.51.100.10:41030", b: "198.51.100.10:41031"},
		{name: "different host", a: "198.51.100.10:41030", b: "198.51.100.11:41030"},
		{name: "advertised hostname rejected", a: "example.com:41030", b: "198.51.100.10:41030"},
		{name: "peer hostname rejected", a: "198.51.100.10:41030", b: "example.com:41030"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sameAdvertisedEndpoint(tt.a, tt.b); got != tt.want {
				t.Fatalf("sameAdvertisedEndpoint(%q, %q) = %t, want %t", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

func TestReachabilityProbePeerIDsExcludeFreshPrivatePeer(t *testing.T) {
	node := &Node{
		peers: map[string]*peerConn{
			"private": {id: "private", addr: "10.0.0.8:41030"},
		},
		knownPeers: map[string]knownPeer{},
	}

	peerIDs := node.reachabilityProbePeerIDsLocked()
	if len(peerIDs) != 0 {
		t.Fatalf("expected no eligible reachability probers, got %#v", peerIDs)
	}
}

func TestReachabilityProbePeerIDsIncludeBootstrapAndPublicPeers(t *testing.T) {
	node := &Node{
		peers: map[string]*peerConn{
			"bootstrap": {id: "bootstrap", addr: "10.0.0.8:41030", bootstrap: true},
			"public":    {id: "public", addr: "198.51.100.10:41030"},
		},
		knownPeers: map[string]knownPeer{
			"public": {id: "public", publicReachable: true},
		},
	}

	peerIDs := node.reachabilityProbePeerIDsLocked()
	if len(peerIDs) != 2 || peerIDs[0] != "bootstrap" || peerIDs[1] != "public" {
		t.Fatalf("unexpected eligible reachability probers: %#v", peerIDs)
	}
}

func TestReachabilityProbePeerIDsIncludePublicTransportAddr(t *testing.T) {
	node := &Node{
		peers: map[string]*peerConn{
			"public-addr": {id: "public-addr", addr: "203.0.113.20:41030"},
		},
		knownPeers: map[string]knownPeer{},
	}

	peerIDs := node.reachabilityProbePeerIDsLocked()
	if len(peerIDs) != 1 || peerIDs[0] != "public-addr" {
		t.Fatalf("expected public peer address to be eligible, got %#v", peerIDs)
	}
}
