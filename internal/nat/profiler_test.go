package nat

import "testing"

func TestProfilerDetectClassifiesSpecialRanges(t *testing.T) {
	profiler := NewProfiler()
	tests := []struct {
		addr string
		want Type
	}{
		{addr: "127.0.0.1:9000", want: TypeRestrictedCone},
		{addr: "10.1.2.3:9000", want: TypeFullCone},
		{addr: "100.64.1.10:9000", want: TypeCGNAT},
		{addr: "203.0.113.7:9000", want: TypePublic},
		{addr: "[::]:9000", want: TypeUnknown},
	}
	for _, tc := range tests {
		profile := profiler.Detect(tc.addr)
		if profile.Type != tc.want {
			t.Fatalf("Detect(%q) = %q, want %q", tc.addr, profile.Type, tc.want)
		}
	}
}

func TestProfilerWithExternalAddressKeepsReachabilityConservative(t *testing.T) {
	profiler := NewProfiler()
	base := profiler.Detect("0.0.0.0:4040")
	mapped := profiler.WithExternalAddress(base, "198.51.100.10:5050")
	if mapped.PublicReachable {
		t.Fatal("expected mapped profile to stay unconfirmed")
	}
	if mapped.ExternalAddress != "198.51.100.10:5050" {
		t.Fatalf("unexpected external address %q", mapped.ExternalAddress)
	}
	if mapped.Type != TypePortRestricted && mapped.Type != TypeUnknown {
		t.Fatalf("unexpected mapped type %q", mapped.Type)
	}
}

func TestProfilerWithBindingObservationsDetectsSymmetricNAT(t *testing.T) {
	profiler := NewProfiler()
	base := profiler.Detect("10.0.0.5:4040")
	profile := profiler.WithBindingObservations(base, []string{
		"198.51.100.10:50000",
		"198.51.100.10:50004",
		"198.51.100.10:50008",
	})
	if profile.Type != TypeSymmetric {
		t.Fatalf("unexpected profile type %q", profile.Type)
	}
	if profile.PublicReachable {
		t.Fatal("symmetric NAT should not be marked public reachable")
	}
}

// TestProfilerNeverInfersPublicFromReflexiveAddress guards the CGNAT bug: a
// node bound to 0.0.0.0 whose STUN reflexive address is public (its provider's
// WAN IP) must NOT be classified public or reachable from address shape alone.
func TestProfilerNeverInfersPublicFromReflexiveAddress(t *testing.T) {
	profiler := NewProfiler()
	base := profiler.Detect("0.0.0.0:4040") // -> TypeUnknown
	profile := profiler.WithBindingObservations(base, []string{
		"188.122.209.9:50000",
		"188.122.209.9:50000",
		"188.122.209.9:50000",
	})
	if profile.Type == TypePublic {
		t.Fatalf("reflexive public address must not yield TypePublic, got %q", profile.Type)
	}
	if profile.PublicReachable {
		t.Fatal("reflexive public address must not mark the node publicly reachable")
	}
}

func TestProfilerWithBindingObservationsDetectsStableRestrictedProfile(t *testing.T) {
	profiler := NewProfiler()
	base := profiler.Detect("10.0.0.5:4040")
	profile := profiler.WithBindingObservations(base, []string{
		"198.51.100.10:50000",
		"198.51.100.10:50000",
		"198.51.100.10:50000",
	})
	if profile.Type != TypePortRestricted {
		t.Fatalf("unexpected profile type %q", profile.Type)
	}
}
