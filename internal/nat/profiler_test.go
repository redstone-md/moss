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

func TestProfilerWithExternalAddressMarksProfileReachable(t *testing.T) {
	profiler := NewProfiler()
	base := profiler.Detect("10.0.0.5:4040")
	mapped := profiler.WithExternalAddress(base, "198.51.100.10:5050")
	if !mapped.PublicReachable {
		t.Fatal("expected mapped profile to be public reachable")
	}
	if mapped.ExternalAddress != "198.51.100.10:5050" {
		t.Fatalf("unexpected external address %q", mapped.ExternalAddress)
	}
	if mapped.Type != TypeFullCone {
		t.Fatalf("unexpected mapped type %q", mapped.Type)
	}
}
