//go:build !geoip

package geo

import (
	"net"
	"testing"
)

// In default builds (no `geoip` tag) the GeoLite2 database is not embedded,
// so every lookup must degrade to "unknown" instead of failing.
func TestStubLookupReturnsUnknown(t *testing.T) {
	l := Lookup(net.ParseIP("8.8.8.8"))
	if l != (Location{}) {
		t.Fatalf("stub lookup: want zero Location, got %+v", l)
	}
	if Lookup(nil) != (Location{}) {
		t.Fatal("stub lookup of nil IP must return zero Location")
	}
}

func TestStubProximityIsNeutral(t *testing.T) {
	for _, pair := range [][2]string{
		{"8.8.8.8", "8.8.4.4"},
		{"8.8.8.8", "51.91.242.9"},
		{"192.168.1.1", "8.8.8.8"},
	} {
		if got := Proximity(net.ParseIP(pair[0]), net.ParseIP(pair[1])); got != 0 {
			t.Fatalf("stub proximity %s↔%s: want 0 (neutral), got %d", pair[0], pair[1], got)
		}
	}
	if Proximity(nil, net.ParseIP("8.8.8.8")) != 0 {
		t.Fatal("stub proximity with nil IP must be 0 (neutral)")
	}
}
