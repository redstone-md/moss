package geo

import (
	"net"
	"testing"
)

func TestLookupKnownIP(t *testing.T) {
	l := Lookup(net.ParseIP("8.8.8.8"))
	if l.Country != "US" {
		t.Fatalf("8.8.8.8 country: want US, got %q", l.Country)
	}
	if l.Continent != "NA" {
		t.Fatalf("8.8.8.8 continent: want NA, got %q", l.Continent)
	}
}

func TestProximity(t *testing.T) {
	sameCountry := Proximity(net.ParseIP("8.8.8.8"), net.ParseIP("8.8.4.4")) // US, US
	if sameCountry != 2 {
		t.Fatalf("same-country proximity: want 2, got %d", sameCountry)
	}
	crossContinent := Proximity(net.ParseIP("8.8.8.8"), net.ParseIP("51.91.242.9")) // US vs FR
	if crossContinent != 0 {
		t.Fatalf("cross-continent proximity: want 0, got %d", crossContinent)
	}
	private := Proximity(net.ParseIP("192.168.1.1"), net.ParseIP("8.8.8.8"))
	if private != 0 {
		t.Fatalf("unknown/private proximity: want 0, got %d", private)
	}
	nilIP := Proximity(nil, net.ParseIP("8.8.8.8"))
	if nilIP != 0 {
		t.Fatalf("nil proximity: want 0, got %d", nilIP)
	}
}
