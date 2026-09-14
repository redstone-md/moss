//go:build geoip

package mesh

import "testing"

// With the `geoip` tag the GeoLite2 database is embedded: a relay in the
// target's country (peer-us, US) must win over a higher-scored foreign relay
// (peer-fr, FR), because proximity is ranked above score.
func TestSelectRelayPeerPrefersGeoProximityWithGeoIP(t *testing.T) {
	node := geoRelayFixture(t)
	selected, err := node.selectRelayPeer("target-peer")
	if err != nil {
		t.Fatalf("selectRelayPeer failed: %v", err)
	}
	if selected != "peer-us" {
		t.Fatalf("expected same-country relay peer-us despite lower score, got %s", selected)
	}
}
