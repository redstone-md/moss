//go:build !geoip

package mesh

import "testing"

// Without the `geoip` tag no database is embedded and geo.Proximity is
// neutral, so relay selection must fall back to score/load ordering: the
// higher-scored peer-fr wins even though it is geographically farther.
func TestSelectRelayPeerFallsBackToScoreWithoutGeoIP(t *testing.T) {
	node := geoRelayFixture(t)
	selected, err := node.selectRelayPeer("target-peer")
	if err != nil {
		t.Fatalf("selectRelayPeer failed: %v", err)
	}
	if selected != "peer-fr" {
		t.Fatalf("expected higher-scored peer-fr with geo disabled, got %s", selected)
	}
}
