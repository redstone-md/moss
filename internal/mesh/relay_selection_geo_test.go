package mesh

import (
	"testing"

	"github.com/redstone-md/moss/internal/nat"
)

// geoRelayFixture seeds a node with two equally ranked relay candidates and a
// target: peer-us sits in the target's country, peer-fr does not but carries
// the higher application score. Proximity outranks score in selectRelayPeers,
// so which peer wins depends entirely on whether the `geoip` build tag has
// compiled the GeoLite2 database into internal/geo. The per-build expectations
// live in relay_selection_geo_geoip_test.go / relay_selection_geo_stub_test.go.
func geoRelayFixture(t *testing.T) *Node {
	t.Helper()
	node, err := NewNode("mesh-relay-geo", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	node.peers["peer-us"] = &peerConn{id: "peer-us"}
	node.peers["peer-fr"] = &peerConn{id: "peer-fr"}
	node.knownPeers["peer-us"] = knownPeer{
		id:              "peer-us",
		addr:            "8.8.4.4:4000",
		natType:         nat.TypePublic,
		natTrusted:      true,
		publicReachable: true,
		relayCapable:    true,
	}
	node.knownPeers["peer-fr"] = knownPeer{
		id:              "peer-fr",
		addr:            "51.91.242.9:4000",
		natType:         nat.TypePublic,
		natTrusted:      true,
		publicReachable: true,
		relayCapable:    true,
	}
	node.knownPeers["target-peer"] = knownPeer{
		id:   "target-peer",
		addr: "8.8.8.8:4000",
	}
	node.scoring.SetApplicationScore("peer-us", 1)
	node.scoring.SetApplicationScore("peer-fr", 5)
	return node
}
