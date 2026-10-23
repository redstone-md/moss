package mesh

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/redstone-md/moss/internal/gossip"
)

func TestAnnouncementVerificationHandlerGolden(t *testing.T) {
	node, err := NewNode("announcement-golden", nil, DefaultOfflineConfig())
	if err != nil {
		t.Fatal(err)
	}
	env := announcementBenchmarkFixtures(2, false)[1]
	peer := &peerConn{id: env.AdvertisedPeerID, addr: env.AdvertisedAddr}
	var states []string
	snapshot := func() {
		known := node.knownPeers[env.AdvertisedPeerID]
		states = append(states, fmt.Sprintf("%s|%s|%t|%t|%t|%t", known.id,
			known.addr, known.verified, known.natTrusted, known.publicReachable, known.relayCapable))
	}
	node.handlePeerAnnounce(&peerConn{id: "forwarder", addr: "198.51.100.10:4001"}, env)
	snapshot()
	node.handlePeerAnnounce(peer, env)
	snapshot()
	env.AdvertisedRelayCapable, env.AdvertisedReachable = true, true
	node.handlePeerAnnounce(peer, env) // These unsigned fields never grant capabilities.
	snapshot()
	status := announcementBenchmarkFixtures(2, false)[0]
	statusPeer := &peerConn{id: status.AdvertisedPeerID, addr: status.AdvertisedAddr}
	for range 2 {
		node.handleSupernodeStatus(statusPeer, status, true)
		snapshot()
	}
	status.Type = gossip.TypeSupernodeRevoke
	node.handleSupernodeStatus(statusPeer, status, false) // Old signature must be rejected.
	snapshot()
	raw, err := json.Marshal(states)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("GOLDEN %x", sha256.Sum256(raw))
}
