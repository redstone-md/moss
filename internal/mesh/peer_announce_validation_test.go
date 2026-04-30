package mesh

import (
	"testing"

	"moss/internal/gossip"
	mcrypto "moss/internal/crypto"
)

func TestHandlePeerAnnounceRejectsThirdPartyAdvertisement(t *testing.T) {
	node := &Node{
		knownPeers: make(map[string]knownPeer),
		scoring:    gossip.NewEngine(),
	}
	identity, err := mcrypto.NewIdentity()
	if err != nil {
		t.Fatalf("new identity: %v", err)
	}
	node.identity = identity

	peer := &peerConn{id: "peer-a", addr: "198.51.100.10:4001"}
	node.handlePeerAnnounce(peer, gossip.Envelope{
		AdvertisedPeerID: "peer-b",
		AdvertisedAddr:   "203.0.113.20:5001",
	})

	if len(node.knownPeers) != 0 {
		t.Fatalf("expected third-party advertisement to be ignored, got %d known peers", len(node.knownPeers))
	}
}
