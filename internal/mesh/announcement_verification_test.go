package mesh

import (
	"testing"

	mcrypto "github.com/redstone-md/moss/internal/crypto"
)

func TestAnnouncementVerificationReusesSuccessfulSignature(t *testing.T) {
	identity, err := mcrypto.NewIdentity()
	if err != nil {
		t.Fatal(err)
	}
	key := identity.PublicKey()
	payload := []byte("signed announcement fixture")
	signature := identity.Sign(payload)
	var memo announcementVerifier
	calls := 0
	check := func(key, payload, signature []byte) bool {
		calls++
		return mcrypto.Verify(key, payload, signature)
	}
	for range 2 {
		if !memo.verify(key[:], payload, signature, check) {
			t.Fatal("valid repeated signature rejected")
		}
	}
	if calls != 1 {
		t.Fatalf("unchanged valid announcement ran crypto %d times, want 1", calls)
	}
}

func TestWarmAnnouncementStillReevaluatesSenderTrust(t *testing.T) {
	node, err := NewNode("announcement-memo", nil, DefaultOfflineConfig())
	if err != nil {
		t.Fatal(err)
	}
	signer, err := NewNode("announcement-memo", nil, DefaultOfflineConfig())
	if err != nil {
		t.Fatal(err)
	}
	env := signer.peerAnnouncementEnvelope(knownPeer{
		id: signer.localPeerID(), addr: "203.0.113.20:55005",
	})
	node.handlePeerAnnounce(&peerConn{id: "forwarder", addr: "198.51.100.10:4001"}, env)
	if known := node.knownPeers[signer.localPeerID()]; known.verified || known.addr == "" {
		t.Fatal("third-party signed claim incorrectly granted direct-peer trust")
	}
	node.handlePeerAnnounce(&peerConn{id: signer.localPeerID(), addr: env.AdvertisedAddr}, env)
	if known := node.knownPeers[signer.localPeerID()]; !known.verified {
		t.Fatal("warm signature skipped direct sender trust update")
	}
	malformed := env
	malformed.AdvertisedNoiseStatic = []byte{1}
	if verifyPeerAnnouncementSignature(malformed, node.verifyAnnouncementSignature) {
		t.Fatal("warm signature admitted a malformed Noise key")
	}
}
