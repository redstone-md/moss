package mesh

import (
	"encoding/hex"
	"testing"

	"moss/internal/gossip"
	"moss/internal/nat"
)

func TestSupernodeEnvelopeSignatureRoundTrip(t *testing.T) {
	node, err := NewNode("mesh-supernode-sign", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	env := gossip.Envelope{
		Type:                   gossip.TypeSupernodeAnnounce,
		AdvertisedPeerID:       node.localPeerID(),
		AdvertisedAddr:         "192.168.1.50:41030",
		AdvertisedNATType:      string(nat.TypePublic),
		AdvertisedReachable:    true,
		AdvertisedRelayCapable: true,
	}
	signed := node.signSupernodeEnvelope(env)
	if len(signed.AdvertisedSignature) == 0 {
		t.Fatal("expected signed supernode envelope to have signature")
	}
	if !verifySupernodeEnvelope(signed) {
		t.Fatal("expected valid supernode signature to verify")
	}
	signed.AdvertisedAddr = "192.168.1.99:41030"
	if verifySupernodeEnvelope(signed) {
		t.Fatal("expected modified supernode envelope to fail verification")
	}
}

func TestPeerAnnouncementSignatureRoundTrip(t *testing.T) {
	node, err := NewNode("mesh-peer-sign", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	env := gossip.Envelope{
		Type:                   gossip.TypePeerAnnounce,
		AdvertisedPeerID:       node.localPeerID(),
		AdvertisedAddr:         "192.168.1.50:41030",
		AdvertisedNATType:      string(nat.TypePublic),
		AdvertisedReachable:    true,
		AdvertisedRelayCapable: false,
	}
	signed := node.signPeerAnnouncementEnvelope(env)
	if len(signed.AdvertisedSignature) == 0 {
		t.Fatal("expected signed peer announcement to have signature")
	}
	if !verifyPeerAnnouncementEnvelope(signed) {
		t.Fatal("expected valid peer announcement signature to verify")
	}
	signed.AdvertisedAddr = "192.168.1.99:41030"
	if verifyPeerAnnouncementEnvelope(signed) {
		t.Fatal("expected modified peer announcement to fail verification")
	}
}

func TestHandleSupernodeStatusRejectsInvalidSignature(t *testing.T) {
	nodeA, err := NewNode("mesh-supernode-invalid", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode nodeA failed: %v", err)
	}
	nodeB, err := NewNode("mesh-supernode-invalid", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode nodeB failed: %v", err)
	}

	pubB := nodeB.PublicKey()
	peerID := hex.EncodeToString(pubB[:])
	peer := &peerConn{id: peerID}

	base := nodeA.scoring.Score(peerID)
	nodeA.handleEnvelope(peer, gossip.Envelope{
		Type:                   gossip.TypeSupernodeAnnounce,
		AdvertisedPeerID:       peerID,
		AdvertisedAddr:         "192.168.1.50:41030",
		AdvertisedNATType:      string(nat.TypePublic),
		AdvertisedReachable:    true,
		AdvertisedRelayCapable: true,
	})

	nodeA.mu.RLock()
	info := nodeA.knownPeers[peerID]
	nodeA.mu.RUnlock()
	if info.relayCapable {
		t.Fatal("expected invalid supernode announce to be rejected")
	}
	if score := nodeA.scoring.Score(peerID); score >= base {
		t.Fatalf("expected invalid supernode announce to penalize sender, base=%f new=%f", base, score)
	}
}

func TestVerifySupernodeEnvelopeRejectsWrongPublicKeyLength(t *testing.T) {
	env := gossip.Envelope{
		Type:                   gossip.TypeSupernodeAnnounce,
		AdvertisedPeerID:       "00",
		AdvertisedAddr:         "192.168.1.50:41030",
		AdvertisedNATType:      string(nat.TypePublic),
		AdvertisedReachable:    true,
		AdvertisedRelayCapable: true,
		AdvertisedSignature:    []byte{1},
	}
	if verifySupernodeEnvelope(env) {
		t.Fatal("expected envelope with invalid advertised public key length to be rejected")
	}
}
