package mesh

import (
	"testing"

	mcrypto "moss/internal/crypto"
	"moss/internal/gossip"
	"moss/internal/nat"
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
	baseScore := node.scoring.Score(peer.id)
	node.handlePeerAnnounce(peer, gossip.Envelope{
		AdvertisedPeerID: "peer-b",
		AdvertisedAddr:   "203.0.113.20:5001",
	})

	if len(node.knownPeers) != 0 {
		t.Fatalf("expected third-party advertisement to be ignored, got %d known peers", len(node.knownPeers))
	}
	if score := node.scoring.Score(peer.id); score != baseScore {
		t.Fatalf("expected unsigned third-party advertisement not to penalize sender, base=%f new=%f", baseScore, score)
	}
}

func TestPeerAnnounceCannotPoisonTargetNATRelayPreference(t *testing.T) {
	node, err := NewNode("mesh-peer-announce-nat-poison", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	node.natProfile.Store(nat.Profile{Type: nat.TypeSymmetric})

	attackerID := "attacker-peer"
	targetID := "target-peer"
	node.handlePeerAnnounce(&peerConn{id: attackerID, addr: "198.51.100.10:4001"}, gossip.Envelope{
		Type:              gossip.TypePeerAnnounce,
		AdvertisedPeerID:  targetID,
		AdvertisedAddr:    "203.0.113.20:5001",
		AdvertisedNATType: string(nat.TypeSymmetric),
	})

	if _, ok := node.knownPeers[targetID]; ok {
		t.Fatal("expected unsigned third-party NAT advertisement to be ignored")
	}
	if node.shouldPreferRelayForTarget(targetID) {
		t.Fatal("expected untrusted NAT advertisement not to force relay preference")
	}
}

func TestUntrustedKnownPeerNATDoesNotForceRelayPreference(t *testing.T) {
	node, err := NewNode("mesh-peer-announce-untrusted-nat", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	node.natProfile.Store(nat.Profile{Type: nat.TypeSymmetric})
	targetID := "target-peer"
	node.knownPeers[targetID] = knownPeer{
		id:      targetID,
		addr:    "203.0.113.20:5001",
		natType: nat.TypeSymmetric,
	}

	if node.shouldPreferRelayForTarget(targetID) {
		t.Fatal("expected untrusted known peer NAT type not to force relay preference")
	}
}

func TestHandlePeerAnnounceAcceptsSignedThirdPartyAdvertisement(t *testing.T) {
	nodeA, err := NewNode("mesh-peer-announce-signed", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode nodeA failed: %v", err)
	}
	nodeB, err := NewNode("mesh-peer-announce-signed", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode nodeB failed: %v", err)
	}
	nodeC, err := NewNode("mesh-peer-announce-signed", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode nodeC failed: %v", err)
	}

	env := nodeC.peerAnnouncementEnvelope(knownPeer{
		id:   nodeC.localPeerID(),
		addr: "203.0.113.20:5001",
	})
	nodeA.handlePeerAnnounce(&peerConn{id: nodeB.localPeerID()}, env)

	nodeA.mu.RLock()
	info, ok := nodeA.knownPeers[nodeC.localPeerID()]
	nodeA.mu.RUnlock()
	if !ok {
		t.Fatal("expected signed third-party advertisement to be stored")
	}
	if info.addr != "203.0.113.20:5001" {
		t.Fatalf("expected signed third-party addr to be stored, got %q", info.addr)
	}
	if len(info.signature) == 0 {
		t.Fatal("expected signed third-party advertisement signature to be retained")
	}
}
