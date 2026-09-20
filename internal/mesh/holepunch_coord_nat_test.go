package mesh

// Bug №3 companion harness: the punch choreography's first rounds run
// NAT-blind — every early census punch logged target_nat:"" — because the
// coordination offer and reply, the only envelopes a fresh node exchanges
// with a punch target before the announce flood arrives, carried an address
// but never a NAT profile. These tests pin the fix: a signed coordination
// envelope teaches the receiver the sender's NAT type and capabilities at
// punch time, while an unsigned one (a legacy sender, or a relay peer
// trying to forge a profile on someone's behalf) teaches nothing beyond the
// address — exactly the legacy behaviour.

import (
	"testing"

	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/nat"
)

func newHolePunchCoordNode(t *testing.T, name string) (*Node, string) {
	t.Helper()
	cfg := DefaultConfig()
	cfg.MasqConfig = MasqConfig{}
	cfg.Trackers = nil
	node, err := NewNode("mesh-coord-nat-"+name, nil, cfg)
	if err != nil {
		t.Fatalf("NewNode %s failed: %v", name, err)
	}
	t.Cleanup(func() { _ = node.Stop() })
	return node, node.localPeerID()
}

func knownPeerInfo(node *Node, peerID string) knownPeer {
	node.mu.RLock()
	defer node.mu.RUnlock()
	return node.knownPeers[peerID]
}

// A signed offer teaches the target of the punch the initiator's NAT type,
// reachability and relay capability, trusted — the same trust a signed
// supernode-status announce carries, arriving at punch time instead of the
// next announce round.
func TestHolePunchCoordOfferTeachesTrustedNAT(t *testing.T) {
	nodeA, _ := newHolePunchCoordNode(t, "offer-receiver")
	initiator, initiatorID := newHolePunchCoordNode(t, "offer-initiator")

	offer := initiator.signHolePunchCoordEnvelope(gossip.Envelope{
		Type:                   gossip.TypeHolePunchCoord,
		RequestID:              "coord-nat-offer",
		CoordStage:             "offer",
		RelaySource:            initiatorID,
		RelayTarget:            nodeA.localPeerID(),
		AdvertisedAddr:         "203.0.113.10:41000",
		AdvertisedPeerID:       initiatorID,
		AdvertisedNATType:      string(nat.TypeSymmetric),
		AdvertisedReachable:    false,
		AdvertisedRelayCapable: true,
	})

	nodeA.handleHolePunchCoord(&peerConn{id: "relay-peer"}, offer)

	info := knownPeerInfo(nodeA, initiatorID)
	if info.addr != "203.0.113.10:41000" {
		t.Fatalf("coord address not recorded: %q", info.addr)
	}
	if info.natType != nat.TypeSymmetric {
		t.Fatalf("signed offer did not teach NAT type: %q", info.natType)
	}
	if !info.natTrusted {
		t.Fatal("coord-learned NAT type is not trusted; the relay preference cannot classify early punch targets")
	}
	if info.relayCapable != true {
		t.Fatal("signed offer did not teach relay capability")
	}
}

// A signed reply teaches the initiator the punch target's NAT type — this is
// the direction the census needs, since target_nat is read from the
// initiator's directory.
func TestHolePunchCoordReplyTeachesTrustedNAT(t *testing.T) {
	nodeA, _ := newHolePunchCoordNode(t, "reply-initiator")
	target, targetID := newHolePunchCoordNode(t, "reply-target")

	nodeA.mu.Lock()
	nodeA.holePunchWait["coord-nat-reply"] = holePunchRequest{targetPeerID: targetID, relayPeerID: "relay-peer"}
	nodeA.mu.Unlock()

	reply := target.signHolePunchCoordEnvelope(gossip.Envelope{
		Type:                   gossip.TypeHolePunchCoord,
		RequestID:              "coord-nat-reply",
		CoordStage:             "reply",
		RelaySource:            targetID,
		RelayTarget:            nodeA.localPeerID(),
		AdvertisedAddr:         "203.0.113.11:42000",
		AdvertisedPeerID:       targetID,
		AdvertisedNATType:      string(nat.TypePortRestricted),
		AdvertisedReachable:    true,
		AdvertisedRelayCapable: false,
	})

	nodeA.handleHolePunchCoord(&peerConn{id: "relay-peer"}, reply)

	info := knownPeerInfo(nodeA, targetID)
	if info.natType != nat.TypePortRestricted {
		t.Fatalf("signed reply did not teach target NAT type: %q", info.natType)
	}
	if !info.natTrusted {
		t.Fatal("reply-learned NAT type is not trusted")
	}
	if !info.publicReachable {
		t.Fatal("signed reply did not teach reachability")
	}
}

// Unsigned envelopes — every legacy sender on the fleet today — must keep
// working as pure address carriers: no NAT claims, no capability claims.
func TestHolePunchCoordUnsignedEnvelopeTeachesOnlyAddress(t *testing.T) {
	nodeA, _ := newHolePunchCoordNode(t, "unsigned-receiver")
	_, strangerID := newHolePunchCoordNode(t, "unsigned-stranger")

	offer := gossip.Envelope{
		Type:                   gossip.TypeHolePunchCoord,
		RequestID:              "coord-unsigned",
		CoordStage:             "offer",
		RelaySource:            strangerID,
		RelayTarget:            nodeA.localPeerID(),
		AdvertisedAddr:         "203.0.113.12:43000",
		AdvertisedPeerID:       strangerID,
		AdvertisedNATType:      string(nat.TypeSymmetric),
		AdvertisedReachable:    true,
		AdvertisedRelayCapable: true,
	}

	nodeA.handleHolePunchCoord(&peerConn{id: "relay-peer"}, offer)

	info := knownPeerInfo(nodeA, strangerID)
	if info.addr != "203.0.113.12:43000" {
		t.Fatalf("legacy coord address not recorded: %q", info.addr)
	}
	if info.natType != "" {
		t.Fatalf("unsigned envelope taught NAT type %q — a relay peer can forge unclaimed profiles", info.natType)
	}
	if info.natTrusted {
		t.Fatal("unsigned envelope taught a trusted profile")
	}
	if info.relayCapable {
		t.Fatal("unsigned envelope taught relay capability")
	}
}

// A signature from the wrong identity must not validate: the relay peer
// coordinating the punch knows the request ID and could otherwise forge the
// target's profile to steer the initiator onto its own relay.
func TestHolePunchCoordForgedSignatureTeachesNothing(t *testing.T) {
	nodeA, _ := newHolePunchCoordNode(t, "forge-receiver")
	target, targetID := newHolePunchCoordNode(t, "forge-target")
	forger, _ := newHolePunchCoordNode(t, "forge-relay")

	nodeA.mu.Lock()
	nodeA.holePunchWait["coord-forged"] = holePunchRequest{targetPeerID: targetID, relayPeerID: "relay-peer"}
	nodeA.mu.Unlock()

	// The forger claims to BE the target, signing with its own identity.
	reply := forger.signHolePunchCoordEnvelope(gossip.Envelope{
		Type:                   gossip.TypeHolePunchCoord,
		RequestID:              "coord-forged",
		CoordStage:             "reply",
		RelaySource:            targetID,
		RelayTarget:            nodeA.localPeerID(),
		AdvertisedAddr:         "203.0.113.13:44000",
		AdvertisedPeerID:       targetID,
		AdvertisedNATType:      string(nat.TypeSymmetric),
		AdvertisedReachable:    true,
		AdvertisedRelayCapable: true,
	})
	_ = target

	nodeA.handleHolePunchCoord(&peerConn{id: "relay-peer"}, reply)

	info := knownPeerInfo(nodeA, targetID)
	if info.natType != "" {
		t.Fatalf("forged profile accepted: NAT type %q", info.natType)
	}
	if info.natTrusted {
		t.Fatal("forged profile trusted")
	}
}

// The sender side: a node with no NAT classification yet must not sign an
// empty profile (legacy receivers would see a signed "unknown"), and a node
// with a classified profile signs the envelope it sends.
func TestSignedHolePunchCoordEnvelopeGatesOnProfile(t *testing.T) {
	node, nodeID := newHolePunchCoordNode(t, "envelope-builder")

	env := gossip.Envelope{
		Type:           gossip.TypeHolePunchCoord,
		RequestID:      "coord-build",
		CoordStage:     "offer",
		RelaySource:    nodeID,
		RelayTarget:    "some-target",
		AdvertisedAddr: "203.0.113.14:45000",
	}
	empty := node.signedHolePunchCoordEnvelope(env)
	if empty.AdvertisedPeerID != "" || empty.AdvertisedNATType != "" || len(empty.AdvertisedSignature) != 0 {
		t.Fatal("unclassified node signed a coordination envelope; legacy peers would receive a bogus empty profile")
	}

	node.natProfile.Store(nat.Profile{Type: nat.TypePortRestricted, PublicReachable: true})
	signed := node.signedHolePunchCoordEnvelope(env)
	if signed.AdvertisedPeerID != nodeID {
		t.Fatal("signed envelope does not name its sender")
	}
	if signed.AdvertisedNATType != string(nat.TypePortRestricted) {
		t.Fatalf("signed envelope carries %q, want the node's profile", signed.AdvertisedNATType)
	}
	if len(signed.AdvertisedSignature) == 0 {
		t.Fatal("classified node sent an unsigned coordination envelope")
	}
	if !verifyHolePunchCoordEnvelope(signed) {
		t.Fatal("self-signed coordination envelope does not verify")
	}
}
