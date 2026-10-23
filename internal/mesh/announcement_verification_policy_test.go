package mesh

import (
	"testing"

	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/nat"
)

func announcementTestNode(t *testing.T) *Node {
	t.Helper()
	node, err := NewNode("announcement-memo", nil, DefaultOfflineConfig())
	if err != nil {
		t.Fatal(err)
	}
	return node
}

func TestWarmStatusChecksEverySignedCapabilityField(t *testing.T) {
	node, signer := announcementTestNode(t), announcementTestNode(t)
	env := signer.signSupernodeEnvelope(gossip.Envelope{
		Type: gossip.TypeSupernodeAnnounce, AdvertisedPeerID: signer.localPeerID(),
		AdvertisedAddr: "203.0.113.20:55005", AdvertisedNATType: string(nat.TypePublic),
		AdvertisedReachable: true, AdvertisedRelayCapable: true,
	})
	if !verifySupernodeStatusEnvelope(env, node.verifyAnnouncementSignature) {
		t.Fatal("fixture rejected")
	}
	changes := []func(*gossip.Envelope){
		func(e *gossip.Envelope) { e.Type = gossip.TypeSupernodeRevoke },
		func(e *gossip.Envelope) { e.AdvertisedAddr = "203.0.113.21:55005" },
		func(e *gossip.Envelope) { e.AdvertisedNATType = string(nat.TypeSymmetric) },
		func(e *gossip.Envelope) { e.AdvertisedReachable = false },
		func(e *gossip.Envelope) { e.AdvertisedRelayCapable = false },
	}
	for index, change := range changes {
		changed := env
		change(&changed)
		if verifySupernodeStatusEnvelope(changed, node.verifyAnnouncementSignature) {
			t.Fatalf("warm verification ignored changed signed field %d", index)
		}
		changed = signer.signSupernodeEnvelope(changed)
		if !verifySupernodeStatusEnvelope(changed, node.verifyAnnouncementSignature) {
			t.Fatalf("fresh signature for changed field %d rejected", index)
		}
	}
}

func TestWarmStatusStillAppliesCapabilityRevocation(t *testing.T) {
	node, signer := announcementTestNode(t), announcementTestNode(t)
	env := signer.signSupernodeEnvelope(gossip.Envelope{
		Type: gossip.TypeSupernodeAnnounce, AdvertisedPeerID: signer.localPeerID(),
		AdvertisedAddr: "203.0.113.20:55005", AdvertisedNATType: string(nat.TypePublic),
		AdvertisedReachable: true, AdvertisedRelayCapable: true,
	})
	peer := &peerConn{id: signer.localPeerID(), addr: env.AdvertisedAddr}
	node.handleSupernodeStatus(peer, env, true)
	node.handleSupernodeStatus(peer, env, true)
	if !node.knownPeers[peer.id].relayCapable {
		t.Fatal("valid relay capability not applied")
	}
	env.Type, env.AdvertisedRelayCapable = gossip.TypeSupernodeRevoke, false
	node.handleSupernodeStatus(peer, env, false)
	if !node.knownPeers[peer.id].relayCapable {
		t.Fatal("old signature admitted changed capability claims")
	}
	env = signer.signSupernodeEnvelope(env)
	node.handleSupernodeStatus(peer, env, false)
	node.handleSupernodeStatus(peer, env, false)
	if node.knownPeers[peer.id].relayCapable {
		t.Fatal("warm verification skipped a signed capability revocation")
	}
}

func TestWarmPeerAnnouncementDoesNotAuthenticateUnsignedCapabilities(t *testing.T) {
	node, signer := announcementTestNode(t), announcementTestNode(t)
	env := signer.peerAnnouncementEnvelope(knownPeer{id: signer.localPeerID(), addr: "203.0.113.20:55005"})
	peer := &peerConn{id: signer.localPeerID(), addr: env.AdvertisedAddr}
	node.handlePeerAnnounce(peer, env)
	env.AdvertisedNATType, env.AdvertisedReachable, env.AdvertisedRelayCapable = string(nat.TypePublic), true, true
	node.handlePeerAnnounce(peer, env)
	known := node.knownPeers[peer.id]
	if known.natTrusted || known.publicReachable || known.relayCapable {
		t.Fatal("cached peer-announcement proof authenticated unsigned capability fields")
	}
}
