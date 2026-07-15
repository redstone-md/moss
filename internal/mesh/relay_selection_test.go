package mesh

import (
	"testing"

	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/nat"
)

func TestSelectRelayPeerPrefersRelayCapablePeer(t *testing.T) {
	node, err := NewNode("mesh-relay-rank", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}

	node.peers["peer-private"] = &peerConn{id: "peer-private"}
	node.peers["peer-public"] = &peerConn{id: "peer-public"}
	node.knownPeers["peer-private"] = knownPeer{
		id:              "peer-private",
		addr:            "10.0.0.10:4000",
		natType:         nat.TypeRestrictedCone,
		publicReachable: false,
		relayCapable:    false,
	}
	node.knownPeers["peer-public"] = knownPeer{
		id:              "peer-public",
		addr:            "198.51.100.20:4000",
		natType:         nat.TypePublic,
		natTrusted:      true,
		publicReachable: true,
		relayCapable:    true,
	}
	node.scoring.SetApplicationScore("peer-private", 10)
	node.scoring.SetApplicationScore("peer-public", 1)

	selected, err := node.selectRelayPeer("target-peer")
	if err != nil {
		t.Fatalf("selectRelayPeer failed: %v", err)
	}
	if selected != "peer-public" {
		t.Fatalf("expected relay-capable peer to be selected, got %s", selected)
	}
}

func TestSelectRelayPeerFallsBackToScoreWhenCapabilityMatches(t *testing.T) {
	node, err := NewNode("mesh-relay-score", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}

	node.peers["peer-a"] = &peerConn{id: "peer-a"}
	node.peers["peer-b"] = &peerConn{id: "peer-b"}
	node.knownPeers["peer-a"] = knownPeer{
		id:              "peer-a",
		addr:            "198.51.100.30:4000",
		natType:         nat.TypePublic,
		natTrusted:      true,
		publicReachable: true,
		relayCapable:    true,
	}
	node.knownPeers["peer-b"] = knownPeer{
		id:              "peer-b",
		addr:            "203.0.113.30:4000",
		natType:         nat.TypePublic,
		natTrusted:      true,
		publicReachable: true,
		relayCapable:    true,
	}
	node.scoring.SetApplicationScore("peer-a", 1)
	node.scoring.SetApplicationScore("peer-b", 5)

	selected, err := node.selectRelayPeer("target-peer")
	if err != nil {
		t.Fatalf("selectRelayPeer failed: %v", err)
	}
	if selected != "peer-b" {
		t.Fatalf("expected higher-scored peer to be selected, got %s", selected)
	}
}

func TestSelectRelayPeersReturnsOrderedCandidates(t *testing.T) {
	node, err := NewNode("mesh-relay-order", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}

	node.peers["peer-a"] = &peerConn{id: "peer-a"}
	node.peers["peer-b"] = &peerConn{id: "peer-b"}
	node.peers["peer-c"] = &peerConn{id: "peer-c"}
	node.knownPeers["peer-a"] = knownPeer{
		id:              "peer-a",
		addr:            "198.51.100.10:4000",
		natType:         nat.TypePublic,
		natTrusted:      true,
		publicReachable: true,
		relayCapable:    true,
	}
	node.knownPeers["peer-b"] = knownPeer{
		id:              "peer-b",
		addr:            "198.51.100.11:4000",
		natType:         nat.TypePublic,
		natTrusted:      true,
		publicReachable: true,
		relayCapable:    true,
	}
	node.knownPeers["peer-c"] = knownPeer{
		id:              "peer-c",
		addr:            "10.0.0.12:4000",
		natType:         nat.TypeRestrictedCone,
		publicReachable: false,
		relayCapable:    false,
	}
	node.scoring.SetApplicationScore("peer-a", 1)
	node.scoring.SetApplicationScore("peer-b", 5)
	node.scoring.SetApplicationScore("peer-c", 20)

	candidates, err := node.selectRelayPeers("target-peer")
	if err != nil {
		t.Fatalf("selectRelayPeers failed: %v", err)
	}
	if len(candidates) != 2 {
		t.Fatalf("expected 2 relay candidates, got %d", len(candidates))
	}
	if candidates[0] != "peer-b" || candidates[1] != "peer-a" {
		t.Fatalf("unexpected relay candidate order: %v", candidates)
	}
}

func TestSelectRelayPeersPrefersLessLoadedRelay(t *testing.T) {
	node, err := NewNode("mesh-relay-load", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}

	node.peers["peer-a"] = &peerConn{id: "peer-a"}
	node.peers["peer-b"] = &peerConn{id: "peer-b"}
	node.knownPeers["peer-a"] = knownPeer{
		id:              "peer-a",
		addr:            "198.51.100.10:4000",
		natType:         nat.TypePublic,
		natTrusted:      true,
		publicReachable: true,
		relayCapable:    true,
	}
	node.knownPeers["peer-b"] = knownPeer{
		id:              "peer-b",
		addr:            "198.51.100.11:4000",
		natType:         nat.TypePublic,
		natTrusted:      true,
		publicReachable: true,
		relayCapable:    true,
	}
	node.scoring.SetApplicationScore("peer-a", 10)
	node.scoring.SetApplicationScore("peer-b", 10)
	node.relayLocals["session-1"] = relayLocalSession{sessionID: "session-1", viaPeerID: "peer-a", remotePeerID: "target-1", established: true}
	node.relayLocals["session-2"] = relayLocalSession{sessionID: "session-2", viaPeerID: "peer-a", remotePeerID: "target-2", established: true}

	candidates, err := node.selectRelayPeers("target-peer")
	if err != nil {
		t.Fatalf("selectRelayPeers failed: %v", err)
	}
	if candidates[0] != "peer-b" {
		t.Fatalf("expected less-loaded relay peer-b first, got %v", candidates)
	}
}

func TestSelectRelayPeersKeepsScoreAheadOfLoad(t *testing.T) {
	node, err := NewNode("mesh-relay-score-load", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}

	node.peers["trusted-relay"] = &peerConn{id: "trusted-relay"}
	node.peers["risky-relay"] = &peerConn{id: "risky-relay"}
	node.knownPeers["trusted-relay"] = knownPeer{
		id:              "trusted-relay",
		addr:            "198.51.100.10:4000",
		natType:         nat.TypePublic,
		natTrusted:      true,
		publicReachable: true,
		relayCapable:    true,
	}
	node.knownPeers["risky-relay"] = knownPeer{
		id:              "risky-relay",
		addr:            "198.51.100.11:4000",
		natType:         nat.TypePublic,
		natTrusted:      true,
		publicReachable: true,
		relayCapable:    true,
	}
	node.scoring.SetApplicationScore("trusted-relay", 1000)
	node.scoring.SetApplicationScore("risky-relay", gossip.PublishThreshold-1)
	node.relayLocals["session-1"] = relayLocalSession{sessionID: "session-1", viaPeerID: "trusted-relay", remotePeerID: "target-1", established: true}

	candidates, err := node.selectRelayPeers("target-peer")
	if err != nil {
		t.Fatalf("selectRelayPeers failed: %v", err)
	}
	if candidates[0] != "trusted-relay" {
		t.Fatalf("expected higher-scored relay first despite load, got %v", candidates)
	}
}

func TestPeerAnnounceCannotUpgradeRelayCapabilities(t *testing.T) {
	node, err := NewNode("mesh-relay-announce", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	attackerNode, err := NewNode("mesh-relay-announce", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("attacker NewNode failed: %v", err)
	}
	attackerID := attackerNode.localPeerID()

	node.peers[attackerID] = &peerConn{id: attackerID, addr: "203.0.113.10:4000"}
	node.knownPeers[attackerID] = knownPeer{
		id:           attackerID,
		addr:         "203.0.113.10:4000",
		direct:       true,
		natType:      nat.TypeRestrictedCone,
		relayCapable: false,
	}
	node.scoring.SetApplicationScore(attackerID, 100)

	selfSignedAnnounce := attackerNode.signSupernodeEnvelope(gossip.Envelope{
		Type:                   gossip.TypePeerAnnounce,
		AdvertisedPeerID:       attackerID,
		AdvertisedAddr:         "203.0.113.10:4000",
		AdvertisedNATType:      string(nat.TypePublic),
		AdvertisedReachable:    true,
		AdvertisedRelayCapable: true,
	})
	node.handlePeerAnnounce(node.peers[attackerID], selfSignedAnnounce)

	info := node.knownPeers[attackerID]
	if info.relayCapable {
		t.Fatalf("expected self-signed peer announce to leave relay capability unchanged")
	}
	if info.publicReachable {
		t.Fatalf("expected self-signed peer announce to leave reachability unchanged")
	}
	if info.natType != nat.TypeRestrictedCone {
		t.Fatalf("expected self-signed peer announce to leave NAT type unchanged, got %s", info.natType)
	}

	node.peers["honest"] = &peerConn{id: "honest", addr: "198.51.100.20:4000"}
	node.knownPeers["honest"] = knownPeer{
		id:              "honest",
		addr:            "198.51.100.20:4000",
		direct:          true,
		natType:         nat.TypePublic,
		natTrusted:      true,
		publicReachable: true,
		relayCapable:    true,
	}
	node.scoring.SetApplicationScore("honest", 1)

	selected, err := node.selectRelayPeer("target-peer")
	if err != nil {
		t.Fatalf("selectRelayPeer failed: %v", err)
	}
	if selected != "honest" {
		t.Fatalf("expected honest relay-capable peer to be selected, got %s", selected)
	}
}

func TestSelectRelayPeersRejectsUntrustedRelayCapablePeer(t *testing.T) {
	node, err := NewNode("mesh-relay-untrusted", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	node.peers["untrusted"] = &peerConn{id: "untrusted"}
	node.knownPeers["untrusted"] = knownPeer{
		id:              "untrusted",
		addr:            "198.51.100.99:4000",
		natType:         nat.TypePublic,
		publicReachable: true,
		relayCapable:    true,
	}
	node.scoring.SetApplicationScore("untrusted", 100)

	if _, err := node.selectRelayPeers("target-peer"); err == nil {
		t.Fatal("expected untrusted relay-capable peer to be rejected")
	}
}
