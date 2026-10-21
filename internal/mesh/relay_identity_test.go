package mesh

import (
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/nat"
)

func relayIdentityFixture(t *testing.T) (*Node, *Node, *peerConn, gossip.Envelope) {
	t.Helper()
	node, remote, _ := peerExchangeFixture(t, 0)
	var remoteID, remoteKey [32]byte
	copy(remoteID[:], remote.identity.PublicKeyBytes())
	copy(remoteKey[:], remote.identity.NoiseStaticPublic())
	peer := &peerConn{id: remote.localPeerID(), session: newRecordedSessionForPeer(t, remoteID, remoteKey).session}
	node.peers[peer.id] = peer
	claim := remote.peerAnnouncementEnvelope(knownPeer{
		id: peer.id, addr: "203.0.113.10:45000", noiseStatic: remote.identity.NoiseStaticPublic(),
	})
	return node, remote, peer, claim
}

func TestFailedRelaySetupWritesLeaveNoSessionOrRoute(t *testing.T) {
	node, remote, peer, _ := relayIdentityFixture(t)
	node.peers["unwritable"] = &peerConn{id: "unwritable"}
	if _, err := node.OpenRelaySession("unwritable", remote.localPeerID(), time.Second); err == nil {
		t.Fatal("failed relay identity write reported success")
	}
	if len(node.relayLocals) != 0 {
		t.Fatal("failed outgoing setup retained its pending session")
	}
	node.handleRelayRequest(peer, remote.signRelayRequestEnvelope(gossip.Envelope{
		Type: gossip.TypeRelayRequest, RelaySession: "failed-forward", RelaySource: peer.id, RelayTarget: "unwritable",
	}))
	if len(node.relayRoutes) != 0 || node.relaySessions.Count() != 0 {
		t.Fatal("failed forwarding write retained relay capacity")
	}
}

func TestRelayIdentitySurvivesStatusAndOwnsClaim(t *testing.T) {
	node, remote, peer, claim := relayIdentityFixture(t)
	node.handlePeerAnnounce(peer, claim)
	node.handleEnvelope(peer, remote.signSupernodeEnvelope(gossip.Envelope{
		Type: gossip.TypeSupernodeAnnounce, AdvertisedPeerID: peer.id, AdvertisedAddr: claim.AdvertisedAddr,
		AdvertisedNATType: string(nat.TypePublic), AdvertisedReachable: true, AdvertisedRelayCapable: true,
	}))
	claim.AdvertisedSignature[0] ^= 1
	claim.AdvertisedNoiseStatic[0] ^= 1
	if peer.selfAnnouncement == nil || !verifyPeerAnnouncementEnvelope(*peer.selfAnnouncement) {
		t.Fatal("relay lost the original self-signed key claim or retained mutable caller slices")
	}
	recipient := newRecordedSession(t)
	if !node.introduceRelayEndpoint(&peerConn{session: recipient.session}, peer.id) || recipient.writeCount() != 1 {
		t.Fatal("relay could not backfill the authenticated endpoint claim")
	}
}

func TestRelayIdentityRejectsUnboundClaims(t *testing.T) {
	for _, kind := range []string{"signature", "key", "legacy", "third-party", "retired"} {
		t.Run(kind, func(t *testing.T) {
			node, remote, peer, claim := relayIdentityFixture(t)
			switch kind {
			case "signature":
				claim.AdvertisedSignature[0] ^= 1
			case "key":
				claim.AdvertisedNoiseStatic[0] ^= 1
				claim = remote.signPeerAnnouncementEnvelope(claim)
			case "legacy":
				claim.AdvertisedNoiseStatic = nil
				claim = remote.signPeerAnnouncementEnvelope(claim)
			case "third-party":
				peer.id = "forwarder"
			case "retired":
				node.peers[peer.id] = &peerConn{id: peer.id}
			}
			node.handlePeerAnnounce(peer, claim)
			if peer.selfAnnouncement != nil {
				t.Fatal("unbound or retired claim became a relay identity")
			}
		})
	}
}

func TestRelayAcceptBeforeIdentityCannotReportReady(t *testing.T) {
	node, remote, peer, claim := relayIdentityFixture(t)
	accept := remote.signRelayAcceptEnvelope(gossip.Envelope{
		Type: gossip.TypeRelayAccept, RelaySession: "reordered", RelaySource: remote.localPeerID(), RelayTarget: node.localPeerID(),
	})
	wait := make(chan struct{})
	node.relayLocals[accept.RelaySession] = relayLocalSession{
		sessionID: accept.RelaySession, viaPeerID: peer.id, remotePeerID: remote.localPeerID(), wait: wait,
	}
	node.handleRelayAccept(peer, accept)
	select {
	case <-wait:
		t.Fatal("accept without a Noise key falsely reported relay readiness")
	default:
	}
	if _, exists := node.relayLocals[accept.RelaySession]; exists {
		t.Fatal("missing identity left a half-open relay session")
	}
	// An authenticated late claim enables a normal retry, never plaintext.
	forwarder := &peerConn{id: "relay"}
	node.handlePeerAnnounce(forwarder, claim)
	node.peers[forwarder.id] = forwarder
	node.relayLocals[accept.RelaySession] = relayLocalSession{
		sessionID: accept.RelaySession, viaPeerID: forwarder.id, remotePeerID: remote.localPeerID(), wait: wait,
	}
	delete(node.peers, remote.localPeerID())
	node.handleRelayAccept(forwarder, accept)
	select {
	case <-wait:
	default:
		t.Fatal("authenticated identity did not enable retry")
	}
	node.handleRelayClose(forwarder, gossip.Envelope{
		Type: gossip.TypeRelayClose, RelaySession: accept.RelaySession, RelayTarget: node.localPeerID(),
	})
	if node.peerByID(remote.localPeerID()) != nil {
		t.Fatal("closing a retried relay left an unusable peer in the routing table")
	}
}
