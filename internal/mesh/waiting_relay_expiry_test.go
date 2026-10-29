package mesh

import (
	"testing"
	"time"
)

func TestOrdinaryOriginRecoversSilentRelayRouteLoss(t *testing.T) {
	carrier, origin, receiver := silentRelayFixture(t)
	peer := origin.peerByID(receiver.localPeerID())
	session := peer.relaySessionID
	// Legacy carriers can discard a forwarding route without a Close reply.
	// Its physical connections remain healthy; end-to-end probes must detect it.
	carrier.mu.Lock()
	delete(carrier.relayRoutes, session)
	delete(carrier.relayRouteExpiry, session)
	carrier.relaySessions.Release(session)
	carrier.mu.Unlock()
	lists := expiredVirtualProbe(t, origin, peer)
	origin.applyPruneLists(lists)
	origin.mu.RLock()
	retry := origin.explicitTargets[receiver.localPeerID()].identityRetry
	origin.mu.RUnlock()
	if !retry {
		t.Error("failed current virtual path did not request ordinary identity recovery")
	}
	if origin.peerByID(carrier.localPeerID()) == nil {
		t.Fatal("virtual failure closed healthy physical carrier")
	}
	// The real directed API must establish a fresh route and deliver both ways.
	assertWaitingBidirectionalPackets(t, origin, receiver)
	if current := origin.peerByID(receiver.localPeerID()); current == nil || current.relaySessionID == session {
		t.Fatal("application reused the dead virtual session")
	}
}

func TestStaleVirtualPruneKeepsReplacementAndTopic(t *testing.T) {
	carrier, origin, receiver := silentRelayFixture(t)
	old := origin.peerByID(receiver.localPeerID())
	lists := expiredVirtualProbe(t, origin, old)
	if _, err := origin.OpenRelaySession(carrier.localPeerID(), receiver.localPeerID(), time.Second); err != nil {
		t.Fatal(err)
	}
	current := origin.peerByID(receiver.localPeerID())
	if current == old {
		t.Fatal("fixture did not replace virtual incarnation")
	}
	const topic = "new-relay-topic"
	origin.pubsub.Subscribe(topic)
	origin.pubsub.SetMeshPeer(topic, receiver.localPeerID(), true)
	origin.applyPruneLists(lists)
	origin.mu.RLock()
	preserved := origin.peers[receiver.localPeerID()] == current && current.meshBlocked.IsZero()
	origin.mu.RUnlock()
	if !preserved || !origin.pubsub.InMesh(topic, receiver.localPeerID()) {
		t.Fatal("stale prune changed the replacement virtual peer or its topic")
	}
	assertWaitingBidirectionalPackets(t, origin, receiver)
}

func TestActiveVirtualPruneCannotRetireAfterWaiting(t *testing.T) {
	_, origin, receiver := silentRelayFixture(t)
	peer := origin.peerByID(receiver.localPeerID())
	lists := expiredVirtualProbe(t, origin, peer)
	origin.SetActivityMode(ActivityWaiting)
	awaitActivity(t, origin, "waiting", "ready")
	origin.applyPruneLists(lists)
	if origin.peerByID(receiver.localPeerID()) != peer {
		t.Fatal("an old active probe retired waiting virtual membership")
	}
	assertWaitingBidirectionalPackets(t, origin, receiver)
}

func silentRelayFixture(t *testing.T) (*Node, *Node, *Node) {
	t.Helper()
	carrier, origin, receiver := startCappedOverlayNode(t, "silent-core"), startCappedOverlayNode(t, "silent-leaf"), startCappedOverlayNode(t, "silent-leaf")
	waitingRelayFixture(t, origin, carrier)
	waitingRelayFixture(t, receiver, carrier)
	receiver.SetActivityMode(ActivityWaiting)
	awaitActivity(t, receiver, "waiting", "ready")
	openUpgradeFixtureRelay(t, carrier, origin, receiver)
	origin.ConnectToPeer(receiver.localPeerID())
	assertWaitingBidirectionalPackets(t, origin, receiver)
	return carrier, origin, receiver
}

// Advance the existing probe clock to the real miss limit, not a new timeout.
func expiredVirtualProbe(t *testing.T, n *Node, peer *peerConn) pruneLists {
	t.Helper()
	now := time.Now()
	n.mu.Lock()
	defer n.mu.Unlock()
	if peer == nil || !peer.relayed || n.peers[peer.id] != peer {
		t.Fatal("fixture requires the current established virtual peer")
	}
	peer.connectedAt = now.Add(-time.Minute)
	peer.pingPending = "expired-end-to-end-probe"
	peer.pingSentAt = now.Add(-peerPingTimeout - time.Second)
	peer.pingMisses = peerDisconnectMissLimit - 1
	return n.collectPruneLocked(now)
}
