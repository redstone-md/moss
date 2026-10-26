package mesh

import (
	"testing"
	"time"
)

func TestActivityWaitingDoesNotPruneRelayedPeerForSuspendedProbe(t *testing.T) {
	n, err := NewNode("waiting-pending-probe", nil, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	const topic, id = "waiting-topic", "relayed-member"
	n.pubsub.Subscribe(topic)
	n.pubsub.SetMeshPeer(topic, id, true)
	n.activity.requested.Store(int32(ActivityWaiting))
	n.peers[id] = &peerConn{
		id: id, relayed: true, pingPending: "before-waiting",
		pingSentAt: time.Now().Add(-peerPingTimeout - time.Second),
	}
	n.connTickProbeAndPrune(time.Now())
	if !n.pubsub.InMesh(topic, id) {
		t.Fatal("a suspended virtual-peer probe removed preserved room membership")
	}
	if n.peers[id].pingPending != "" || n.peers[id].pingMisses != 0 {
		t.Fatal("a probe suspended by waiting was charged as a missed response")
	}
}

func TestActivityWaitingCandidateCanUseAuthenticatedUDP(t *testing.T) {
	core := startCappedOverlayNode(t, "waiting-udp-core")
	n := startCappedOverlayNode(t, "waiting-udp-leaf")
	waitingRelayFixture(t, n, core)
	waitFor(t, func() bool { return core.directPeerConnected(n.localPeerID()) }, "core admission did not finish")
	core.mu.RLock()
	listener := core.listener
	core.mu.RUnlock()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	core.peerByID(n.localPeerID()).closeSession()
	waitFor(t, func() bool { return n.peerByID(core.localPeerID()) == nil }, "old TCP path did not close")
	if code := n.SetActivityMode(ActivityWaiting); code != MOSS_OK {
		t.Fatal(code)
	}
	state := awaitActivity(t, n, "waiting", "ready")
	if state.Attachments != 1 || n.peerByID(core.localPeerID()) == nil {
		t.Fatal("UDP-only carrier was not admitted and retained")
	}
}

func TestActivityRelayedDirectedTrafficRetainsApplicationPath(t *testing.T) {
	core := startCappedOverlayNode(t, "relay-app-core")
	a := startCappedOverlayNode(t, "relay-app-leaf")
	b := startCappedOverlayNode(t, "relay-app-leaf")
	waitingRelayFixture(t, a, core)
	waitingRelayFixture(t, b, core)
	waitFor(t, func() bool {
		return core.peerByID(a.localPeerID()) != nil && core.peerByID(b.localPeerID()) != nil
	}, "relay endpoint admission did not finish")
	received := make(chan struct{}, 1)
	b.SetPacketCallback(func(_ [32]byte, data []byte) {
		if string(data) == "application-message" {
			received <- struct{}{}
		}
	})
	if a.peerByID(b.localPeerID()) != nil {
		t.Fatal("fixture must resolve the absent target through its relay")
	}
	if err := a.SendToPeer(b.localPeerID(), []byte("application-message"), time.Second); err != nil {
		t.Fatal(err)
	}
	select {
	case <-received:
	case <-time.After(time.Second):
		t.Fatal("directed relay payload was not delivered")
	}
	for _, pair := range [][2]*Node{{a, b}, {b, a}} {
		peer := pair[0].peerByID(pair[1].localPeerID())
		if peer == nil || !peer.relayed || !peer.applicationUsed.Load() {
			t.Fatal("directed relay traffic was omitted from required application paths")
		}
	}
}
