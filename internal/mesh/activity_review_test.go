package mesh

import (
	"context"
	"testing"
	"time"
)

func TestActivityAbandonedPunchDoesNotCountTimeout(t *testing.T) {
	for _, reason := range []string{"waiting", "cancelled"} {
		t.Run(reason, func(t *testing.T) {
			n := startCappedOverlayNode(t, "punch-policy-cancel")
			addTrustedRelayCandidate(n, "relay-peer")
			n.mu.Lock()
			n.knownPeers["target-peer"] = knownPeer{id: "target-peer", addr: "203.0.113.9:45000"}
			n.mu.Unlock()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := make(chan bool, 1)
			go func() { result <- n.attemptHolePunchContext(ctx, "target-peer", 30*time.Second, false) }()
			waitFor(t, func() bool { return counterValue(n, "__punch_attempt__") == 1 }, "punch did not begin")
			if reason == "waiting" {
				n.SetActivityMode(ActivityWaiting)
			} else {
				cancel()
			}
			select {
			case connected := <-result:
				if connected || counterValue(n, "__punch_timeout__") != 0 {
					t.Fatal("an abandoned punch was recorded as a network timeout")
				}
			case <-time.After(time.Second):
				t.Fatal("abandoned punch did not finish")
			}
		})
	}
}

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
