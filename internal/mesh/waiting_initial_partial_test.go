package mesh

import (
	"context"
	"testing"
	"time"
)

func TestWaitingInitialPartialMigrationRetainsCarrierBudget(t *testing.T) {
	core, next := startCappedOverlayNode(t, "initial-partial-core"), startCappedOverlayNode(t, "initial-partial-next")
	n := startCappedOverlayNode(t, "initial-partial-phone")
	a, b := startCappedOverlayNode(t, "initial-partial-app"), startCappedOverlayNode(t, "initial-partial-app")
	if a.localPeerID() > b.localPeerID() {
		a, b = b, a
	}
	b.AllowPeer(n.localPeerID())
	waitingRelayFixture(t, n, core)
	waitingRelayFixture(t, a, core)
	for _, peer := range []*Node{a, b} {
		attachLeaf(t, n, peer)
		deadline := time.Now().Add(time.Second)
		for n.peerByID(peer.localPeerID()) == nil || peer.peerByID(n.localPeerID()) == nil {
			if time.Now().After(deadline) {
				t.Fatal("fixture needs bidirectional physical admission before migration")
			}
			time.Sleep(time.Millisecond)
		}
		assertWaitingBidirectionalPackets(t, n, peer)
	}
	n.mu.Lock()
	n.activity.requested.Store(int32(ActivityWaiting))
	n.mu.Unlock()
	n.reconcileWaiting(context.Background())
	n.mu.RLock()
	partial := n.peers[a.localPeerID()]
	state := n.activity.actual == ActivityActive && n.activity.attachment == "" && len(n.activity.recovering) == 0
	n.mu.RUnlock()
	if !state || partial == nil || !partial.relayed || partial.viaPeerID != core.localPeerID() {
		t.Fatal("fixture did not retain a useful initial partial migration after later target refusal")
	}
	n.mu.Lock()
	n.activity.candidate, n.activity.candidateAddr = next.localPeerID(), nodeAddr(next)
	n.activity.attempt++
	attempt := n.activity.attempt
	n.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ctx = context.WithValue(ctx, waitingAttemptKey{}, attempt)
	n.retireUnusedWaitingCarriers(next.localPeerID(), attempt)
	_ = n.connectWaitingCandidate(ctx, nodeAddr(next), next.localPeerID())
	n.mu.RLock()
	count := n.directPeerCountLocked()
	n.mu.RUnlock()
	if count > 2 {
		t.Fatalf("initial partial migration lost its candidate ownership across failure: %d physical", count)
	}
	deadline := time.Now().Add(time.Second)
	for {
		local, remote := n.peerByID(a.localPeerID()), a.peerByID(n.localPeerID())
		if local != nil && remote != nil && local.relayed && remote.relayed && local.relaySessionID == remote.relaySessionID {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("preserved initial partial path did not finish its direct drain")
		}
		time.Sleep(time.Millisecond)
	}
	assertWaitingBidirectionalPackets(t, n, a)
	assertWaitingBidirectionalPackets(t, n, b)
}
