package mesh

import (
	"context"
	"testing"
	"time"
)

func TestWaitingPartialMigrationCannotAdmitThirdCarrier(t *testing.T) {
	for _, used := range []bool{true, false} {
		t.Run(map[bool]string{true: "both-used", false: "former-unused"}[used], func(t *testing.T) { waitingPartialCarrierBudget(t, used) })
	}
}

func waitingPartialCarrierBudget(t *testing.T, used bool) {
	old, partial, next := startCappedOverlayNode(t, "partial-old"), startCappedOverlayNode(t, "partial-new"), startCappedOverlayNode(t, "partial-next")
	n, origin := startCappedOverlayNode(t, "partial-phone"), startCappedOverlayNode(t, "partial-origin")
	waitingRelayFixture(t, n, old)
	waitingRelayFixture(t, n, partial)
	waitingRelayFixture(t, origin, partial)
	openUpgradeFixtureRelay(t, partial, origin, n)
	assertWaitingBidirectionalPackets(t, origin, n)
	oldPeer := n.peerByID(old.localPeerID())
	oldPeer.applicationUsed.Store(used)
	n.mu.Lock()
	n.activity.requested.Store(int32(ActivityWaiting))
	n.activity.attachment = old.localPeerID()
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
		t.Fatalf("retained partial migration admitted a third physical carrier: %d", count)
	}
	if !used && (n.peerByID(old.localPeerID()) != nil || n.peerByID(next.localPeerID()) == nil) {
		t.Fatal("unused former attachment was not retired before the next candidate")
	}
	if used && n.peerByID(old.localPeerID()) != oldPeer {
		t.Fatal("working former attachment was retired to admit a third candidate")
	}
	assertWaitingBidirectionalPackets(t, origin, n)
}
