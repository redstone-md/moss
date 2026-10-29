package mesh

import (
	"context"
	"sort"
	"testing"
	"time"
)

func TestWaitingConnectedUnrelatedCarrierLeavesBudgetForCommonCarrier(t *testing.T) {
	cores := []*Node{startCappedOverlayNode(t, "connected-alternative-core"), startCappedOverlayNode(t, "connected-alternative-core")}
	sort.Slice(cores, func(i, j int) bool { return nodeAddr(cores[i]) < nodeAddr(cores[j]) })
	unrelated, common := cores[0], cores[1]
	phone, origin := startCappedOverlayNode(t, "connected-alternative-phone"), startCappedOverlayNode(t, "connected-alternative-origin")
	for _, core := range cores {
		waitingRelayFixture(t, phone, core)
	}
	waitingRelayFixture(t, origin, common)
	attachLeaf(t, origin, phone)
	assertWaitingBidirectionalPackets(t, origin, phone)
	if unrelated.peerByID(origin.localPeerID()) != nil {
		t.Fatal("fixture unrelated carrier already knows the application target")
	}
	phone.mu.Lock()
	phone.activity.requested.Store(int32(ActivityWaiting))
	first, _ := phone.waitingCandidateLocked()
	phone.mu.Unlock()
	if first != unrelated.localPeerID() {
		t.Fatal("fixture did not select the unrelated connected carrier first")
	}
	started := time.Now()
	phone.reconcileWaiting(context.Background())
	state := readActivity(t, phone)
	if state.Reachability != "ready" || state.Attachments != 1 || state.Direct != 1 {
		t.Fatalf("unrelated connected carrier consumed healthy common carrier budget: reach=%s direct=%d elapsed=%s", state.Reachability, state.Direct, time.Since(started))
	}
	if time.Since(started) > phone.config.HandshakeTimeout()+relayMigrationGracePeriod+250*time.Millisecond {
		t.Fatal("repair extended its parent deadline")
	}
	deadline := time.Now().Add(time.Second)
	for {
		a, b := origin.peerByID(phone.localPeerID()), phone.peerByID(origin.localPeerID())
		if a != nil && b != nil && a.relayed && b.relayed && a.viaPeerID == common.localPeerID() && a.relaySessionID == b.relaySessionID {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("healthy common carrier did not preserve both application routes")
		}
		time.Sleep(time.Millisecond)
	}
	assertWaitingAlternativePackets(t, origin, phone)
}

func TestWaitingUsefulCarrierRetainsItsFullMigrationBudget(t *testing.T) {
	cores := []*Node{startCappedOverlayNode(t, "useful-alternative-core"), startCappedOverlayNode(t, "useful-alternative-core")}
	sort.Slice(cores, func(i, j int) bool { return nodeAddr(cores[i]) < nodeAddr(cores[j]) })
	useful := cores[0]
	phone, origin := startCappedOverlayNode(t, "useful-alternative-phone"), startCappedOverlayNode(t, "useful-alternative-origin")
	for _, core := range cores {
		waitingRelayFixture(t, phone, core)
	}
	waitingRelayFixture(t, origin, useful)
	attachLeaf(t, origin, phone)
	assertWaitingBidirectionalPackets(t, origin, phone)
	// This carrier also carries actual application traffic. A late remote
	// acceptance must still get the original full parent migration budget.
	assertWaitingBidirectionalPackets(t, phone, useful)
	phone.mu.Lock()
	phone.activity.requested.Store(int32(ActivityWaiting))
	phone.mu.Unlock()
	origin.mu.Lock()
	unlocked := make(chan struct{})
	go func() {
		time.Sleep(3 * phone.config.HandshakeTimeout() / 4)
		origin.mu.Unlock()
		close(unlocked)
	}()
	defer func() { <-unlocked }()
	started := time.Now()
	phone.reconcileWaiting(context.Background())
	state := readActivity(t, phone)
	if state.Reachability != "ready" || state.Attachments != 1 || state.Direct != 1 {
		t.Fatalf("useful retained carrier lost its full migration budget: reach=%s direct=%d elapsed=%s", state.Reachability, state.Direct, time.Since(started))
	}
	if p := phone.peerByID(origin.localPeerID()); p == nil || !p.relayed || p.viaPeerID != useful.localPeerID() {
		t.Fatal("late authenticated acceptance was replaced by an unrelated carrier")
	}
	assertWaitingAlternativePackets(t, origin, phone)
	assertWaitingAlternativePackets(t, phone, useful)
}
