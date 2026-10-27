package mesh

import (
	"testing"
	"time"
)

func TestWaitingAlternativesCancelWithoutDialingNext(t *testing.T) {
	for _, stop := range []bool{false, true} {
		t.Run(map[bool]string{false: "active", true: "stop"}[stop], func(t *testing.T) {
			bad, fresh := startCappedOverlayNode(t, "cancel-core"), startCappedOverlayNode(t, "cancel-core")
			if nodeAddr(bad) > nodeAddr(fresh) {
				bad, fresh = fresh, bad
			}
			n := startCappedOverlayNode(t, "cancel-leaf")
			waitingRelayFixture(t, n, bad)
			waitingRelayFixture(t, n, fresh)
			negotiated := refuseAlternativeCarrierAdmissions(t, bad, true)
			bad.Stop()
			if peer := n.peerByID(fresh.localPeerID()); peer != nil {
				peer.closeSession()
			}
			deadline := time.Now().Add(time.Second)
			for len(connectedDirectIDs(t, n)) != 0 {
				if time.Now().After(deadline) {
					t.Fatal("old fixture candidates did not retire")
				}
				time.Sleep(time.Millisecond)
			}
			n.SetActivityMode(ActivityWaiting)
			select {
			case <-negotiated:
			case <-time.After(time.Second):
				t.Fatal("first real Noise candidate did not negotiate")
			}
			done := make(chan struct{})
			go func() {
				if stop {
					n.Stop()
				} else {
					n.SetActivityMode(ActivityActive)
				}
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(500 * time.Millisecond):
				t.Fatal("cancellation did not release the candidate batch")
			}
			if !stop {
				awaitActivity(t, n, "active", "unavailable")
			}
			if n.peerByID(fresh.localPeerID()) != nil {
				t.Fatal("cancelled Waiting batch admitted its next candidate")
			}
		})
	}
}
