package mesh

import (
	"testing"
	"time"
)

func TestWaitingColdCarrierDoesNotTrustNoiseOnlyAdmission(t *testing.T) {
	carrier := startCappedOverlayNode(t, "waiting-noise-core")
	leaf := startCappedOverlayNode(t, "waiting-noise-leaf")
	waitingRelayFixture(t, leaf, carrier)
	_, negotiated, refuse := heldCarrierRefusalEndpoint(t, carrier)
	defer refuse()
	carrier.Stop()
	deadline := time.Now().Add(time.Second)
	for leaf.peerByID(carrier.localPeerID()) != nil {
		if time.Now().After(deadline) {
			t.Fatal("previous physical carrier did not retire")
		}
		time.Sleep(time.Millisecond)
	}
	leaf.mu.RLock()
	trusted := leaf.isTrustedRelayCandidateLocked(carrier.localPeerID())
	leaf.mu.RUnlock()
	if !trusted {
		t.Fatal("fixture lost its previously verified signed relay capability")
	}
	leaf.SetActivityMode(ActivityWaiting)
	select {
	case <-negotiated:
	case <-time.After(time.Second):
		t.Fatal("cold candidate did not complete real Noise handshake")
	}
	// The independent endpoint intentionally sends no mesh packet and has not
	// admitted this identity. Cached capability plus Noise alone is insufficient.
	deadline = time.Now().Add(250 * time.Millisecond)
	for time.Now().Before(deadline) {
		if state := readActivity(t, leaf); state.Reachability == "ready" {
			t.Fatal("waiting announced Ready for a Noise-only candidate before mesh admission")
		}
		time.Sleep(time.Millisecond)
	}
}
