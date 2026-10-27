package mesh

import (
	"sort"
	"testing"
	"time"
)

func TestWaitingCarrierLossKeepsPreviousCommonCarrierInBoundedBatch(t *testing.T) {
	cores, phone, origin := previousCarrierFixture(t, false)
	previous := cores[0]
	// Only this physical leaf connection fails. The verified previous carrier
	// stays alive and still directly knows the ordinary application origin.
	lost := phone.peerByID(previous.localPeerID())
	lost.closeSession()
	started := time.Now()
	deadline := started.Add(phone.config.HandshakeTimeout() + relayMigrationGracePeriod + 250*time.Millisecond)
	for {
		phone.mu.RLock()
		ready := phone.activity.reachability == "ready" && phone.activity.attachment == previous.localPeerID() && phone.peers[previous.localPeerID()] != lost
		app := phone.peers[origin.localPeerID()]
		ready = ready && app != nil && app.relayed && app.viaPeerID == previous.localPeerID()
		phone.mu.RUnlock()
		if ready {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("bounded rotation excluded the only verified common carrier and entered outage backoff")
		}
		time.Sleep(time.Millisecond)
	}
	if state := readActivity(t, phone); state.Direct != 1 || state.Attachments != 1 {
		t.Fatal("recovery expanded waiting physical ownership")
	}
	assertWaitingAlternativePackets(t, origin, phone)
}

func previousCarrierFixture(t *testing.T, commonAlternative bool) ([]*Node, *Node, *Node) {
	t.Helper()
	cores := make([]*Node, 4)
	for i := range cores {
		cores[i] = startCappedOverlayNode(t, "previous-carrier-core")
	}
	sort.Slice(cores, func(i, j int) bool { return nodeAddr(cores[i]) < nodeAddr(cores[j]) })
	previous := cores[0]
	phone, origin := startCappedOverlayNode(t, "previous-carrier-phone"), startCappedOverlayNode(t, "previous-carrier-origin")
	unrelated := cores[1:]
	if commonAlternative {
		unrelated = cores[1:3]
	}
	for _, core := range unrelated {
		// These valid carriers cannot admit the origin; peer exchange must
		// not accidentally warm the missing application path during setup.
		core.AllowPeer(phone.localPeerID())
	}
	for _, core := range cores {
		waitingRelayFixture(t, phone, core)
	}
	waitingRelayFixture(t, origin, previous)
	if commonAlternative {
		waitingRelayFixture(t, origin, cores[3])
	}
	attachLeaf(t, origin, phone)
	assertWaitingBidirectionalPackets(t, origin, phone)
	phone.SetActivityMode(ActivityWaiting)
	awaitActivity(t, phone, "waiting", "ready")
	phone.mu.RLock()
	first := phone.activity.attachment
	phone.mu.RUnlock()
	if first != previous.localPeerID() {
		t.Fatal("fixture did not use the first common carrier")
	}
	for _, core := range unrelated {
		if core.peerByID(origin.localPeerID()) != nil {
			t.Fatal("unrelated carrier already knows application target")
		}
	}
	assertWaitingAlternativePackets(t, origin, phone)

	return cores, phone, origin
}

func TestWaitingDeadPreviousCarrierDoesNotTrapAlternativeRotation(t *testing.T) {
	cores, phone, origin := previousCarrierFixture(t, true)
	if code := cores[0].Stop(); code != MOSS_OK {
		t.Fatal(code)
	}
	deadline := time.Now().Add(2*phone.config.HandshakeTimeout() + 7*time.Second)
	for {
		phone.mu.RLock()
		ready := phone.activity.reachability == "ready" && phone.activity.attachment == cores[3].localPeerID()
		phone.mu.RUnlock()
		if ready {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("dead previous carrier trapped retries before the later healthy common candidate")
		}
		time.Sleep(time.Millisecond)
	}
	if state := readActivity(t, phone); state.Direct != 1 || state.Attachments != 1 {
		t.Fatal("dead-carrier recovery expanded physical budget")
	}
	assertWaitingAlternativePackets(t, origin, phone)
}

func TestWaitingReadmittedPreviousWithoutTargetDoesNotTrapRotation(t *testing.T) {
	cores, phone, origin := previousCarrierFixture(t, true)
	previous := cores[0]
	previous.AllowPeer(phone.localPeerID())
	lost := phone.peerByID(previous.localPeerID())
	lost.closeSession()
	deadline := time.Now().Add(time.Second)
	for phone.peerByID(previous.localPeerID()) == lost {
		if time.Now().After(deadline) {
			t.Fatal("physical carrier did not retire")
		}
		time.Sleep(time.Millisecond)
	}
	// The same authenticated carrier can readmit the phone, but current policy
	// no longer admits the application target. It is not a retained useful path.
	previous.DisallowPeer(origin.localPeerID())
	deadline = time.Now().Add(2*phone.config.HandshakeTimeout() + 7*time.Second)
	for {
		phone.mu.RLock()
		ready := phone.activity.reachability == "ready" && phone.activity.attachment == cores[3].localPeerID()
		phone.mu.RUnlock()
		if ready {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("unused readmitted previous carrier trapped later healthy common alternative")
		}
		time.Sleep(time.Millisecond)
	}
	if state := readActivity(t, phone); state.Direct != 1 || state.Attachments != 1 {
		t.Fatal("failed readmission expanded physical ownership")
	}
	assertWaitingAlternativePackets(t, origin, phone)
}
