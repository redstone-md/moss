package mesh

import (
	"testing"
	"time"
)

func TestActivityWaitingRetainsStreamRegistrationWithoutRetainingUnusedMeshPeers(t *testing.T) {
	relay := startCappedOverlayNode(t, "waiting-stream-relay")
	leaf := startCappedOverlayNode(t, "waiting-stream-leaf")
	warm := startCappedOverlayNode(t, "waiting-stream-warm")
	waitingRelayFixture(t, leaf, relay)
	attachLeaf(t, leaf, warm)
	received := make(chan string, 1)
	if code := leaf.OnStream(77, func(peerID string, data []byte) { received <- peerID + ":" + string(data) }); code != MOSS_OK {
		t.Fatal(code)
	}
	if code := leaf.SetActivityMode(ActivityWaiting); code != MOSS_OK {
		t.Fatal(code)
	}
	state := awaitActivity(t, leaf, "waiting", "ready")
	if state.Direct != 1 {
		t.Fatalf("unused stream readers retained mesh: %+v", state)
	}
	leaf.SetActivityMode(ActivityActive)
	awaitActivity(t, leaf, "active", "unavailable")
	if code := leaf.Connect(nodeAddr(warm)); code != MOSS_OK {
		t.Fatal(code)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if code := warm.SendStream(leaf.localPeerID(), 77, []byte("after-resume")); code == MOSS_OK {
			break
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case got := <-received:
		if got != warm.localPeerID()+":after-resume" {
			t.Fatal(got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("registered handler was lost across transport replacement")
	}
}
