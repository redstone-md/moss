package mesh

import (
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/transport"
)

// TestSlowSenderDoesNotBlockOtherSenders is the regression pin for the directed
// delivery head-of-line fix: a single dispatchLoop used to invoke packetCB
// inline, so one application callback parked on sender A's DM froze the only
// consumer and every other sender's DMs dropped once dispatchCh filled. After
// the fix, deliverDirected shunts each payload to a per-sender worker, so
// sender A's slow callback cannot touch sender B's traffic.
func TestSlowSenderDoesNotBlockOtherSenders(t *testing.T) {
	node, err := NewNode("mesh-directed-hol", nil, isolatedTestConfig("directed-hol"))
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	if !transport.RunningGoTest() {
		t.Fatal("test build flag not set: the node would dial real discovery")
	}
	if code := node.Start(); code != MOSS_OK {
		t.Fatalf("Start: %d", code)
	}
	t.Cleanup(func() { node.Stop() })

	senderA := [32]byte{0xAA}
	senderB := [32]byte{0xBB}

	// sender A's first delivery blocks until the test releases it — standing in
	// for an application that decrypts or writes to disk slowly.
	release := make(chan struct{})
	aEntered := make(chan struct{})
	gotB := make(chan string, 8)

	node.SetPacketCallback(func(sender [32]byte, data []byte) {
		if sender == senderA {
			// Only the first A payload blocks; later ones drain fast.
			select {
			case <-aEntered:
			default:
				close(aEntered)
				<-release
			}
			return
		}
		if sender == senderB {
			gotB <- string(data)
		}
	})

	// Drive delivery as the transport read path does: the queue is keyed by
	// the authenticated session identity (queueKey), while the callback still
	// sees the claimed sender. Distinct sessions here, so A parking its own
	// worker must not touch B's. Before the fix A occupied the sole consumer
	// and B never arrived while it was parked.
	node.dispatchCh <- dispatchPacket{sender: senderA, queueKey: senderA, data: []byte("a-0")}

	// Wait until A's callback has actually entered (A's worker is now parked).
	select {
	case <-aEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("sender A's callback never ran")
	}

	for i := range 4 {
		node.dispatchCh <- dispatchPacket{sender: senderB, queueKey: senderB, data: []byte(string(rune('b' + i)))}
	}

	// B's payloads must be delivered even though A's callback is still parked.
	for i := range 4 {
		select {
		case got := <-gotB:
			if want := string(rune('b' + i)); got != want {
				t.Fatalf("sender B payload out of order: got %q want %q", got, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("sender B payload %d was not delivered while sender A parked — HOL not fixed", i)
		}
	}

	close(release)
}
