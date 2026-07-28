package mesh

import (
	"sync"
	"testing"
	"time"
)

// Delivery to the application is a synchronous callback that decrypts and
// writes to disk. It used to be fed straight from the read loop, so a file
// transfer's chunks filled the shared queue, the send blocked, the read loop
// stopped, and the transport's inbound buffer overflowed — discarding whatever
// arrived next, pings included. Measured on a live pair: 36 dropped packets,
// transfers stuck at 63%, and sessions dying at 37s on a healthy link.
//
// Enqueueing must therefore never block, however slow the application is.
func TestSlowDeliveryNeverBlocksTheReader(t *testing.T) {
	node, err := NewNode("mesh-local-delivery", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	release := make(chan struct{})
	var once sync.Once
	node.SetMessageCallback(func(string, [32]byte, []byte) {
		<-release // the application is busy for as long as we like
	})
	defer once.Do(func() { close(release) })

	done := make(chan struct{})
	go func() {
		defer close(done)
		// Comfortably more than one queue's worth: past the depth the sends
		// must still return, dropping rather than waiting.
		for i := 0; i < localDeliveryQueueDepth+64; i++ {
			node.enqueueLocal(dispatchMessage{channel: "chat", data: []byte("x")})
		}
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("enqueueLocal blocked while the application was slow — the read " +
			"loop stalls behind it and the transport buffer overflows")
	}
}

// One queue per channel. A blob transfer that occupies its own worker must not
// delay control traffic, which is the only reason ordering is kept per channel
// rather than globally.
func TestASlowChannelDoesNotDelayAnother(t *testing.T) {
	node, err := NewNode("mesh-local-delivery-isolation", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	blocked := make(chan struct{})
	arrived := make(chan string, 4)
	var once sync.Once
	node.SetMessageCallback(func(channel string, _ [32]byte, _ []byte) {
		if channel == "blob" {
			<-blocked
			return
		}
		arrived <- channel
	})
	defer once.Do(func() { close(blocked) })

	node.enqueueLocal(dispatchMessage{channel: "blob", data: []byte("chunk")})
	node.enqueueLocal(dispatchMessage{channel: "control", data: []byte("keypkg")})

	select {
	case got := <-arrived:
		if got != "control" {
			t.Fatalf("delivered %q, want the control message", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a stalled blob transfer held up control traffic — the two must " +
			"drain on separate workers")
	}
}
