package transport

import "testing"

// Latest-wins inverts the overflow policy: the newest payload always makes it
// into the buffer, the oldest is evicted. A game tick arriving on a full
// buffer must displace the stale tick nobody consumed, not vanish behind it.
func TestLatestWinsDropsOldestWhenFull(t *testing.T) {
	_, session, _, _ := newStubSessionPairWithBuffers(t, BufferConfig{StreamBufferSize: 2})
	stream := session.mux.Stream(7)
	stream.SetLatestWins()

	// Two fit, two force evictions of the oldest buffered payload.
	for _, payload := range [][]byte{[]byte("p1"), []byte("p2"), []byte("p3"), []byte("p4")} {
		stream.enqueue(payload)
	}

	if drops := stream.Drops(); drops != 2 {
		t.Fatalf("expected 2 evictions on a 2-slot latest-wins buffer, got %d", drops)
	}

	// The two evictions must have removed p1 and p2, not the fresh data.
	first, err := stream.ReadPacket()
	if err != nil || string(first) != "p3" {
		t.Fatalf("latest-wins kept the stale packet: first read is %q, %v", first, err)
	}
	second, err := stream.ReadPacket()
	if err != nil || string(second) != "p4" {
		t.Fatalf("latest-wins lost the newest packet: second read is %q, %v", second, err)
	}
	if queued := len(stream.buffer); queued != 0 {
		t.Fatalf("expected an empty buffer after draining both packets, %d left", queued)
	}
}

// Without the flag the default drop-newest policy must be untouched — p3 and
// p4 bounce off the full buffer while the older pair survives — and the
// global counters still see the losses.
func TestLatestWinsFlagOffDropsNewest(t *testing.T) {
	_, session, _, _ := newStubSessionPairWithBuffers(t, BufferConfig{StreamBufferSize: 2})
	stream := session.mux.Stream(8)

	before := StreamDrops()
	for _, payload := range [][]byte{[]byte("p1"), []byte("p2"), []byte("p3"), []byte("p4")} {
		stream.enqueue(payload)
	}

	if drops := stream.Drops(); drops != 2 {
		t.Fatalf("expected 2 dropped packets on a full default buffer, got %d", drops)
	}
	if dropped := StreamDrops() - before; dropped != 2 {
		t.Fatalf("global counter reported %d drops, expected 2", dropped)
	}

	first, err := stream.ReadPacket()
	if err != nil || string(first) != "p1" {
		t.Fatalf("default policy should keep the oldest packets, first read is %q, %v", first, err)
	}
	second, err := stream.ReadPacket()
	if err != nil || string(second) != "p2" {
		t.Fatalf("default policy should keep the oldest packets, second read is %q, %v", second, err)
	}
}
