package transport

import "testing"

// A packet dropped for want of buffer space must be counted.
//
// It is invisible everywhere else: the sender's WritePacket returns nil, the
// carrier delivers it, TCP stays up, and the packet simply ceases to exist. When
// those are pings the peer counts six misses and drops a session whose
// connection was healthy throughout — the fleet's 37-38s deaths, on both ends of
// links where each side received a handful of packets and neither heard the
// other.
//
// An overflow hook was already here to notice this. Nothing ever installed it,
// so in production these drops have never once been counted.
func TestDroppedPacketsAreCounted(t *testing.T) {
	_, session, _, _ := newStubSessionPairWithBuffers(t, BufferConfig{StreamBufferSize: 2})
	stream := session.mux.Default()

	before := StreamDrops()
	// Two fit; nobody is reading, so the rest have nowhere to go.
	for i := 0; i < 6; i++ {
		stream.enqueue([]byte("packet"))
	}
	dropped := StreamDrops() - before

	if dropped != 4 {
		t.Fatalf("a full buffer discarded %d packets and reported %d: drops stay invisible", 4, dropped)
	}
}

// Packets that fit must not be counted as drops — a metric that cries wolf on a
// healthy session is worse than none.
func TestPacketsThatFitAreNotCountedAsDrops(t *testing.T) {
	_, session, _, _ := newStubSessionPairWithBuffers(t, BufferConfig{StreamBufferSize: 4})
	stream := session.mux.Default()

	before := StreamDrops()
	for i := 0; i < 4; i++ {
		stream.enqueue([]byte("packet"))
	}
	if dropped := StreamDrops() - before; dropped != 0 {
		t.Fatalf("%d packets reported dropped while all four fit in the buffer", dropped)
	}
}
