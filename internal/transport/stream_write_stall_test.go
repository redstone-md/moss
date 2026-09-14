package transport

import (
	"errors"
	"net"
	"os"
	"testing"
	"time"
)

// A stalled stream peer must fail fast, twice over.
//
// Every WritePacket on a session serializes on the session's write mutex,
// and the mesh writes to many peers from single goroutines (broadcast,
// maintenance, relay forwarding). A peer whose TCP window is full held each
// of those goroutines for the full 5s write deadline — per envelope — while
// the rest of the fleet waited behind it. Bounding the deadline is not
// enough on its own: a timed-out write may already have pushed a partial
// frame into the kernel buffer, so a retry writes a new header mid-body and
// corrupts the stream for the reader, and it blocks the caller for another
// full timeout. The first write error on a stream session is terminal; the
// carrier must say so immediately instead of spending the deadline again.
func TestStreamCarrierStopsRetryingAStalledPeer(t *testing.T) {
	old := streamWriteTimeout
	streamWriteTimeout = 50 * time.Millisecond
	t.Cleanup(func() { streamWriteTimeout = old })

	// A pipe with no reader: writes block until the deadline expires.
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	carrier := newStreamCarrier(client).(*streamCarrier)
	packet := make([]byte, 8192)

	started := time.Now()
	firstErr := carrier.WritePacket(packet)
	if firstErr == nil {
		t.Fatal("write to a peer that never reads unexpectedly succeeded")
	}
	if !errors.Is(firstErr, os.ErrDeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", firstErr)
	}
	if elapsed := time.Since(started); elapsed < 50*time.Millisecond {
		t.Fatalf("deadline was not honoured: write failed after %s", elapsed)
	}

	// The retry must fail immediately, not spend another full deadline
	// blocking every peer queued behind this one.
	started = time.Now()
	secondErr := carrier.WritePacket(packet)
	if secondErr == nil {
		t.Fatal("write after a deadline failure unexpectedly succeeded; the stream may now carry a torn frame")
	}
	if !errors.Is(secondErr, firstErr) {
		t.Fatalf("expected the sticky first error %v, got %v", firstErr, secondErr)
	}
	if elapsed := time.Since(started); elapsed > 10*time.Millisecond {
		t.Fatalf("retry after failure blocked for %s; a lost peer must not stall its writers again", elapsed)
	}
}

// A healthy stream round-trip must be untouched by the fail-fast path: two
// packets, both delivered, both readable. Uses loopback TCP rather than
// net.Pipe — a pipe has no kernel buffer, so a write blocks until the peer
// reads, which would pin every "healthy" write against the deadline.
func TestStreamCarrierHealthyRoundTripUnchanged(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	defer listener.Close()

	conn, err := net.Dial("tcp4", listener.Addr().String())
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()
	serverConn, err := listener.Accept()
	if err != nil {
		t.Fatalf("accept failed: %v", err)
	}
	defer serverConn.Close()

	carrier := newStreamCarrier(conn).(*streamCarrier)
	if err := carrier.WritePacket([]byte("first")); err != nil {
		t.Fatalf("first write failed: %v", err)
	}
	if err := carrier.WritePacket([]byte("second")); err != nil {
		t.Fatalf("second write failed: %v", err)
	}

	reader := newStreamCarrier(serverConn)
	got, err := reader.ReadPacket()
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if string(got) != "first" {
		t.Fatalf("expected first packet, got %q", got)
	}
	got, err = reader.ReadPacket()
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if string(got) != "second" {
		t.Fatalf("expected second packet, got %q", got)
	}
}
