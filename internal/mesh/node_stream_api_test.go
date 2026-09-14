package mesh

import (
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/transport"
)

// newStreamTestNode builds an unstarted Node with a direct peer wired to a
// recorded session and a relayed peer, for exercising the stream API without
// a live network. config is required for MaxMessageSizeBytes; the literal
// peers skip identity and handshake entirely.
func newStreamTestNode(t *testing.T) (*Node, *recordedSession) {
	t.Helper()
	direct := newRecordedSession(t)
	node := &Node{
		config: DefaultConfig(),
		peers: map[string]*peerConn{
			"direct":  {id: "direct", session: direct.session},
			"relayed": {id: "relayed", relayed: true, viaPeerID: "via", relaySessionID: "s"},
		},
	}
	return node, direct
}

// TestOnStreamDeliversPerStreamIsolated: two handlers on two stream IDs must
// never see each other's payloads — the whole point of multiplexing is that
// game-tick traffic does not interleave with presence on the same stream.
func TestOnStreamDeliversPerStreamIsolated(t *testing.T) {
	node, rec := newStreamTestNode(t)
	farEnd := newCapturingCarrier()
	farSess := cipherMatchedSession(t, farEnd)

	seen7 := make(chan string, 4)
	seen9 := make(chan string, 4)
	if code := node.OnStream(7, func(peerID string, data []byte) {
		seen7 <- peerID + ":" + string(data)
	}); code != MOSS_OK {
		t.Fatalf("OnStream(7): %d", code)
	}
	if code := node.OnStream(9, func(peerID string, data []byte) {
		seen9 <- peerID + ":" + string(data)
	}); code != MOSS_OK {
		t.Fatalf("OnStream(9): %d", code)
	}

	// Feed the inbound side: write through a cipher-matched far session and
	// hand the ciphertext to the node's carrier, exactly like a real peer.
	feed := func(streamID transport.StreamID, payload string) {
		t.Helper()
		if err := farSess.Stream(streamID).WritePacket([]byte(payload)); err != nil {
			t.Fatalf("far-end write: %v", err)
		}
		rec.carrier.reads <- farEnd.lastWrite()
	}
	feed(7, "seven")
	feed(9, "nine")

	expectOne := func(ch chan string, want, stream string) {
		select {
		case got := <-ch:
			if got != want {
				t.Fatalf("stream %s saw %q, want %q", stream, got, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("payload %q was never delivered on stream %s", want, stream)
		}
	}
	expectOne(seen7, "direct:seven", "7")
	expectOne(seen9, "direct:nine", "9")

	// The other stream's handler must stay untouched after both deliveries.
	select {
	case got := <-seen9:
		t.Fatalf("stream 9 handler saw %q; stream 7 traffic leaked across streams", got)
	default:
	}
	select {
	case got := <-seen7:
		t.Fatalf("stream 7 handler saw %q; stream 9 traffic leaked across streams", got)
	default:
	}
}

// TestSendStreamErrorCodeMapping pins the error codes of the fast send path:
// reserved stream IDs rejected before anything else, size limits enforced
// against config, unknown and relayed peers never touched.
func TestSendStreamErrorCodeMapping(t *testing.T) {
	node, rec := newStreamTestNode(t)

	if code := node.SendStream("unknown", 7, []byte("x")); code != MOSS_ERR_NO_PEERS {
		t.Fatalf("unknown peer: %d, want MOSS_ERR_NO_PEERS", code)
	}
	if code := node.SendStream("relayed", 7, []byte("x")); code != MOSS_ERR_RELAY_FAILED {
		t.Fatalf("relayed peer: %d, want MOSS_ERR_RELAY_FAILED", code)
	}
	if code := node.SendStream("direct", 0, []byte("x")); code != MOSS_ERR_CONFIG_INVALID {
		t.Fatalf("stream 0: %d, want MOSS_ERR_CONFIG_INVALID", code)
	}
	if code := node.SendStream("direct", transport.DefaultStream, []byte("x")); code != MOSS_ERR_CONFIG_INVALID {
		t.Fatalf("default stream: %d, want MOSS_ERR_CONFIG_INVALID", code)
	}
	oversize := make([]byte, node.config.Security.MaxMessageSizeBytes+1)
	if code := node.SendStream("direct", 7, oversize); code != MOSS_ERR_MESSAGE_TOO_LARGE {
		t.Fatalf("oversize payload: %d, want MOSS_ERR_MESSAGE_TOO_LARGE", code)
	}

	before := rec.writeCount()
	if code := node.SendStream("direct", 7, []byte("tick")); code != MOSS_OK {
		t.Fatalf("direct send: %d, want MOSS_OK", code)
	}
	if got := rec.writeCount() - before; got != 1 {
		t.Fatalf("direct send produced %d carrier writes, want exactly 1", got)
	}
}

// TestOnStreamRejectsInvalidArguments: reserved streams and nil handlers
// must be refused — the gossip stream is consumed by readPeer and a nil
// handler would panic inside a reader goroutine.
func TestOnStreamRejectsInvalidArguments(t *testing.T) {
	node, _ := newStreamTestNode(t)
	if code := node.OnStream(0, func(string, []byte) {}); code != MOSS_ERR_CONFIG_INVALID {
		t.Fatalf("OnStream(0): %d, want MOSS_ERR_CONFIG_INVALID", code)
	}
	if code := node.OnStream(transport.DefaultStream, func(string, []byte) {}); code != MOSS_ERR_CONFIG_INVALID {
		t.Fatalf("OnStream(DefaultStream): %d, want MOSS_ERR_CONFIG_INVALID", code)
	}
	if code := node.OnStream(7, nil); code != MOSS_ERR_CONFIG_INVALID {
		t.Fatalf("OnStream(7, nil): %d, want MOSS_ERR_CONFIG_INVALID", code)
	}
}

// TestSetStreamUnreliableValidatesStreamID: the reserved gossip stream must
// never become latest-wins — evicting pings would look like a dead peer.
func TestSetStreamUnreliableValidatesStreamID(t *testing.T) {
	node, _ := newStreamTestNode(t)
	if code := node.SetStreamUnreliable(0); code != MOSS_ERR_CONFIG_INVALID {
		t.Fatalf("SetStreamUnreliable(0): %d, want MOSS_ERR_CONFIG_INVALID", code)
	}
	if code := node.SetStreamUnreliable(transport.DefaultStream); code != MOSS_ERR_CONFIG_INVALID {
		t.Fatalf("SetStreamUnreliable(DefaultStream): %d, want MOSS_ERR_CONFIG_INVALID", code)
	}
	if code := node.SetStreamUnreliable(7); code != MOSS_OK {
		t.Fatalf("SetStreamUnreliable(7): %d, want MOSS_OK", code)
	}
}

// TestOpenStreamResolvesDirectPeer: OpenStream on an already-known direct
// peer succeeds and spawns a reader; unknown and relayed peers fail with the
// codes callers switch on.
func TestOpenStreamResolvesDirectPeer(t *testing.T) {
	node, _ := newStreamTestNode(t)
	if code := node.OpenStream("direct", 7); code != MOSS_OK {
		t.Fatalf("OpenStream(direct): %d, want MOSS_OK", code)
	}
	if code := node.OpenStream("unknown", 7); code != MOSS_ERR_NO_PEERS {
		t.Fatalf("OpenStream(unknown): %d, want MOSS_ERR_NO_PEERS", code)
	}
	if code := node.OpenStream("relayed", 7); code != MOSS_ERR_RELAY_FAILED {
		t.Fatalf("OpenStream(relayed): %d, want MOSS_ERR_RELAY_FAILED", code)
	}
	if code := node.OpenStream("direct", 0); code != MOSS_ERR_CONFIG_INVALID {
		t.Fatalf("OpenStream(direct, 0): %d, want MOSS_ERR_CONFIG_INVALID", code)
	}
}

// TestSendStreamSpawnsReader: the reader ensured by SendStream must deliver
// inbound data on the same stream — send and receive sides of one stream ID
// are a single logical pipe, not two unrelated channels.
func TestSendStreamSpawnsReader(t *testing.T) {
	node, rec := newStreamTestNode(t)
	farEnd := newCapturingCarrier()
	farSess := cipherMatchedSession(t, farEnd)

	received := make(chan string, 4)
	if code := node.OnStream(7, func(peerID string, data []byte) {
		received <- peerID + ":" + string(data)
	}); code != MOSS_OK {
		t.Fatalf("OnStream(7): %d", code)
	}
	if code := node.SendStream("direct", 7, []byte("tick")); code != MOSS_OK {
		t.Fatalf("SendStream(direct): %d, want MOSS_OK", code)
	}

	// Inbound traffic on the same stream must reach the handler through the
	// reader SendStream just ensured.
	if err := farSess.Stream(7).WritePacket([]byte("ack")); err != nil {
		t.Fatalf("far-end write: %v", err)
	}
	rec.carrier.reads <- farEnd.lastWrite()

	select {
	case got := <-received:
		if got != "direct:ack" {
			t.Fatalf("received %q, want %q", got, "direct:ack")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("inbound payload never reached the handler; SendStream did not ensure a reader")
	}
}
