package transport

import (
	"encoding/binary"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// fuzzWireConn is a net.Conn whose reads are served from a fixed buffer in
// one shot: it exists so ReadPacket can be driven directly with hostile
// header bytes, including truncated frames, without a peer goroutine.
type fuzzWireConn struct {
	remain []byte
	closed chan struct{}
	once   sync.Once
}

func (c *fuzzWireConn) Read(b []byte) (int, error) {
	if len(c.remain) == 0 {
		return 0, io.EOF
	}
	n := copy(b, c.remain)
	c.remain = c.remain[n:]
	return n, nil
}

func (c *fuzzWireConn) Write(b []byte) (int, error) { return len(b), nil }
func (c *fuzzWireConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}
func (c *fuzzWireConn) LocalAddr() net.Addr              { return nil }
func (c *fuzzWireConn) RemoteAddr() net.Addr             { return nil }
func (c *fuzzWireConn) SetDeadline(time.Time) error      { return nil }
func (c *fuzzWireConn) SetReadDeadline(time.Time) error  { return nil }
func (c *fuzzWireConn) SetWriteDeadline(time.Time) error { return nil }

// FuzzStreamFrameHeader drives ReadPacket with arbitrary 4-byte headers and
// trailing bytes: the exact "4-byte lie" a hostile or corrupted peer sends.
// The pinned contracts:
//
//   - no panic on any header value, including 0xFFFFFFFF (the historical
//     4 GiB allocation) and truncated bodies;
//   - a header over maxDataFrameSize is a clean error, counted exactly once;
//   - a header at or under the cap yields the exact body bytes or a clean
//     read error — never a wrong-length or corrupted buffer.
func FuzzStreamFrameHeader(f *testing.F) {
	f.Add([]byte{0, 0, 0, 0}, []byte{})             // empty frame
	f.Add([]byte{0, 0, 0, 8}, []byte("payloa"))     // exact body
	f.Add([]byte{0, 0, 0, 16}, []byte("short"))     // truncated body
	f.Add([]byte{0, 0, 16, 0}, []byte("body"))      // 4096 with a body
	f.Add([]byte{0x00, 0x04, 0x00, 0x00}, []byte{}) // exactly the cap
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF}, []byte{}) // the 4 GiB lie
	f.Add([]byte{0x00, 0x04, 0x00, 0x01}, []byte{}) // cap + 1
	f.Add([]byte{0xDE, 0xAD, 0xBE, 0xEF}, []byte{}) // arbitrary lie
	f.Add([]byte{0, 0, 0, 4}, []byte("payload!"))   // body longer than the header claims
	f.Add([]byte{0, 0}, []byte{})                   // truncated header
	f.Fuzz(func(t *testing.T, header, body []byte) {
		header = append([]byte(nil), header...)
		body = append([]byte(nil), body...)

		// ReadPacket consumes a 4-byte header; feed the header padded
		// or truncated to exactly four bytes so the body alignment is
		// deterministic, followed by whatever body bytes the fuzzer
		// chose.
		wire := make([]byte, 0, 4+len(body))
		if len(header) >= 4 {
			wire = append(wire, header[:4]...)
		} else {
			wire = append(wire, header...)
			wire = append(wire, make([]byte, 4-len(header))...)
		}
		wire = append(wire, body...)
		if len(wire) > 1<<20 {
			wire = wire[:1<<20] // bound the harness, not the code under test
		}

		conn := &fuzzWireConn{remain: wire, closed: make(chan struct{})}
		carrier := newStreamCarrier(conn)
		declared := binary.BigEndian.Uint32(wire[:4])

		before := StreamOversizeFrames()
		packet, err := carrier.ReadPacket()
		after := StreamOversizeFrames()

		if declared > maxDataFrameSize {
			// The lie must be an error, counted exactly once, and
			// never a panic or an allocation-backed success.
			if err == nil {
				t.Fatalf("header declaring %d bytes was accepted", declared)
			}
			if packet != nil {
				t.Fatalf("oversize header returned a buffer of %d bytes", len(packet))
			}
			if after != before+1 {
				t.Fatalf("oversize counter went %d → %d; the lie must be counted exactly once", before, after)
			}
			return
		}
		// At or under the cap: either the full declared body arrives,
		// or a read error — no counter movement, no wrong lengths.
		if after != before {
			t.Fatalf("legal header (declared %d) moved the oversize counter: %d → %d", declared, before, after)
		}
		if err != nil {
			return // truncated body: a clean read error is fine
		}
		if uint32(len(packet)) != declared {
			t.Fatalf("ReadPacket returned %d bytes, header declared %d", len(packet), declared)
		}
		if len(body) >= int(declared) && string(packet) != string(body[:declared]) {
			t.Fatal("frame body corrupted between the wire and the returned buffer")
		}
	})
}

// FuzzStreamHeaderThroughPipe runs the same hostile headers through a real
// net.Pipe so ReadPacket's io.ReadFull blocking semantics are exercised too:
// a header with no body must terminate on EOF, never hang.
func FuzzStreamHeaderThroughPipe(f *testing.F) {
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF}, []byte("garbage"))
	f.Add([]byte{0, 0, 0, 4}, []byte("body"))
	f.Add([]byte{0x00, 0x10, 0x00, 0x00}, []byte{}) // 1 MiB declared, no body
	f.Add([]byte{0x00, 0x04, 0x00, 0x00}, []byte{}) // exactly the cap
	f.Fuzz(func(t *testing.T, header, body []byte) {
		header = append([]byte(nil), header...)
		body = append([]byte(nil), body...)

		wire := make([]byte, 0, 4+len(body))
		if len(header) >= 4 {
			wire = append(wire, header[:4]...)
		} else {
			wire = append(wire, header...)
			wire = append(wire, make([]byte, 4-len(header))...)
		}
		wire = append(wire, body...)
		if len(wire) > 1<<20 {
			wire = wire[:1<<20]
		}

		client, server := net.Pipe()
		defer client.Close()
		defer server.Close()
		go func() {
			_, _ = client.Write(wire)
			_ = client.Close()
		}()

		carrier := newStreamCarrier(server)
		done := make(chan struct{})
		go func() {
			_, _ = carrier.ReadPacket()
			close(done)
		}()
		select {
		case <-done:
			// returned: error or packet, both fine — the contract is
			// "never hangs, never panics"
		case <-time.After(5 * time.Second):
			t.Fatal("ReadPacket hung on a hostile header with a closed pipe")
		}
	})
}

// FuzzObfsSealOpenRoundTrip drives the datagram obfuscation codec: honest
// round-trips through Seal → Open, and hostile wire inputs straight into
// Open. The pinned contracts:
//
//   - Seal never panics for any kind byte and payload;
//   - an honest Seal output opened by the same codec yields the exact kind
//     and payload back — padding must never corrupt the payload;
//   - Open on arbitrary wire bytes never panics: garbage fails cleanly with
//     ok=false, and a corrupted genuine seal also fails (AEAD integrity);
//   - a one-bit flip anywhere in a sealed datagram must never open cleanly
//     — the tag covers nonce, kind, pad length, and payload.
func FuzzObfsSealOpenRoundTrip(f *testing.F) {
	f.Add(byte(udpMessageData), []byte("hello mesh"), byte(1), false)
	f.Add(byte(udpMessageHandshakeInit), []byte(nil), byte(2), true)
	f.Add(byte(255), []byte{0x45, 0, 0, 20}, byte(3), false)
	f.Add(byte(udpMessageObserveResp), make([]byte, 4096), byte(4), true)
	f.Add(byte(0), []byte{}, byte(5), false)
	f.Fuzz(func(t *testing.T, kind byte, payload []byte, flipAt byte, padded bool) {
		payload = append([]byte(nil), payload...)
		if len(payload) > 1<<16 {
			payload = payload[:1<<16] // bound the harness
		}

		codec, err := newScrambleCodec("mesh-1", []byte("shared-secret"), 256, padded)
		if err != nil {
			t.Fatalf("newScrambleCodec: %v", err)
		}

		// Honest round-trip: the seal must open back to the exact kind
		// and payload, whatever the padding drew.
		wire, err := codec.Seal(kind, payload)
		if err != nil {
			t.Fatalf("Seal(kind=%d, %d bytes): %v", kind, len(payload), err)
		}
		gotKind, gotPayload, ok := codec.Open(wire)
		if !ok {
			t.Fatalf("Open rejected an honest Seal output (kind=%d, %d bytes, padded=%v)", kind, len(payload), padded)
		}
		if gotKind != kind {
			t.Fatalf("kind corrupted: got %d, want %d", gotKind, kind)
		}
		if string(gotPayload) != string(payload) {
			t.Fatalf("payload corrupted: got %d bytes, want %d", len(gotPayload), len(payload))
		}

		// Bit-flip integrity: corrupt one byte of the sealed wire and
		// the codec must refuse it. The tag covers the whole frame, so
		// no single-bit mutation can survive as a valid open.
		if len(wire) > 0 {
			corrupted := append([]byte(nil), wire...)
			corrupted[int(flipAt)%len(corrupted)] ^= 0x01
			if _, _, ok := codec.Open(corrupted); ok {
				t.Fatal("Open accepted a one-bit-corrupted seal; AEAD integrity is broken")
			}
		}

		// Hostile wire: any bytes fed to Open fail cleanly. A genuine
		// AEAD open succeeding on random input is a 2^-128 event, so
		// acceptance here would mean the guard is broken.
		garbage := append([]byte(nil), payload...)
		if len(garbage) >= 12+16+3 {
			if _, _, ok := codec.Open(garbage); ok {
				t.Fatal("Open accepted arbitrary non-codec bytes as a valid datagram")
			}
		}
	})
}
