package transport

import (
	"bytes"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/flynn/noise"
)

// Hot-path benchmarks for the session transport: the full packet round trip
// (mux framing + Noise AEAD + carrier), the stream carrier's framed write
// (the syscall-facing half every envelope pays), and the datagram obfuscation
// codec's Seal/Open pair in both padding modes.
//
// All fixtures are bench-local: nothing here shares helper state with the
// package's other test files, and every goroutine a fixture spawns (the
// session multiplexer's read loop) is terminated through the session's Close
// before the benchmark returns.

// benchMemCarrier is a blocking in-memory carrier: writes queue packets for
// the reader, and a ReadPacket on an empty queue parks until a packet or
// Close arrives. Parking matters: the session read loop treats any error —
// including a would-be "empty" EOF — as a teardown, so a one-shot empty read
// would kill the session instead of waiting for traffic.
type benchMemCarrier struct {
	mu     sync.Mutex
	queue  [][]byte
	ready  chan struct{}
	closed bool
}

func newBenchMemCarrier() *benchMemCarrier {
	return &benchMemCarrier{ready: make(chan struct{}, 1)}
}

func (c *benchMemCarrier) WritePacket(p []byte) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return net.ErrClosed
	}
	c.queue = append(c.queue, append([]byte(nil), p...))
	c.mu.Unlock()
	select {
	case c.ready <- struct{}{}:
	default:
	}
	return nil
}

func (c *benchMemCarrier) ReadPacket() ([]byte, error) {
	for {
		c.mu.Lock()
		if len(c.queue) > 0 {
			p := c.queue[0]
			c.queue = c.queue[1:]
			c.mu.Unlock()
			return p, nil
		}
		if c.closed {
			c.mu.Unlock()
			return nil, net.ErrClosed
		}
		c.mu.Unlock()
		<-c.ready
	}
}

func (c *benchMemCarrier) RemoteAddr() net.Addr { return benchAddr{} }

func (c *benchMemCarrier) Close() error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	select {
	case c.ready <- struct{}{}:
	default:
	}
	return nil
}

// benchAddr is the placeholder net.Addr every bench carrier reports.
type benchAddr struct{}

func (benchAddr) Network() string { return "bench" }
func (benchAddr) String() string  { return "bench" }

// benchCipherStates builds two cipher states sharing one key: with one write
// per read on each side, the nonce counters stay in lockstep.
func benchCipherStates() (*noise.CipherState, *noise.CipherState) {
	suite := noise.NewCipherSuite(noise.DH25519, noise.CipherChaChaPoly, noise.HashBLAKE2s)
	var key [32]byte
	for i := range key {
		key[i] = byte(i + 1)
	}
	return noise.UnsafeNewCipherState(suite, key, 0), noise.UnsafeNewCipherState(suite, key, 0)
}

// benchSessionPair wires two sessions A→B over a two-way memory wire. Each
// session's carrier must be DUPLEX: writeRawPacket writes through the same
// carrier the session's read loop reads from, so a session handed a bare
// one-way queue would consume its own ciphertext. benchDuplexCarrier splits
// the two directions: A's writes drain into wireAB, which B's read loop
// reads; A's read loop parks on the forever-empty wireBA.
//
// Nonce discipline: A's send cipher and B's recv cipher share keyA at
// counter 0; B's send and A's recv share keyB. A only writes and B only
// reads in this fixture, so the A→B counter pair stays in lockstep with one
// write per read.
func benchSessionPair(b *testing.B) (a, bSess *Session, cleanup func()) {
	b.Helper()
	wireAB := newBenchMemCarrier() // A writes here; B reads from here
	wireBA := newBenchMemCarrier() // B would write here; A's read loop parks on it
	suite := noise.NewCipherSuite(noise.DH25519, noise.CipherChaChaPoly, noise.HashBLAKE2s)
	var keyA, keyB [32]byte
	for i := range keyA {
		keyA[i] = byte(i + 1)
		keyB[i] = byte(i + 7)
	}
	sessA, err := NewSession(&benchDuplexCarrier{in: wireBA, out: wireAB}, noise.UnsafeNewCipherState(suite, keyA, 0), noise.UnsafeNewCipherState(suite, keyB, 0), [32]byte{}, [32]byte{}, HandshakeModeXX)
	if err != nil {
		b.Fatalf("NewSession A failed: %v", err)
	}
	sessB, err := NewSession(&benchDuplexCarrier{in: wireAB, out: wireBA}, noise.UnsafeNewCipherState(suite, keyB, 0), noise.UnsafeNewCipherState(suite, keyA, 0), [32]byte{}, [32]byte{}, HandshakeModeXX)
	if err != nil {
		b.Fatalf("NewSession B failed: %v", err)
	}
	return sessA, sessB, func() {
		_ = sessA.Close()
		_ = sessB.Close()
	}
}

// benchDuplexCarrier gives one session two one-way carriers: inbound reads
// drain `in`, outbound writes land in `out`. Session A's `out` is Session
// B's `in` and vice versa, so each side's read loop sees only the peer's
// ciphertext.
type benchDuplexCarrier struct {
	in  *benchMemCarrier
	out *benchMemCarrier
}

func (c *benchDuplexCarrier) WritePacket(p []byte) error { return c.out.WritePacket(p) }
func (c *benchDuplexCarrier) ReadPacket() ([]byte, error) {
	return c.in.ReadPacket()
}
func (c *benchDuplexCarrier) RemoteAddr() net.Addr { return benchAddr{} }
func (c *benchDuplexCarrier) Close() error {
	err := c.in.Close()
	_ = c.out.Close()
	return err
}

// BenchmarkSessionPacketRoundTrip drives one packet from A's default stream
// to B's default stream: WritePacket (mux framing → Noise Encrypt → carrier)
// on one side, ReadPacket on the other. This is the per-envelope floor every
// mesh message pays in both directions.
//
// Baseline (Ryzen 5 3600X, linux, go1.25.9, 2026-09-14, 512B payload):
// ~2274 ns/op (225 MB/s), 2360 B/op, 7 allocs/op.
func BenchmarkSessionPacketRoundTrip(b *testing.B) {
	const size = 512
	packet := make([]byte, size)
	for i := range packet {
		packet[i] = byte(i)
	}
	a, bSess, cleanup := benchSessionPair(b)
	defer cleanup()

	b.ReportAllocs()
	b.SetBytes(int64(size))
	b.ResetTimer()
	for b.Loop() {
		if err := a.WritePacket(packet); err != nil {
			b.Fatalf("write failed: %v", err)
		}
		got, err := bSess.ReadPacket()
		if err != nil {
			b.Fatalf("read failed: %v", err)
		}
		if len(got) != size {
			b.Fatalf("round trip delivered %d bytes, want %d", len(got), size)
		}
	}
}

// benchNetPipe returns an in-memory net.Conn pair (like net.Pipe but with
// no-ops for deadlines, which stream carriers set on every write).
type benchNetPipeConn struct {
	mu     sync.Mutex
	buf    bytes.Buffer
	closed bool
}

func (c *benchNetPipeConn) Read(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for c.buf.Len() == 0 && !c.closed {
		c.mu.Unlock()
		time.Sleep(50 * time.Microsecond)
		c.mu.Lock()
	}
	if c.buf.Len() == 0 {
		return 0, io.EOF
	}
	return c.buf.Read(p)
}

func (c *benchNetPipeConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return 0, net.ErrClosed
	}
	return c.buf.Write(p)
}

func (c *benchNetPipeConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	return nil
}

func (c *benchNetPipeConn) LocalAddr() net.Addr              { return benchAddr{} }
func (c *benchNetPipeConn) RemoteAddr() net.Addr             { return benchAddr{} }
func (c *benchNetPipeConn) SetDeadline(time.Time) error      { return nil }
func (c *benchNetPipeConn) SetReadDeadline(time.Time) error  { return nil }
func (c *benchNetPipeConn) SetWriteDeadline(time.Time) error { return nil }

// BenchmarkStreamCarrierWrite measures the framed stream write path in
// isolation: 4-byte length header + payload in one net.Buffers write, the
// shape every stream session's WritePacket produces. The conn is a memory
// buffer, so this is the transport's own overhead (header, deadline,
// buffering) without syscall noise.
//
// Baseline (Ryzen 5 3600X, linux, go1.25.9, 2026-09-14, 512B payload):
// ~520 ns/op (984 MB/s), 1109 B/op, 3 allocs/op.
func BenchmarkStreamCarrierWrite(b *testing.B) {
	const size = 512
	packet := make([]byte, size)
	for i := range packet {
		packet[i] = byte(i)
	}
	conn := &benchNetPipeConn{}
	carrier := newStreamCarrier(conn)
	b.Cleanup(func() { _ = conn.Close() })

	b.ReportAllocs()
	b.SetBytes(int64(size))
	b.ResetTimer()
	for b.Loop() {
		if err := carrier.WritePacket(packet); err != nil {
			b.Fatalf("stream write failed: %v", err)
		}
	}
}

// BenchmarkObfsSealOpen exercises the UDP datagram obfuscation codec: one
// Seal (nonce draw + AEAD encrypt + pad) paired with one Open (AEAD decrypt
// + payload copy). Two sub-runs cover the two production shapes:
//
//   - unpadded: high-throughput mode (padData=false on data datagrams) —
//     the fast path every relayed datagram takes;
//   - padded: handshake-mode padding (padMax=256), where each Seal draws a
//     random pad length and the wire size varies.
//
// Baseline (Ryzen 5 3600X, linux, go1.25.9, 2026-09-14, 512B payload):
//
//	unpadded   ~1104 ns/op  2256 B/op  5 allocs/op
//	padded256  ~1427 ns/op  2589 B/op  5 allocs/op
func BenchmarkObfsSealOpen(b *testing.B) {
	for _, tc := range []struct {
		name    string
		padMax  int
		padData bool
	}{
		{"unpadded", 0, false},
		{"padded256", 256, true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			codec, err := newScrambleCodec("bench-mesh", []byte("bench-psk"), tc.padMax, tc.padData)
			if err != nil {
				b.Fatalf("newScrambleCodec failed: %v", err)
			}
			payload := make([]byte, 512)
			for i := range payload {
				payload[i] = byte(i)
			}

			// Fixture sanity before timing: a sealed datagram must open
			// back to the same kind and payload.
			wire, err := codec.Seal(udpMessageData, payload)
			if err != nil {
				b.Fatalf("Seal failed: %v", err)
			}
			kind, got, ok := codec.Open(wire)
			if !ok || kind != udpMessageData || !bytes.Equal(got, payload) {
				b.Fatalf("Open round trip failed: ok=%v kind=%d", ok, kind)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				wire, err := codec.Seal(udpMessageData, payload)
				if err != nil {
					b.Fatalf("Seal failed: %v", err)
				}
				if kind, got, ok := codec.Open(wire); !ok || kind != udpMessageData || len(got) != len(payload) {
					b.Fatalf("Open failed: ok=%v kind=%d len=%d", ok, kind, len(got))
				}
			}
		})
	}
}
