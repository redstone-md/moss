package transport

import (
	"bytes"
	"testing"

	"golang.org/x/crypto/chacha20poly1305"
)

func TestScrambleRoundTrip(t *testing.T) {
	c, err := newScrambleCodec("mesh-1", []byte("shared-secret"), 256, true)
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []byte{udpMessageHandshakeInit, udpMessageData, udpMessageObserveResp} {
		payload := []byte("payload-for-kind")
		wire, err := c.Seal(kind, payload)
		if err != nil {
			t.Fatalf("seal: %v", err)
		}
		gotKind, gotPayload, ok := c.Open(wire)
		if !ok {
			t.Fatalf("open failed for kind %d", kind)
		}
		if gotKind != kind || !bytes.Equal(gotPayload, payload) {
			t.Fatalf("round-trip mismatch: kind %d->%d payload %q->%q", kind, gotKind, payload, gotPayload)
		}
	}
}

func TestScrambleEmptyPayload(t *testing.T) {
	c, _ := newScrambleCodec("mesh-1", nil, 0, true) // padMax 0, no PSK
	wire, err := c.Seal(udpMessageHandshakeDone, nil)
	if err != nil {
		t.Fatal(err)
	}
	kind, payload, ok := c.Open(wire)
	if !ok || kind != udpMessageHandshakeDone || len(payload) != 0 {
		t.Fatalf("empty payload round-trip failed: ok=%v kind=%d len=%d", ok, kind, len(payload))
	}
}

func TestScrambleTamperRejected(t *testing.T) {
	c, _ := newScrambleCodec("mesh-1", []byte("s"), 16, true)
	wire, _ := c.Seal(udpMessageData, []byte("hello"))
	wire[len(wire)-1] ^= 0xFF // flip a tag byte
	if _, _, ok := c.Open(wire); ok {
		t.Fatal("tampered datagram accepted")
	}
}

func TestScrambleWrongKeyRejected(t *testing.T) {
	a, _ := newScrambleCodec("mesh-1", []byte("secret-A"), 16, true)
	b, _ := newScrambleCodec("mesh-1", []byte("secret-B"), 16, true)
	wire, _ := a.Seal(udpMessageData, []byte("hello"))
	if _, _, ok := b.Open(wire); ok {
		t.Fatal("datagram opened with wrong key")
	}
}

func TestScrambleRejectsOldPlaintextFormat(t *testing.T) {
	// Flag-day: an old [kind][payload] plaintext datagram must be dropped, not parsed.
	c, _ := newScrambleCodec("mesh-1", []byte("s"), 16, true)
	old := append([]byte{udpMessageHandshakeInit, HandshakeModeXX}, bytes.Repeat([]byte{0xAB}, 48)...)
	if _, _, ok := c.Open(old); ok {
		t.Fatal("old plaintext format accepted by new codec")
	}
}

func TestScrambleRejectsShort(t *testing.T) {
	c, _ := newScrambleCodec("mesh-1", []byte("s"), 16, true)
	if _, _, ok := c.Open([]byte{1, 2, 3}); ok {
		t.Fatal("short datagram accepted")
	}
}

func TestScrambleClampsPadMax(t *testing.T) {
	c, err := newScrambleCodec("mesh-1", []byte("s"), 70000, true) // > uint16 max
	if err != nil {
		t.Fatal(err)
	}
	if c.padMax > 65535 {
		t.Fatalf("padMax not clamped: %d", c.padMax)
	}
	// round-trip must stay intact under a huge configured bound
	payload := []byte("payload-intact")
	wire, err := c.Seal(udpMessageHandshakeInit, payload)
	if err != nil {
		t.Fatal(err)
	}
	kind, got, ok := c.Open(wire)
	if !ok || kind != udpMessageHandshakeInit || !bytes.Equal(got, payload) {
		t.Fatalf("large padMax corrupted round-trip: ok=%v kind=%d payload=%q", ok, kind, got)
	}
}

func TestScrambleNoFixedFingerprint(t *testing.T) {
	c, err := newScrambleCodec("mesh-1", []byte("secret"), 256, true)
	if err != nil {
		t.Fatal(err)
	}
	const N = 256
	// Fixed payload + kind: proves the codec alone varies size and bytes,
	// exactly the handshake-init case that fixed-size DPI keys on.
	payload := bytes.Repeat([]byte{0x00}, 48)
	packets := make([][]byte, N)
	for i := range packets {
		w, err := c.Seal(udpMessageHandshakeInit, payload)
		if err != nil {
			t.Fatal(err)
		}
		packets[i] = w
	}

	// (a) packet sizes vary
	sizes := map[int]bool{}
	minLen := len(packets[0])
	for _, p := range packets {
		sizes[len(p)] = true
		if len(p) < minLen {
			minLen = len(p)
		}
	}
	if len(sizes) < 8 {
		t.Fatalf("packet sizes barely vary: %d distinct lengths", len(sizes))
	}

	// (b) no byte position is constant across all packets (over common prefix)
	for pos := 0; pos < minLen; pos++ {
		v := packets[0][pos]
		constant := true
		for _, p := range packets {
			if p[pos] != v {
				constant = false
				break
			}
		}
		if constant {
			t.Fatalf("byte position %d is constant across all packets (fingerprint)", pos)
		}
	}

	// (c) nonces unique
	seen := map[string]bool{}
	for _, p := range packets {
		seen[string(p[:chacha20poly1305.NonceSize])] = true
	}
	if len(seen) != N {
		t.Fatalf("nonces not unique: %d/%d", len(seen), N)
	}
}

// The unpadded fast path (data kind, padData=false) must draw its nonce in a
// single 12B rand.Read and produce a fixed-size wire — this is the hot
// per-datagram path in high-throughput mode.
func TestScrambleFastPathFixedLayout(t *testing.T) {
	c, err := newScrambleCodec("mesh-1", []byte("secret"), 256, false)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("fixed-hot-path")
	want := chacha20poly1305.NonceSize + 3 + len(payload) + c.aead.Overhead()
	for range 64 {
		wire, err := c.Seal(udpMessageData, payload)
		if err != nil {
			t.Fatal(err)
		}
		if len(wire) != want {
			t.Fatalf("unpadded fast path varied size: got %d, want %d", len(wire), want)
		}
		kind, got, ok := c.Open(wire)
		if !ok || kind != udpMessageData || !bytes.Equal(got, payload) {
			t.Fatalf("fast-path round-trip failed: ok=%v kind=%d payload=%q", ok, kind, got)
		}
	}
}

// The padded path must draw padLen and the nonce together (one rand.Read) while
// keeping padLen uniform over [0, bound] and the wire round-trip intact.
func TestScramblePadDrawCoversBound(t *testing.T) {
	const bound = 32
	c, err := newScrambleCodec("mesh-1", []byte("secret"), bound, true)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("padded")
	minLen, maxLen := 1<<62, 0
	for range 512 {
		wire, err := c.Seal(udpMessageHandshakeInit, payload)
		if err != nil {
			t.Fatal(err)
		}
		if len(wire) < minLen {
			minLen = len(wire)
		}
		if len(wire) > maxLen {
			maxLen = len(wire)
		}
		kind, got, ok := c.Open(wire)
		if !ok || kind != udpMessageHandshakeInit || !bytes.Equal(got, payload) {
			t.Fatalf("padded round-trip failed at padLen=%d: ok=%v kind=%d payload=%q",
				len(wire)-chacha20poly1305.NonceSize-3-len(payload)-c.aead.Overhead(), ok, kind, got)
		}
	}
	if minLen != chacha20poly1305.NonceSize+3+len(payload)+c.aead.Overhead() {
		t.Fatalf("padded path emitted below the zero-pad floor: min=%d", minLen)
	}
	if maxLen != chacha20poly1305.NonceSize+3+len(payload)+c.aead.Overhead()+bound {
		t.Fatalf("padded path never hit the bound: max=%d want +%d", maxLen, bound)
	}
}
