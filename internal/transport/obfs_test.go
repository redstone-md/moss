package transport

import (
	"bytes"
	"testing"
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
