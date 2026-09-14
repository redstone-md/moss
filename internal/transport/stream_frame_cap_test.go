package transport

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/flynn/noise"
)

// One hostile 4-byte header must not become a giant allocation. ReadPacket
// used to size its buffer straight off the wire length: a peer claiming
// 0xFFFFFFFF turned those 4 bytes into a 4 GiB make() — process-fatal for a
// library linked into its host. The cap must reject the lie with an error
// (which the multiplexer's read loop turns into session teardown) and count
// it, never panic and never allocate.
func TestStreamCarrierRejectsOversizeFrameHeader(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	go func() {
		header := make([]byte, 4)
		binary.BigEndian.PutUint32(header, 0xFFFFFFFF)
		_, _ = client.Write(header)
	}()

	carrier := newStreamCarrier(server).(*streamCarrier)
	before := StreamOversizeFrames()
	_, err := carrier.ReadPacket()
	if err == nil {
		t.Fatal("4-byte header claiming a 4 GiB frame was accepted")
	}
	if got := StreamOversizeFrames(); got != before+1 {
		t.Fatalf("oversize counter went from %d to %d; the lie must be counted exactly once", before, got)
	}
}

// The cap must never eat legal traffic: a frame exactly at the ceiling is a
// frame an honest peer can produce, and it must arrive whole.
func TestStreamCarrierAdmitsFrameAtCap(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	frame := make([]byte, 4+maxDataFrameSize)
	binary.BigEndian.PutUint32(frame, uint32(maxDataFrameSize))
	body := frame[4:]
	for i := range body {
		body[i] = byte(i % 251)
	}
	go func() {
		_, _ = client.Write(frame)
	}()

	carrier := newStreamCarrier(server).(*streamCarrier)
	before := StreamOversizeFrames()
	packet, err := carrier.ReadPacket()
	if err != nil {
		t.Fatalf("frame at the cap rejected: %v", err)
	}
	if len(packet) != maxDataFrameSize {
		t.Fatalf("frame at the cap truncated: got %d bytes, want %d", len(packet), maxDataFrameSize)
	}
	for i, b := range packet {
		if b != byte(i%251) {
			t.Fatalf("frame body corrupted at offset %d: got %d, want %d", i, b, byte(i%251))
		}
	}
	if got := StreamOversizeFrames(); got != before {
		t.Fatalf("a legal frame was counted as oversize: counter went from %d to %d", before, got)
	}
}

// The cap must stay above the largest frame an honest peer can produce. A
// maximal publish is room-AEAD sealed (+28), base64'd into its JSON envelope
// (x4/3), relay-AEAD sealed again for relayed peers (+28), base64'd again
// (x4/3), then framed with a 4 B mux header and a 16 B Noise tag. The JSON
// field overhead (message_id, sender_id, topic) gets a flat 512 B allowance
// per envelope layer — generous, but the quantity being guarded is the order
// of magnitude, not the last hundred bytes.
//
// The arithmetic lives here rather than importing the mesh constants:
// mesh imports transport, so the dependency only points one way.
func TestDataFrameCapAdmitsLargestLegitimateEnvelope(t *testing.T) {
	const (
		appCeiling    = 65536  // Security.MaxMessageSizeBytes — a publish of exactly 64 KiB is legal
		aeadOverhead  = 28     // 12 B nonce + 16 B tag, per AEAD seal
		jsonOverhead  = 512    // message_id/sender_id/topic/field names per envelope layer
		transportOver = 4 + 16 // mux header + Noise tag
	)
	base64Len := func(n int) int { return (n + 2) / 3 * 4 }

	roomSealed := appCeiling + aeadOverhead
	direct := base64Len(roomSealed) + jsonOverhead
	if wire := direct + transportOver; wire >= maxDataFrameSize {
		t.Fatalf("largest direct wire frame %d B no longer fits under the %d B cap", wire, maxDataFrameSize)
	}

	relayedSealed := direct + aeadOverhead
	relayed := base64Len(relayedSealed) + jsonOverhead
	if wire := relayed + transportOver; wire >= maxDataFrameSize {
		t.Fatalf("largest relayed wire frame %d B no longer fits under the %d B cap", wire, maxDataFrameSize)
	}
}

// The cap is only half the contract: an oversize header must tear the session
// down, not just be dropped. The multiplexer's read loop converts the carrier
// error into closeAll, so a reader's ReadPacket must report EOF instead of
// waiting forever on a peer that has proven itself hostile.
func TestOversizeFrameTearsDownTheSession(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	suite := noise.NewCipherSuite(noise.DH25519, noise.CipherChaChaPoly, noise.HashBLAKE2s)
	var key [32]byte
	for i := range key {
		key[i] = byte(i + 1)
	}
	sendState := noise.UnsafeNewCipherState(suite, key, 0)
	recvState := noise.UnsafeNewCipherState(suite, key, 0)
	session, err := NewSessionWithBuffers(newStreamCarrier(server), sendState, recvState, [32]byte{}, [32]byte{}, HandshakeModeXX, BufferConfig{})
	if err != nil {
		t.Fatalf("NewSessionWithBuffers failed: %v", err)
	}

	go func() {
		header := make([]byte, 4)
		binary.BigEndian.PutUint32(header, 0xFFFFFFFF)
		_, _ = client.Write(header)
	}()

	readDone := make(chan error, 1)
	go func() {
		_, err := session.ReadPacket()
		readDone <- err
	}()
	select {
	case err := <-readDone:
		if !errors.Is(err, io.EOF) {
			t.Fatalf("expected EOF after an oversize frame, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("oversize frame did not tear the session down within 2s; the reader is stranded on a stream that will never deliver again")
	}
}
