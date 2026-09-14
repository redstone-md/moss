package transport

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

// An oversize WritePacket must be rejected BEFORE the wire, with an error the
// caller can distinguish from a dead session — and the session must stay
// usable for every legal frame after it, so writeErr (terminal by contract)
// is deliberately left unset.
func TestWritePacketRejectsOversizeWithoutKillingSession(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	carrier := newStreamCarrier(client).(*streamCarrier)
	before := StreamOversizeFramesOut()

	oversize := make([]byte, maxDataFrameSize+1)
	if err := carrier.WritePacket(oversize); err == nil {
		t.Fatal("oversize WritePacket unexpectedly succeeded")
	} else if !strings.Contains(err.Error(), "exceeds the") {
		t.Fatalf("expected oversize rejection error, got %v", err)
	}
	if got := StreamOversizeFramesOut(); got != before+1 {
		t.Fatalf("outbound oversize counter went from %d to %d; must be counted exactly once", before, got)
	}
	if carrier.writeErr != nil {
		t.Fatalf("oversize send poisoned the session: writeErr=%v", carrier.writeErr)
	}

	// Session survives: a legal frame after the rejection must be delivered,
	// proving the gate is per-send and not terminal. net.Pipe is synchronous
	// — the write completes only against a concurrent read — so the reader
	// drains the frame while WritePacket is in flight.
	type readResult struct {
		payload []byte
		err     error
	}
	readCh := make(chan readResult, 1)
	go func() {
		header := make([]byte, 4)
		if _, err := io.ReadFull(server, header); err != nil {
			readCh <- readResult{err: err}
			return
		}
		buf := make([]byte, binary.BigEndian.Uint32(header))
		if _, err := io.ReadFull(server, buf); err != nil {
			readCh <- readResult{err: err}
			return
		}
		readCh <- readResult{payload: buf}
	}()

	legal := make([]byte, 32)
	copy(legal, "still-alive")
	if err := carrier.WritePacket(legal); err != nil {
		t.Fatalf("legal WritePacket after oversize rejection failed: %v", err)
	}
	select {
	case res := <-readCh:
		if res.err != nil {
			t.Fatalf("reading frame from live session failed: %v", res.err)
		}
		if !bytes.Equal(res.payload, legal) {
			t.Fatalf("unexpected payload after oversize rejection: %q", string(res.payload))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for legal frame on the live session")
	}
}

// A frame exactly at the cap is honest traffic and must pass, mirroring the
// inbound cap's own at-cap admission test: the gate must reject only frames
// the receiving end would die on, never ones it can carry.
func TestWritePacketAdmitsFrameAtCap(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()

	before := StreamOversizeFramesOut()

	carrier := newStreamCarrier(client).(*streamCarrier)
	done := make(chan error, 1)
	go func() {
		header := make([]byte, 4)
		if _, err := io.ReadFull(server, header); err != nil {
			done <- err
			return
		}
		if got := binary.BigEndian.Uint32(header); got != maxDataFrameSize {
			done <- fmt.Errorf("at-cap frame header claims %d bytes", got)
			return
		}
		buf := make([]byte, maxDataFrameSize)
		if _, err := io.ReadFull(server, buf); err != nil {
			done <- err
			return
		}
		done <- nil
	}()

	atCap := make([]byte, maxDataFrameSize)
	if err := carrier.WritePacket(atCap); err != nil {
		t.Fatalf("at-cap WritePacket failed: %v", err)
	}
	if got := StreamOversizeFramesOut(); got != before {
		t.Fatalf("outbound oversize counter moved on a legal at-cap frame: %d -> %d", before, got)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("at-cap frame did not arrive whole: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for at-cap frame")
	}
}
