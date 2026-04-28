package transport

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"testing"
	"time"
)

func TestStreamCarrierReadPacketRejectsOversizedFrame(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	done := make(chan error, 1)
	go func() {
		header := make([]byte, 4)
		binary.BigEndian.PutUint32(header, maxStreamPacketSize+1)
		_, err := client.Write(header)
		done <- err
	}()

	c := &streamCarrier{conn: server}
	if _, err := c.ReadPacket(); !errors.Is(err, errPacketTooLarge) {
		t.Fatalf("expected errPacketTooLarge, got %v", err)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out writing oversized header")
	}
}

func TestReadFrameRejectsOversizedFrame(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	done := make(chan error, 1)
	go func() {
		header := make([]byte, 4)
		binary.BigEndian.PutUint32(header, maxStreamPacketSize+1)
		_, err := client.Write(header)
		done <- err
	}()

	if _, err := readFrame(context.Background(), server); !errors.Is(err, errPacketTooLarge) {
		t.Fatalf("expected errPacketTooLarge, got %v", err)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out writing oversized header")
	}
}
