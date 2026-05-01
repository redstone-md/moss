package transport

import (
	"net"
	"testing"

	"github.com/flynn/noise"
)

type stubDatagramCarrier struct {
	incoming chan []byte
	writes   [][]byte
	closed   bool
}

func newStubDatagramCarrier() *stubDatagramCarrier {
	return &stubDatagramCarrier{incoming: make(chan []byte, 16)}
}

func (c *stubDatagramCarrier) WritePacket(packet []byte) error {
	c.writes = append(c.writes, append([]byte(nil), packet...))
	return nil
}

func (c *stubDatagramCarrier) ReadPacket() ([]byte, error) {
	packet, ok := <-c.incoming
	if !ok {
		return nil, net.ErrClosed
	}
	return append([]byte(nil), packet...), nil
}

func (c *stubDatagramCarrier) RemoteAddr() net.Addr {
	return &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9999}
}

func (c *stubDatagramCarrier) Close() error {
	if !c.closed {
		close(c.incoming)
		c.closed = true
	}
	return nil
}

func (c *stubDatagramCarrier) supportsDatagramSession() bool {
	return true
}

func TestDatagramSessionAcceptsOutOfOrderPackets(t *testing.T) {
	sender, receiver, senderCarrier, receiverCarrier := newStubDatagramSessionPair(t)

	if err := sender.WritePacket([]byte("first")); err != nil {
		t.Fatalf("WritePacket first failed: %v", err)
	}
	if err := sender.WritePacket([]byte("second")); err != nil {
		t.Fatalf("WritePacket second failed: %v", err)
	}
	receiverCarrier.incoming <- senderCarrier.writes[1]
	receiverCarrier.incoming <- senderCarrier.writes[0]

	packet, err := receiver.ReadPacket()
	if err != nil {
		t.Fatalf("ReadPacket second failed: %v", err)
	}
	if got := string(packet); got != "second" {
		t.Fatalf("expected reordered packet to decrypt, got %q", got)
	}
	packet, err = receiver.ReadPacket()
	if err != nil {
		t.Fatalf("ReadPacket first failed: %v", err)
	}
	if got := string(packet); got != "first" {
		t.Fatalf("expected earlier packet to decrypt after reorder, got %q", got)
	}
}

func TestDatagramSessionSkipsDuplicatePackets(t *testing.T) {
	sender, receiver, senderCarrier, receiverCarrier := newStubDatagramSessionPair(t)

	if err := sender.WritePacket([]byte("once")); err != nil {
		t.Fatalf("WritePacket first failed: %v", err)
	}
	if err := sender.WritePacket([]byte("next")); err != nil {
		t.Fatalf("WritePacket second failed: %v", err)
	}
	receiverCarrier.incoming <- senderCarrier.writes[0]
	receiverCarrier.incoming <- senderCarrier.writes[0]
	receiverCarrier.incoming <- senderCarrier.writes[1]

	packet, err := receiver.ReadPacket()
	if err != nil {
		t.Fatalf("ReadPacket once failed: %v", err)
	}
	if got := string(packet); got != "once" {
		t.Fatalf("expected first payload, got %q", got)
	}
	packet, err = receiver.ReadPacket()
	if err != nil {
		t.Fatalf("ReadPacket next failed: %v", err)
	}
	if got := string(packet); got != "next" {
		t.Fatalf("expected duplicate to be skipped, got %q", got)
	}
}

func newStubDatagramSessionPair(t *testing.T) (*Session, *Session, *stubDatagramCarrier, *stubDatagramCarrier) {
	t.Helper()

	suite := noise.NewCipherSuite(noise.DH25519, noise.CipherChaChaPoly, noise.HashBLAKE2s)
	var key [32]byte
	for i := range key {
		key[i] = byte(i + 1)
	}

	sendState := noise.UnsafeNewCipherState(suite, key, 0)
	recvState := noise.UnsafeNewCipherState(suite, key, 0)
	senderCarrier := newStubDatagramCarrier()
	receiverCarrier := newStubDatagramCarrier()

	sender, err := NewSession(senderCarrier, sendState, recvState, [32]byte{}, [32]byte{}, HandshakeModeXX)
	if err != nil {
		t.Fatalf("NewSession sender failed: %v", err)
	}

	sendState = noise.UnsafeNewCipherState(suite, key, 0)
	recvState = noise.UnsafeNewCipherState(suite, key, 0)
	receiver, err := NewSession(receiverCarrier, sendState, recvState, [32]byte{}, [32]byte{}, HandshakeModeXX)
	if err != nil {
		t.Fatalf("NewSession receiver failed: %v", err)
	}

	return sender, receiver, senderCarrier, receiverCarrier
}
