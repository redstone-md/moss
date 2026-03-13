package transport

import (
	"testing"

	"github.com/flynn/noise"
)

func TestDefaultStreamPreservesSessionAPI(t *testing.T) {
	sender, receiver, senderCarrier, receiverCarrier := newStubSessionPair(t)

	if err := sender.WritePacket([]byte("default")); err != nil {
		t.Fatalf("WritePacket failed: %v", err)
	}
	receiverCarrier.incoming <- senderCarrier.writes[0]

	packet, err := receiver.ReadPacket()
	if err != nil {
		t.Fatalf("ReadPacket failed: %v", err)
	}
	if got := string(packet); got != "default" {
		t.Fatalf("unexpected default stream payload %q", got)
	}
}

func TestNamedStreamsStayIsolated(t *testing.T) {
	sender, receiver, senderCarrier, receiverCarrier := newStubSessionPair(t)
	streamA := sender.Stream(7)
	streamB := sender.Stream(9)

	if err := streamA.WritePacket([]byte("alpha")); err != nil {
		t.Fatalf("WritePacket alpha failed: %v", err)
	}
	if err := streamB.WritePacket([]byte("beta")); err != nil {
		t.Fatalf("WritePacket beta failed: %v", err)
	}

	receiverCarrier.incoming <- senderCarrier.writes[1]
	receiverCarrier.incoming <- senderCarrier.writes[0]

	packet, err := receiver.Stream(9).ReadPacket()
	if err != nil {
		t.Fatalf("ReadPacket beta failed: %v", err)
	}
	if got := string(packet); got != "beta" {
		t.Fatalf("unexpected stream 9 payload %q", got)
	}

	packet, err = receiver.Stream(7).ReadPacket()
	if err != nil {
		t.Fatalf("ReadPacket alpha failed: %v", err)
	}
	if got := string(packet); got != "alpha" {
		t.Fatalf("unexpected stream 7 payload %q", got)
	}
}

func newStubSessionPair(t *testing.T) (*Session, *Session, *stubDatagramCarrier, *stubDatagramCarrier) {
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
