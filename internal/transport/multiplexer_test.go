package transport

import (
	"io"
	"net"
	"testing"
	"time"

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

func TestStreamReadReturnsEOFWhenSessionCloses(t *testing.T) {
	_, receiver, _, receiverCarrier := newStubSessionPair(t)
	stream := receiver.Stream(11)

	done := make(chan error, 1)
	go func() {
		_, err := stream.ReadPacket()
		done <- err
	}()

	time.Sleep(20 * time.Millisecond)
	if err := receiverCarrier.Close(); err != nil {
		t.Fatalf("receiver carrier close failed: %v", err)
	}

	select {
	case err := <-done:
		if err == nil || err != io.EOF {
			t.Fatalf("expected io.EOF after session close, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("stream read did not unblock after carrier close")
	}
}

func TestStreamWriteReturnsClosedAfterSessionClose(t *testing.T) {
	sender, _, _, _ := newStubSessionPair(t)
	stream := sender.Stream(5)
	if err := sender.Close(); err != nil {
		t.Fatalf("sender close failed: %v", err)
	}
	if err := stream.WritePacket([]byte("late")); err == nil {
		t.Fatal("expected write after session close to fail")
	} else if err != net.ErrClosed {
		t.Fatalf("expected net.ErrClosed after session close, got %v", err)
	}
}

func TestMalformedFrameIsIgnoredBeforeNextValidPacket(t *testing.T) {
	sender, receiver, senderCarrier, receiverCarrier := newStubSessionPair(t)
	stream := sender.Stream(7)

	if err := stream.WritePacket([]byte("valid")); err != nil {
		t.Fatalf("WritePacket valid failed: %v", err)
	}
	receiverCarrier.incoming <- []byte{0x01, 0x02, 0x03}
	receiverCarrier.incoming <- senderCarrier.writes[0]

	packet, err := receiver.Stream(7).ReadPacket()
	if err != nil {
		t.Fatalf("ReadPacket after malformed frame failed: %v", err)
	}
	if got := string(packet); got != "valid" {
		t.Fatalf("unexpected stream payload %q", got)
	}
}

func TestInboundStreamCreationIsCapped(t *testing.T) {
	sender, receiver, senderCarrier, receiverCarrier := newStubSessionPair(t)

	for id := StreamID(2); id <= StreamID(maxInboundStreams+100); id++ {
		if err := sender.Stream(id).WritePacket([]byte("x")); err != nil {
			t.Fatalf("WritePacket stream %d failed: %v", id, err)
		}
		receiverCarrier.incoming <- senderCarrier.writes[len(senderCarrier.writes)-1]
	}

	time.Sleep(50 * time.Millisecond)

	receiver.mux.mu.RLock()
	streamCount := len(receiver.mux.streams)
	receiver.mux.mu.RUnlock()

	if streamCount != maxInboundStreams {
		t.Fatalf("expected at most %d inbound streams, got %d", maxInboundStreams, streamCount)
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
