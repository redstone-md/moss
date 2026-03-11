package transport

import (
	"context"
	"strconv"
	"testing"
	"time"

	mcrypto "moss/internal/crypto"
)

func TestUDPHandshakeAndEncryptedPacketRoundTrip(t *testing.T) {
	clientIdentity, err := mcrypto.NewIdentity()
	if err != nil {
		t.Fatalf("client identity failed: %v", err)
	}
	serverIdentity, err := mcrypto.NewIdentity()
	if err != nil {
		t.Fatalf("server identity failed: %v", err)
	}
	serverListener, port, err := ListenUDP(0, HandshakeConfig{
		MeshID:   "mesh-udp-transport",
		PSK:      []byte("01234567890123456789012345678901"),
		Identity: serverIdentity,
	})
	if err != nil {
		t.Fatalf("ListenUDP server failed: %v", err)
	}
	defer serverListener.Close()

	clientListener, _, err := ListenUDP(0, HandshakeConfig{
		MeshID:   "mesh-udp-transport",
		PSK:      []byte("01234567890123456789012345678901"),
		Identity: clientIdentity,
	})
	if err != nil {
		t.Fatalf("ListenUDP client failed: %v", err)
	}
	defer clientListener.Close()

	type result struct {
		session *Session
		err     error
	}
	serverCh := make(chan result, 1)
	go func() {
		session, err := serverListener.Accept()
		serverCh <- result{session: session, err: err}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	clientSession, err := clientListener.DialContext(ctx, "127.0.0.1:"+strconv.Itoa(port))
	if err != nil {
		t.Fatalf("DialContext failed: %v", err)
	}
	defer clientSession.Close()

	serverRes := <-serverCh
	if serverRes.err != nil {
		t.Fatalf("Accept failed: %v", serverRes.err)
	}
	defer serverRes.session.Close()

	if got := clientSession.RemoteID(); got != serverIdentity.PublicKey() {
		t.Fatal("client session did not bind responder identity")
	}
	if got := serverRes.session.RemoteID(); got != clientIdentity.PublicKey() {
		t.Fatal("server session did not bind initiator identity")
	}

	received := make(chan []byte, 1)
	go func() {
		payload, err := serverRes.session.ReadPacket()
		if err != nil {
			received <- nil
			return
		}
		received <- payload
	}()
	if err := clientSession.WritePacket([]byte("hello-udp-moss")); err != nil {
		t.Fatalf("WritePacket failed: %v", err)
	}

	select {
	case payload := <-received:
		if string(payload) != "hello-udp-moss" {
			t.Fatalf("unexpected payload: %q", string(payload))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for udp packet")
	}
}
