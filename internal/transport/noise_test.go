package transport

import (
	"net"
	"testing"
	"time"

	mcrypto "moss/internal/crypto"
)

func TestHandshakeAndEncryptedPacketRoundTrip(t *testing.T) {
	clientIdentity, err := mcrypto.NewIdentity()
	if err != nil {
		t.Fatalf("client identity failed: %v", err)
	}
	serverIdentity, err := mcrypto.NewIdentity()
	if err != nil {
		t.Fatalf("server identity failed: %v", err)
	}
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	type result struct {
		session *Session
		err     error
	}
	clientCh := make(chan result, 1)
	serverCh := make(chan result, 1)
	go func() {
		session, err := ClientHandshake(t.Context(), clientConn, HandshakeConfig{
			MeshID:   "mesh-transport",
			PSK:      []byte("01234567890123456789012345678901"),
			Identity: clientIdentity,
		})
		clientCh <- result{session: session, err: err}
	}()
	go func() {
		session, err := ServerHandshake(t.Context(), serverConn, HandshakeConfig{
			MeshID:   "mesh-transport",
			PSK:      []byte("01234567890123456789012345678901"),
			Identity: serverIdentity,
		})
		serverCh <- result{session: session, err: err}
	}()

	clientRes := <-clientCh
	serverRes := <-serverCh
	if clientRes.err != nil {
		t.Fatalf("client handshake failed: %v", clientRes.err)
	}
	if serverRes.err != nil {
		t.Fatalf("server handshake failed: %v", serverRes.err)
	}
	defer clientRes.session.Close()
	defer serverRes.session.Close()

	done := make(chan []byte, 1)
	go func() {
		payload, err := serverRes.session.ReadPacket()
		if err != nil {
			done <- nil
			return
		}
		done <- payload
	}()
	if err := clientRes.session.WritePacket([]byte("hello-moss")); err != nil {
		t.Fatalf("WritePacket failed: %v", err)
	}

	select {
	case payload := <-done:
		if string(payload) != "hello-moss" {
			t.Fatalf("unexpected payload: %q", string(payload))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for encrypted packet")
	}
}
