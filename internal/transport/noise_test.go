package transport

import (
	"context"
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
	if got := clientRes.session.RemoteID(); got != serverIdentity.PublicKey() {
		t.Fatal("client session did not bind responder identity")
	}
	if got := serverRes.session.RemoteID(); got != clientIdentity.PublicKey() {
		t.Fatal("server session did not bind initiator identity")
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

func TestHandshakeFailsOnMeshMismatch(t *testing.T) {
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
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	clientCh := make(chan error, 1)
	serverCh := make(chan error, 1)
	go func() {
		_, err := ClientHandshake(ctx, clientConn, HandshakeConfig{
			MeshID:   "mesh-a",
			Identity: clientIdentity,
		})
		clientCh <- err
	}()
	go func() {
		_, err := ServerHandshake(ctx, serverConn, HandshakeConfig{
			MeshID:   "mesh-b",
			Identity: serverIdentity,
		})
		serverCh <- err
	}()

	if err := <-clientCh; err == nil {
		t.Fatal("client handshake unexpectedly succeeded")
	}
	if err := <-serverCh; err == nil {
		t.Fatal("server handshake unexpectedly succeeded")
	}
}
