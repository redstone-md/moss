package transport

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"net"
	"strings"
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

func TestReadFrameRejectsOversizedFrame(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	errCh := make(chan error, 1)
	go func() {
		header := make([]byte, 4)
		binary.BigEndian.PutUint32(header, maxHandshakeFrameSize+1)
		_, err := clientConn.Write(header)
		errCh <- err
	}()

	_, err := readFrame(t.Context(), serverConn)
	if err == nil || !strings.Contains(err.Error(), "handshake frame too large") {
		t.Fatalf("expected oversized frame error, got %v", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("write oversized frame header failed: %v", err)
	}
}

func TestVerifyIdentityPayloadRejectsInvalidSignatureLength(t *testing.T) {
	identity, err := mcrypto.NewIdentity()
	if err != nil {
		t.Fatalf("NewIdentity failed: %v", err)
	}
	raw, err := json.Marshal(identityPayload{
		IdentityPub: identity.PublicKeyBytes(),
		Signature:   make([]byte, 63),
	})
	if err != nil {
		t.Fatalf("marshal identity payload failed: %v", err)
	}
	var out [32]byte
	err = verifyIdentityPayload(raw, "mesh-invalid-signature", identity.NoiseStaticPublic(), &out)
	if err == nil || !strings.Contains(err.Error(), "invalid identity signature length") {
		t.Fatalf("expected invalid signature length error, got %v", err)
	}
}

func TestReconnectUsesIKWithCachedRemoteStatic(t *testing.T) {
	clientIdentity, err := mcrypto.NewIdentity()
	if err != nil {
		t.Fatalf("client identity failed: %v", err)
	}
	serverIdentity, err := mcrypto.NewIdentity()
	if err != nil {
		t.Fatalf("server identity failed: %v", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen failed: %v", err)
	}
	defer listener.Close()

	acceptHandshake := func() *Session {
		t.Helper()
		conn, err := listener.Accept()
		if err != nil {
			t.Fatalf("Accept failed: %v", err)
		}
		session, err := ServerHandshake(t.Context(), conn, HandshakeConfig{
			MeshID:   "mesh-ik",
			Identity: serverIdentity,
		})
		if err != nil {
			t.Fatalf("ServerHandshake failed: %v", err)
		}
		return session
	}

	clientConn1, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("Dial first failed: %v", err)
	}
	serverDone1 := make(chan *Session, 1)
	go func() {
		serverDone1 <- acceptHandshake()
	}()
	clientSession1, err := ClientHandshake(t.Context(), clientConn1, HandshakeConfig{
		MeshID:   "mesh-ik",
		Identity: clientIdentity,
	})
	if err != nil {
		t.Fatalf("ClientHandshake first failed: %v", err)
	}
	serverSession1 := <-serverDone1
	if clientSession1.HandshakeMode() != HandshakeModeXX || serverSession1.HandshakeMode() != HandshakeModeXX {
		t.Fatalf("expected first handshake to use XX, got client=%d server=%d", clientSession1.HandshakeMode(), serverSession1.HandshakeMode())
	}
	remoteStatic := clientSession1.RemoteStaticPublic()
	_ = clientSession1.Close()
	_ = serverSession1.Close()

	clientConn2, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("Dial second failed: %v", err)
	}
	serverDone2 := make(chan *Session, 1)
	go func() {
		serverDone2 <- acceptHandshake()
	}()
	clientSession2, err := ClientHandshake(t.Context(), clientConn2, HandshakeConfig{
		MeshID:       "mesh-ik",
		Identity:     clientIdentity,
		RemoteStatic: remoteStatic[:],
	})
	if err != nil {
		t.Fatalf("ClientHandshake second failed: %v", err)
	}
	serverSession2 := <-serverDone2
	defer clientSession2.Close()
	defer serverSession2.Close()
	if clientSession2.HandshakeMode() != HandshakeModeIK || serverSession2.HandshakeMode() != HandshakeModeIK {
		t.Fatalf("expected reconnect handshake to use IK, got client=%d server=%d", clientSession2.HandshakeMode(), serverSession2.HandshakeMode())
	}
	if got := clientSession2.RemoteID(); got != serverIdentity.PublicKey() {
		t.Fatal("client reconnect session did not bind responder identity")
	}
}
