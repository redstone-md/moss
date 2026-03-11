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

func TestUDPReconnectUsesIKWithCachedRemoteStatic(t *testing.T) {
	clientIdentity, err := mcrypto.NewIdentity()
	if err != nil {
		t.Fatalf("client identity failed: %v", err)
	}
	serverIdentity, err := mcrypto.NewIdentity()
	if err != nil {
		t.Fatalf("server identity failed: %v", err)
	}
	serverListener, port, err := ListenUDP(0, HandshakeConfig{
		MeshID:   "mesh-udp-ik",
		Identity: serverIdentity,
	})
	if err != nil {
		t.Fatalf("ListenUDP server failed: %v", err)
	}
	defer serverListener.Close()

	clientListener, _, err := ListenUDP(0, HandshakeConfig{
		MeshID:   "mesh-udp-ik",
		Identity: clientIdentity,
	})
	if err != nil {
		t.Fatalf("ListenUDP client failed: %v", err)
	}
	defer clientListener.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	serverCh1 := make(chan *Session, 1)
	go func() {
		session, _ := serverListener.Accept()
		serverCh1 <- session
	}()
	clientSession1, err := clientListener.DialContext(ctx, "127.0.0.1:"+strconv.Itoa(port))
	if err != nil {
		t.Fatalf("DialContext first failed: %v", err)
	}
	serverSession1 := <-serverCh1
	if clientSession1.HandshakeMode() != HandshakeModeXX || serverSession1.HandshakeMode() != HandshakeModeXX {
		t.Fatalf("expected first udp handshake to use XX, got client=%d server=%d", clientSession1.HandshakeMode(), serverSession1.HandshakeMode())
	}
	remoteStatic := clientSession1.RemoteStaticPublic()
	_ = clientSession1.Close()
	_ = serverSession1.Close()
	time.Sleep(100 * time.Millisecond)

	serverCh2 := make(chan *Session, 1)
	go func() {
		session, _ := serverListener.Accept()
		serverCh2 <- session
	}()
	clientSession2, err := clientListener.DialPeerContext(ctx, "127.0.0.1:"+strconv.Itoa(port), remoteStatic[:])
	if err != nil {
		t.Fatalf("DialPeerContext second failed: %v", err)
	}
	serverSession2 := <-serverCh2
	defer clientSession2.Close()
	defer serverSession2.Close()
	if clientSession2.HandshakeMode() != HandshakeModeIK || serverSession2.HandshakeMode() != HandshakeModeIK {
		t.Fatalf("expected reconnect udp handshake to use IK, got client=%d server=%d", clientSession2.HandshakeMode(), serverSession2.HandshakeMode())
	}
	if got := clientSession2.RemoteID(); got != serverIdentity.PublicKey() {
		t.Fatal("udp reconnect session did not bind responder identity")
	}
}
