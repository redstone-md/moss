package transport

import (
	"context"
	"net"
	"testing"
	"time"

	mcrypto "github.com/redstone-md/moss/internal/crypto"
)

type pskHandshakeResult struct {
	session *Session
	err     error
}

// runPSKHandshake performs one client/server handshake pair over net.Pipe and
// reports both outcomes, so a test can assert on either side independently.
func runPSKHandshake(t *testing.T, clientPSK, serverPSK []byte) (pskHandshakeResult, pskHandshakeResult) {
	t.Helper()
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
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	go func() {
		session, err := ClientHandshake(ctx, clientConn, HandshakeConfig{
			MeshID:   "mesh-psk-isolation",
			PSK:      clientPSK,
			Identity: clientIdentity,
		})
		clientCh <- result{session: session, err: err}
	}()
	go func() {
		session, err := ServerHandshake(ctx, serverConn, HandshakeConfig{
			MeshID:   "mesh-psk-isolation",
			PSK:      serverPSK,
			Identity: serverIdentity,
		})
		serverCh <- result{session: session, err: err}
	}()
	clientRes := <-clientCh
	serverRes := <-serverCh
	if clientRes.session != nil {
		defer clientRes.session.Close()
	}
	if serverRes.session != nil {
		defer serverRes.session.Close()
	}
	return pskHandshakeResult{clientRes.session, clientRes.err}, pskHandshakeResult{serverRes.session, serverRes.err}
}

// Two nodes agreeing on the PSK must interoperate: the PSK is a gate, not a
// fork. If the knob broke the common case, isolation would arrive together
// with silence.
func TestPSKHandshakeSameKeySucceeds(t *testing.T) {
	client, server := runPSKHandshake(t, []byte("01234567890123456789012345678901"), []byte("01234567890123456789012345678901"))
	if client.err != nil {
		t.Fatalf("client handshake with matching PSK failed: %v", client.err)
	}
	if server.err != nil {
		t.Fatalf("server handshake with matching PSK failed: %v", server.err)
	}
	if got := client.session.RemoteID(); got == [32]byte{} {
		t.Fatal("client session did not bind responder identity")
	}
}

// A peer without the PSK must not get past the handshake — this is the entire
// point of transport-level isolation. Both sides must fail, not hang.
func TestPSKHandshakeMismatchIsRejected(t *testing.T) {
	client, server := runPSKHandshake(t, []byte("01234567890123456789012345678901"), []byte("abcdefghijklmnopqrstuvwxyz012345"))
	if client.err == nil {
		t.Fatal("client handshake with mismatched PSK unexpectedly succeeded")
	}
	if server.err == nil {
		t.Fatal("server handshake with mismatched PSK unexpectedly succeeded")
	}
}

// A PSK-gated node must reject an open (PSK-less) peer just as it rejects a
// wrong-PSK peer: isolation is against everyone who does not hold the key,
// including honest peers that simply never configured one.
func TestPSKHandshakeRejectsPeerWithoutPSK(t *testing.T) {
	client, server := runPSKHandshake(t, []byte("01234567890123456789012345678901"), nil)
	if client.err == nil {
		t.Fatal("PSK-carrying client unexpectedly handshook with an open server")
	}
	if server.err == nil {
		t.Fatal("open server unexpectedly handshook with a PSK-carrying client")
	}
}

// The PSK derivation is deterministic: mesh must be able to derive the same
// transport PSK on both sides from the same room material. Different network
// IDs must derive different transport keys, so one leaked PSK never crosses
// substrate boundaries. The mesh-side derivation is mirrored here against
// the constant the transport layer accepts (32 bytes).
func TestPSKHandshakeRejectsPeerWithDifferentMeshID(t *testing.T) {
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
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	clientCh := make(chan error, 1)
	serverCh := make(chan error, 1)
	go func() {
		_, err := ClientHandshake(ctx, clientConn, HandshakeConfig{
			MeshID:   "mesh-psk-a",
			PSK:      []byte("01234567890123456789012345678901"),
			Identity: clientIdentity,
		})
		clientCh <- err
	}()
	go func() {
		_, err := ServerHandshake(ctx, serverConn, HandshakeConfig{
			MeshID:   "mesh-psk-b",
			PSK:      []byte("01234567890123456789012345678901"),
			Identity: serverIdentity,
		})
		serverCh <- err
	}()
	if err := <-clientCh; err == nil {
		t.Fatal("handshake across different mesh IDs unexpectedly succeeded")
	}
	if err := <-serverCh; err == nil {
		t.Fatal("handshake across different mesh IDs unexpectedly succeeded")
	}
}
