package transport

import (
	"context"

	"strconv"
	"testing"
	"time"

	mcrypto "github.com/redstone-md/moss/internal/crypto"
)

// A finished inbound session that cannot be queued for Accept must be
// dropped visibly, never by stalling the read loop.
//
// handleHandshakeDone used to send into acceptC with a blocking send — on
// the listener's single read-loop goroutine. With the old 16-deep backlog
// and a fleet-sized dial wave (100 peers, every maintenance pass firing
// together) the loop froze solid: no datagrams of any kind were processed,
// node-wide, until the mesh's Accept loop drained. That is the datagram twin
// of the stream HOL that killed sessions at 37-38s.
//
// The proof is end-to-end: fill the backlog, complete a real handshake, and
// watch the session be dropped and counted — then complete a SECOND
// handshake, which could only work if the read loop kept processing
// datagrams after the overflow (a blocking send would have frozen it
// forever).
func TestAcceptBacklogOverflowIsCountedNotStalling(t *testing.T) {
	serverListener, port, err := ListenUDP(0, HandshakeConfig{
		MeshID:   "mesh-udp-accept-overflow",
		Identity: mustTestIdentity(t, "server"),
	})
	if err != nil {
		t.Fatalf("ListenUDP server failed: %v", err)
	}
	defer serverListener.Close()

	// Fill the backlog to capacity so the first finished handshake has
	// nowhere to go. Nobody calls Accept in this test.
	serverListener.mu.Lock()
	for len(serverListener.acceptC) < cap(serverListener.acceptC) {
		serverListener.acceptC <- &Session{}
	}
	serverListener.mu.Unlock()

	before := UDPAcceptDrops()

	// Dial 1: completes (the client finishes at the server's handshakeResp),
	// but the server's finished session cannot queue and must be dropped.
	ctx1, cancel1 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel1()
	clientListener1, _, err := ListenUDP(0, HandshakeConfig{
		MeshID:   "mesh-udp-accept-overflow",
		Identity: mustTestIdentity(t, "client1"),
	})
	if err != nil {
		t.Fatalf("ListenUDP client1 failed: %v", err)
	}
	defer clientListener1.Close()
	if _, err := clientListener1.DialContext(ctx1, "127.0.0.1:"+strconv.Itoa(port)); err != nil {
		t.Fatalf("first dial failed: %v", err)
	}
	waitFor(t, 2*time.Second, func() bool {
		return UDPAcceptDrops()-before == 1
	})
	if got := UDPAcceptDrops() - before; got != 1 {
		t.Fatalf("expected exactly 1 counted drop after the first overflowed session, got %d", got)
	}

	// Dial 2: only possible if the read loop survived the overflow. A
	// blocking send would have frozen it with the backlog full, this dial
	// would never see a handshakeResp, and the context would expire.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	clientListener2, _, err := ListenUDP(0, HandshakeConfig{
		MeshID:   "mesh-udp-accept-overflow",
		Identity: mustTestIdentity(t, "client2"),
	})
	if err != nil {
		t.Fatalf("ListenUDP client2 failed: %v", err)
	}
	defer clientListener2.Close()
	if _, err := clientListener2.DialContext(ctx2, "127.0.0.1:"+strconv.Itoa(port)); err != nil {
		t.Fatalf("second dial failed — the read loop did not survive the accept overflow: %v", err)
	}
	waitFor(t, 2*time.Second, func() bool {
		return UDPAcceptDrops()-before == 2
	})
	if got := UDPAcceptDrops() - before; got != 2 {
		t.Fatalf("expected exactly 2 counted drops, got %d", got)
	}

	// The dropped sessions must not linger in the listener's table: the
	// overflow path closed them, and Close deregisters.
	serverListener.mu.Lock()
	remaining := len(serverListener.sessions)
	serverListener.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("overflowed sessions left %d carriers registered; closed sessions must deregister", remaining)
	}
}

func mustTestIdentity(t *testing.T, name string) *mcrypto.Identity {
	t.Helper()
	identity, err := mcrypto.NewIdentity()
	if err != nil {
		t.Fatalf("identity %s failed: %v", name, err)
	}
	return identity
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Accept drains sessions that finished before Close, then reports EOF — the
// channel is never closed, so a session that completed its handshake during
// shutdown is still delivered rather than lost to a closed channel.
func TestAcceptDrainsAfterCloseThenEOF(t *testing.T) {
	l := &UDPListener{
		acceptC:  make(chan *Session, 1),
		closed:   make(chan struct{}),
		sessions: make(map[string]*udpCarrier),
	}
	close(l.closed)
	l.acceptC <- &Session{}

	session, err := l.Accept()
	if err != nil {
		t.Fatalf("Accept after close dropped a finished session: %v", err)
	}
	if session == nil {
		t.Fatal("Accept after close returned nil session without error")
	}
	if _, err := l.Accept(); err == nil {
		t.Fatal("Accept after draining did not report EOF")
	}
}
