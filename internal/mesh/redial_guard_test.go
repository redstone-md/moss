package mesh

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"
)

// A client reported 1131 sessions in fifteen minutes with a median lifetime of
// ZERO milliseconds, all from dial_tcp — full Noise handshakes to peers it was
// already connected to, closed by the dedup the instant they completed. About
// 75 wasted handshakes a minute; handshakes are asymmetric crypto, and players
// felt the pile as stalls.
//
// The guard matched on the address string alone. A session remembers the
// address it was OBSERVED on; we dial the one the peer ADVERTISES, and for
// anything behind NAT those differ — so the guard missed every time and we
// redialed forever. Identity is what "already connected" means.
func TestDoesNotRedialAPeerWeAlreadyHoldUnderAnotherAddress(t *testing.T) {
	a := startOverlayNode(t, "room")
	b := startOverlayNode(t, "room")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	dialed := net.JoinHostPort("127.0.0.1", strconv.Itoa(b.ListenPort()))
	if err := a.connectPeer(ctx, dialed); err != nil {
		t.Fatalf("connect: %v", err)
	}
	peer := a.peerByID(b.localPeerID())
	if peer == nil {
		t.Fatal("no session formed")
	}

	// The same peer, reached under an address our session does not carry —
	// exactly what an advertised address looks like next to an observed one.
	other := net.JoinHostPort("localhost", strconv.Itoa(b.ListenPort()))
	if other == peer.addr {
		t.Skip("addresses coincide; nothing to prove here")
	}

	waitForSessions(t, b, 1)
	before := sessionCount(t, b)
	if err := a.connectPeerWithHint(ctx, other, b.localPeerID()); err != nil {
		t.Fatalf("guard should decline quietly, not error: %v", err)
	}
	time.Sleep(300 * time.Millisecond)

	// No second handshake may have reached the far side.
	if after := sessionCount(t, b); after != before {
		t.Fatalf("a redial got through: the far side handshook %d extra time(s). "+
			"Knowing WHO we are dialing must beat matching an address string", after-before)
	}
}

// Without an id we can only compare addresses — that path must still work.
func TestAddressGuardStillHoldsWithoutAnID(t *testing.T) {
	a := startOverlayNode(t, "room")
	b := startOverlayNode(t, "room")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(b.ListenPort()))
	if err := a.connectPeer(ctx, addr); err != nil {
		t.Fatalf("connect: %v", err)
	}
	// Let the first session settle on BOTH sides before measuring, or we race
	// our own setup and blame the guard for it.
	waitForSessions(t, b, 1)
	before := sessionCount(t, b)
	// connectPeer passes no id; the same address must still be declined.
	if err := a.connectPeer(ctx, addr); err != nil {
		t.Fatalf("same-address redial should be declined quietly: %v", err)
	}
	time.Sleep(300 * time.Millisecond)
	if after := sessionCount(t, b); after != before {
		t.Fatalf("the address guard stopped working: %d extra handshake(s)", after-before)
	}
}

// waitForSessions blocks until the node holds want sessions, so a measurement
// is not racing the setup that produces it.
func waitForSessions(t *testing.T, n *Node, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if sessionCount(t, n) >= want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("node never reached %d session(s); have %d", want, sessionCount(t, n))
}

func sessionCount(t *testing.T, n *Node) int {
	t.Helper()
	n.mu.RLock()
	defer n.mu.RUnlock()
	return len(n.peers)
}
