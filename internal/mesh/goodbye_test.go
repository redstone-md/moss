package mesh

import (
	"context"
	"testing"
	"time"
)

func waitForPeer(t *testing.T, n *Node, peerID string, want bool) bool {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if (n.peerByID(peerID) != nil) == want {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

// A session teardown must reach the other end.
//
// TCP has FIN. A datagram carrier has nothing at all, so Session.Close() on a
// UDP session is a purely local act: the far side keeps a peer it will never
// hear from again, reports it to the application as connected, and only drops it
// six unanswered pings — about 37 seconds — later. The fleet's hole-punched
// sessions died at a median of exactly 37s and 14 of 15 had received no packet
// at all: dead links nobody had told, with the game still writing into them.
//
// The farewell is sent here WITHOUT closing the session, so nothing but the
// goodbye itself can be what tears the peer down — a test that closed the
// carrier would pass on TCP's FIN alone and prove nothing.
func TestGoodbyeTearsDownTheFarSideAtOnce(t *testing.T) {
	a := startOverlayNode(t, "room")
	b := startOverlayNode(t, "room")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := a.connectPeer(ctx, nodeAddr(b)); err != nil {
		t.Fatalf("connect: %v", err)
	}
	if !waitForPeer(t, b, a.localPeerID(), true) {
		t.Fatal("B never saw A")
	}

	peer := a.peerByID(b.localPeerID())
	if peer == nil || peer.session == nil {
		t.Fatal("A holds no session with B")
	}
	farewell(peer.session) // deliberately no Close: the carrier stays up

	if !waitForPeer(t, b, a.localPeerID(), false) {
		t.Fatal("B still holds A after a goodbye: the far side of every dropped " +
			"UDP session stays a ghost for ~37s, and the game keeps writing to it")
	}
}

// A goodbye must only ever tear down the session it arrived on.
//
// The dedup rejects duplicates by closing them, and that close now says goodbye —
// so a goodbye routinely arrives on a session the far side has ALREADY replaced
// with a better one. If it tore down the peer by identity, every duplicate the
// dedup cleaned up would take the live link down with it, turning a fix for
// ghosts into a disconnect storm.
func TestGoodbyeOnAStaleSessionCannotKillTheLiveLink(t *testing.T) {
	a := startOverlayNode(t, "room")
	b := startOverlayNode(t, "room")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := a.connectPeer(ctx, nodeAddr(b)); err != nil {
		t.Fatalf("connect: %v", err)
	}
	if !waitForPeer(t, b, a.localPeerID(), true) {
		t.Fatal("B never saw A")
	}

	live := b.peerByID(a.localPeerID())
	if live == nil {
		t.Fatal("B holds no peer for A")
	}
	// A goodbye that names A but arrives bearing a session B no longer holds.
	b.removePeer(a.localPeerID(), nil)

	if b.peerByID(a.localPeerID()) == nil {
		t.Fatal("a teardown for a session B does not hold removed the live peer anyway")
	}
}
