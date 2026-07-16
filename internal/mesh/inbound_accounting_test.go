package mesh

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"
)

// Every UDP session in production dies on exactly six unanswered pings — 60 of
// 60, 5 of 5 — while TCP sessions average 0.1 misses. UDP hides the reason: a
// write to a dead remote succeeds locally, so "we sent six pings" proves
// nothing about whether anything left the machine.
//
// Counting what ARRIVES separates the two candidates. A session that receives
// nothing was writing into a void; one that receives data but no pongs would
// mean the reply is broken, not the route. Without this the two are
// indistinguishable, and guessing between them has a poor record here.
func TestSessionCountsWhatArrives(t *testing.T) {
	a := startOverlayNode(t, "room")
	b := startOverlayNode(t, "room")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := a.connectPeer(ctx, net.JoinHostPort("127.0.0.1", strconv.Itoa(b.ListenPort()))); err != nil {
		t.Fatalf("connect: %v", err)
	}
	peer := a.peerByID(b.localPeerID())
	if peer == nil {
		t.Fatal("no session")
	}

	// A healthy session must show arrivals: the peers exchange announcements and
	// pings unprompted. Zero here would be the very signature we are hunting.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if peer.inboundPackets.Load() > 0 {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("a live session recorded zero arrivals; the counter is not wired, "+
		"and the measurement it exists for would silently read as a dead path")
}
