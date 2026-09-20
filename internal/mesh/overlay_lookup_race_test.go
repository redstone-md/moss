package mesh

// Overlay lookup race harness. The fleet census caught the race detector
// twice on the public-mesh harness and once on clean e43d572 (git worktree)
// — a pre-existing defect CI could not see because the census harness is
// not in the tree. This file pins it with a deterministic, fleet-free unit.
//
// The shape: overlayLookup fans a batch of alpha goroutines out to write
// results[i] (each with its own 4s query deadline). The outer select can
// give up first — ctx.Done or the 12s batch bound — and the reader loop
// then walks the results slice WHILE stragglers are still parked inside
// overlayQuery, about to write. A lookup under load against a busy overlay
// is exactly this: in-flight queries, a ctx cancelled by the caller, and a
// straggler writing as the loop reads.

import (
	"context"
	"testing"
	"time"

	mcrypto "github.com/redstone-md/moss/internal/crypto"
	"github.com/redstone-md/moss/internal/overlay"
	"github.com/redstone-md/moss/internal/transport"
)

// silentOverlayContact wires one decoy contact into the node's overlay table:
// reachable by session (peerByID finds it) but never answering a mesh
// envelope — the noise handshake completes (pure transport, see
// ghostUDPListener), then silence. Every overlayQuery against it parks for
// its full per-query window, which is what keeps the batch goroutines alive
// past the abandon point.
//
// Its ID sits right on top of the lookup key, so the batch cannot skip it.
func silentOverlayContact(t *testing.T, n *Node, key overlay.NodeID, index byte) {
	t.Helper()
	n.mu.RLock()
	networkID := n.networkID
	n.mu.RUnlock()

	ghostAddr := ghostUDPListener(t, networkID)

	id := key
	id[overlay.IDLen-1] ^= index // distance 1, 2, 3... — nearer than any real node

	// A live transport session to the ghost, dialed from a raw listener of
	// its own so the ghost sees a distinct peer per session. The session
	// exists so overlayCanReach passes and the batch spends a slot here —
	// exactly how a busy core that stopped answering looks from the leaf.
	dialer, _, err := transport.ListenUDP(0, transport.HandshakeConfig{
		MeshID:   networkID,
		Identity: mustGhostIdentity(t),
	})
	if err != nil {
		t.Fatalf("raw dialer listener: %v", err)
	}
	t.Cleanup(func() { _ = dialer.Close() })
	dialCtx, cancelDial := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelDial()
	session, err := dialer.DialPeerContext(dialCtx, ghostAddr, nil)
	if err != nil {
		t.Fatalf("ghost session dial: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })

	peerID := id.String()
	n.mu.Lock()
	n.peers[peerID] = &peerConn{id: peerID, addr: ghostAddr, session: session}
	n.mu.Unlock()
	n.overlayTable.Add(overlay.Contact{ID: id, Addr: ghostAddr, LastSeen: time.Now()})
}

func mustGhostIdentity(t *testing.T) *mcrypto.Identity {
	t.Helper()
	identity, err := mcrypto.NewIdentity()
	if err != nil {
		t.Fatalf("ghost dialer identity: %v", err)
	}
	return identity
}

// TestOverlayLookupAbandonedBatchDoesNotRaceResults pins the
// read-while-stragglers-write defect: cancel the lookup ctx while the batch
// is still querying silent contacts, and the abandon path must not read the
// slots the writers still own.
//
// Before the fix this is a guaranteed data race under -race: the writers'
// results[i] assignment is still pending when the reader dereferences the
// same slice. After the fix the reader consumes only what has arrived (the
// results flow through a channel), and late stragglers finish into a buffer
// nobody reads unsynchronized.
//
// Rounds repeat the abandon three times: one round already reproduces, but
// the select's case choice is scheduler-dependent, and a regression test
// must not depend on winning a coin flip — each round cancels the ctx while
// every query is parked, so the abandon path is the only one that can fire.
func TestOverlayLookupAbandonedBatchDoesNotRaceResults(t *testing.T) {
	n := startOverlayNode(t, "overlay-race-room")
	key := overlay.ChannelKey(n.roomTopic("raced"))
	for i := 0; i < overlayAlpha; i++ {
		silentOverlayContact(t, n, key, byte(i+1))
	}

	for round := 0; round < 3; round++ {
		// Short enough that the deadline fires while every query is still
		// parked in overlayQuery's reply select (its own window is 4s).
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		providers, closest := n.overlayLookup(ctx, key, true)
		cancel()

		// Abandon-path semantics that must survive the fix: the shortlist
		// is still returned, and a timed-out contact is not dropped from
		// the table for one missed query (the fleet's table-drain bug).
		if len(closest) == 0 {
			t.Fatalf("round %d: lookup returned an empty shortlist — the abandon path lost the table's answer", round)
		}
		if len(providers) != 0 {
			t.Fatalf("round %d: silent contacts produced providers; the batch queried something else", round)
		}
		if n.overlayTable.Len() < overlayAlpha {
			t.Fatalf("round %d: silent contacts were evicted for one abandoned query (table=%d)", round, n.overlayTable.Len())
		}
		// Stragglers of this round must be out of overlayQuery before the
		// next one re-enters the same node state.
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			n.overlayMu.Lock()
			pending := len(n.overlayPending)
			n.overlayMu.Unlock()
			if pending == 0 {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}
