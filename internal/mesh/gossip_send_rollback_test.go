package mesh

// The ask/serve cooldowns are the claim "an envelope for this id is in
// flight to this peer", so they may only stand when the send actually
// happened. A dropped envelope (full outbound queue, a closed session,
// a send racing teardown) with the marker still recorded silences the
// ONLY recovery path a fresh payload has: the next IHAVE naming the id
// finds the stale marker and never re-asks, and once the id ages out of
// the announce ring it becomes permanently unrequestable. These tests pin
// the rollback: a failed ask/serve leaves no marker, and the retry goes
// through.

import (
	"testing"

	"github.com/redstone-md/moss/internal/gossip"
)

// A peer with no session on a stopped node fails the send: the IWANT never
// leaves, so the ask markers must not stand — the next IHAVE re-asks.
func TestFailedAskLeavesNoCooldownMarker(t *testing.T) {
	node, err := NewNode("mesh-ask-rollback", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	node.pubsub.Subscribe("alpha")

	peer := &peerConn{id: "peer-dead"} // no session: the send fails
	env := gossip.Envelope{
		Type:       gossip.TypeIHave,
		Channel:    "alpha",
		MessageIDs: []string{"msg-1", "msg-2"},
	}

	node.handleIHave(peer, env)
	if asks := node.iwantAsks["peer-dead"]; len(asks) != 0 {
		t.Fatalf("a failed IWANT enqueue left %d ask markers; a cooldown may only stand for envelopes that actually left the node", len(asks))
	}

	// The same IHAVE again must retry rather than sit out the cooldown for
	// an envelope that is not in flight. With a live session this time, the
	// ask goes out on the first retry — proving the rollback, not the
	// cooldown expiry, is what allowed it.
	sender := newRecordedSession(t)
	live := &peerConn{id: "peer-dead", session: sender.session}
	node.handleIHave(live, env)
	if got := sender.writeCount(); got != 1 {
		t.Fatalf("expected the retry after a failed ask to send one IWANT, got %d packets", got)
	}
}

// The serve side of the same contract: a replay whose enqueue fails must
// not count as served, or the asker misses the payload for the whole serve
// cooldown while the announcer believes it was delivered.
func TestFailedServeLeavesNoCooldownMarker(t *testing.T) {
	node, err := NewNode("mesh-serve-rollback", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	node.cache.Store(gossip.Envelope{
		Type:      gossip.TypePublish,
		Channel:   "alpha",
		MessageID: "msg-1",
		Payload:   []byte("payload"),
	})

	peer := &peerConn{id: "peer-dead"} // no session: the replay fails
	node.handleIWant(peer, gossip.Envelope{Type: gossip.TypeIWant, MessageIDs: []string{"msg-1"}})
	if served := node.iwantServes["peer-dead"]; len(served) != 0 {
		t.Fatalf("a failed replay left %d serve markers; the asker would be throttled for the whole cooldown without ever receiving the payload", len(served))
	}

	// The retry, now with a live session, must replay instead of being
	// swallowed by a marker standing for an envelope that never left.
	sender := newRecordedSession(t)
	live := &peerConn{id: "peer-dead", session: sender.session}
	node.handleIWant(live, gossip.Envelope{Type: gossip.TypeIWant, MessageIDs: []string{"msg-1"}})
	if got := sender.writeCount(); got != 1 {
		t.Fatalf("expected the retry after a failed serve to replay the payload, got %d packets", got)
	}
}
