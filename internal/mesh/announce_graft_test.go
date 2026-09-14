package mesh

import (
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
)

// The subscription-announce path must obey the same graft contract as the
// mesh-maintenance path: a GRAFT per peer per channel per retry window, the
// marker recorded so a PRUNE answering it is recognized as join choreography,
// and no GRAFT to a peer that just told us it wants out.
//
// Before this, announce-path GRAFTs recorded no graftedAt: meshGraftedWithin
// could not see them, so a PRUNE answering an announce took the LONG 30s
// block instead of the 2s one, and every node re-announcing to every peer on
// each refresh pass re-sent the same GRAFTs forever — a standing GRAFT/PRUNE
// cycle with strangers instead of a mesh.

// A peer is announced a channel once per retry window, not once per refresh
// pass: the second announce inside meshGraftRetryInterval is suppressed by
// the recorded graft marker, exactly as a maintenance graft would be.
func TestAnnounceGraftIsThrottledPerWindow(t *testing.T) {
	node, err := NewNode("mesh-announce-graft-throttle", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	announced := newRecordedSession(t)
	peer := &peerConn{id: "peer-quiet", session: announced.session}
	node.mu.Lock()
	node.peers["peer-quiet"] = peer
	node.mu.Unlock()

	// A node with no local subscriptions announces nothing, and records
	// nothing.
	node.announceLocalSubscriptionsToPeer(peer)
	if got := announced.writeCount(); got != 0 {
		t.Fatalf("expected no GRAFT with no local subscriptions, got %d", got)
	}
	node.mu.RLock()
	_, grafted := peer.graftedAt["alpha"]
	node.mu.RUnlock()
	if grafted {
		t.Fatal("a GRAFT was recorded for a channel this node does not subscribe to")
	}

	node.pubsub.Subscribe("alpha")
	node.announceLocalSubscriptionsToPeer(peer)
	node.mu.RLock()
	_, grafted = peer.graftedAt["alpha"]
	node.mu.RUnlock()
	if !grafted {
		t.Fatal("an announce-graft did not record its graft marker")
	}
	if got := announced.writeCount(); got != 1 {
		t.Fatalf("expected the first announce of a subscribed channel to send one GRAFT, got %d total", got)
	}

	// The refresh pass inside the retry window: suppressed.
	node.refreshLocalSubscriptions()
	if got := announced.writeCount(); got != 1 {
		t.Fatalf("expected a refresh inside the graft retry window to send nothing, got %d total", got)
	}

	// Backdate the marker past the window: the safety net may re-offer.
	node.mu.Lock()
	peer.graftedAt["alpha"] = time.Now().Add(-meshGraftRetryInterval - time.Second)
	node.mu.Unlock()
	node.refreshLocalSubscriptions()
	if got := announced.writeCount(); got != 2 {
		t.Fatalf("expected a refresh after the retry window to send one GRAFT, got %d total", got)
	}
}

// announceLocalSubscription (the Subscribe-path fan-out) applies the same
// throttle: every connected peer gets one GRAFT for the new channel, and a
// second call inside the window sends nothing.
func TestAnnounceLocalSubscriptionMarksGraftAndThrottles(t *testing.T) {
	node, err := NewNode("mesh-announce-sub-throttle", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	announced := newRecordedSession(t)
	peer := &peerConn{id: "peer-a", session: announced.session}
	node.mu.Lock()
	node.peers["peer-a"] = peer
	node.mu.Unlock()

	node.announceLocalSubscription("alpha")
	if got := announced.writeCount(); got != 1 {
		t.Fatalf("expected one GRAFT for the new channel, got %d", got)
	}
	node.mu.RLock()
	_, grafted := peer.graftedAt["alpha"]
	node.mu.RUnlock()
	if !grafted {
		t.Fatal("the Subscribe-path announce did not record its graft marker")
	}

	// A repeat announce inside the window — e.g. a second Subscribe call or a
	// concurrent refresh — must not re-send.
	node.announceLocalSubscription("alpha")
	if got := announced.writeCount(); got != 1 {
		t.Fatalf("expected a repeat announce inside the window to send nothing, got %d", got)
	}
}

// A peer that answered with PRUNE (meshBlocked) must not be re-grafted by the
// announce path; the block clears only when the TTL says so.
func TestAnnounceGraftRespectsMeshBlocked(t *testing.T) {
	node, err := NewNode("mesh-announce-graft-blocked", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	node.pubsub.Subscribe("alpha")

	announced := newRecordedSession(t)
	peer := &peerConn{id: "peer-refused", session: announced.session}
	node.mu.Lock()
	node.peers["peer-refused"] = peer
	peer.meshBlocked = time.Now().Add(30 * time.Second)
	node.mu.Unlock()

	node.announceLocalSubscriptionsToPeer(peer)
	if got := announced.writeCount(); got != 0 {
		t.Fatalf("expected no GRAFT to a meshBlocked peer, got %d", got)
	}

	// The long block lapses; the announce path may offer again.
	node.mu.Lock()
	peer.meshBlocked = time.Now().Add(-time.Second)
	node.mu.Unlock()
	node.announceLocalSubscriptionsToPeer(peer)
	if got := announced.writeCount(); got != 1 {
		t.Fatalf("expected one GRAFT after the block lapsed, got %d", got)
	}
}

// A PRUNE answering an announce-graft is join choreography, not war: because
// the announce path now records graftedAt, meshGraftedWithin sees it and the
// receiver gets the SHORT block, so the peer's own GRAFT microseconds later
// is not refused for the full 30s backoff.
func TestPruneAnsweringAnnounceGraftGetsShortBlock(t *testing.T) {
	node, err := NewNode("mesh-announce-graft-prune", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	peer := &peerConn{id: "peer-joining", session: newRecordedSession(t).session}
	node.mu.Lock()
	node.peers["peer-joining"] = peer
	node.mu.Unlock()

	// The announce path records the marker...
	node.announceLocalSubscription("alpha")
	node.mu.RLock()
	_, grafted := peer.graftedAt["alpha"]
	node.mu.RUnlock()
	if !grafted {
		t.Fatal("announce-graft marker missing; cannot test the short-block path")
	}

	// ...so the PRUNE answering it is recognized within the retry window.
	if !node.meshGraftedWithin("peer-joining", "alpha", meshGraftRetryInterval, time.Now()) {
		t.Fatal("a PRUNE answering an announce-graft was not recognized as join choreography")
	}

	// And the matching sender-side retry is backdated, not parked.
	node.markMeshGraftRefused("peer-joining", "alpha", pruneAnswerShortTTL, time.Now())
	node.mu.RLock()
	retryAt := peer.graftedAt["alpha"]
	node.mu.RUnlock()
	if !time.Now().Add(meshGraftRetryInterval - pruneAnswerShortTTL).After(retryAt.Add(-time.Second)) {
		// The backdated marker must fall one retry window minus the short TTL
		// in the past, i.e. well before "now": backdated = now + short - long.
		t.Fatalf("expected the graft marker backdated by the PRUNE, got %v", retryAt)
	}
}

// The malformed-channel gate: control traffic claiming a channel we would
// record (subscriptions, mesh membership, suppression) must be dropped before
// any state is written, so a peer cannot grow our maps with junk keys.
func TestHandleEnvelopeRejectsMalformedChannelOnControl(t *testing.T) {
	node, err := NewNode("mesh-malformed-channel", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	peer := &peerConn{id: "peer-junk"}
	huge := string(make([]byte, 1024))

	// A GRAFT with no channel must not be recorded as a subscription.
	node.handleEnvelope(peer, gossip.Envelope{Type: gossip.TypeGraft})
	if node.pubsub.HasPeerSubscription("peer-junk", "") {
		t.Fatal("an empty-channel GRAFT was recorded as a subscription")
	}
	// An oversized channel likewise.
	node.handleEnvelope(peer, gossip.Envelope{Type: gossip.TypeGraft, Channel: huge})
	if node.pubsub.HasPeerSubscription("peer-junk", huge) {
		t.Fatal("an oversized-channel GRAFT was recorded as a subscription")
	}
	// A PRUNE with no channel must not touch mesh membership.
	node.pubsub.SetMeshPeer("alpha", "peer-junk", true)
	node.handleEnvelope(peer, gossip.Envelope{Type: gossip.TypePrune})
	if !node.pubsub.InMesh("alpha", "peer-junk") {
		t.Fatal("an empty-channel PRUNE removed a peer from the mesh")
	}
}

// An inbound envelope with a nil peer (a future dispatch path, a defensive
// requirement) must be dropped by the handlers that would otherwise
// dereference it — not panic. One call per envelope type; the contract is
// "no panic", not "handled".
func TestHandleEnvelopeSurvivesNilPeer(t *testing.T) {
	node, err := NewNode("mesh-nil-peer", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	for _, env := range []gossip.Envelope{
		{Type: gossip.TypeGraft, Channel: "alpha"},
		{Type: gossip.TypePrune, Channel: "alpha"},
		{Type: gossip.TypeIDontWant, MessageIDs: []string{"msg-1"}},
		{Type: gossip.TypePublish, Channel: "alpha", MessageID: "msg-1", Payload: []byte("x")},
		{Type: gossip.TypeIHave, Channel: "alpha", MessageIDs: []string{"msg-1"}},
		{Type: gossip.TypeIWant, MessageIDs: []string{"msg-1"}},
		{Type: gossip.TypePing, RequestID: "req"},
	} {
		node.handleEnvelope(nil, env) // must not panic
	}
}
