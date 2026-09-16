package mesh

import (
	"context"
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
)

// Suppression is the IDontWant half of the serve path: a peer that told us
// not to send an id must not get it from an IWANT either. The batch check in
// handleIWant must keep the per-id skip — one suppressed id in the list
// silences exactly that id, not the request.
func TestHandleIWantSkipsSuppressedIDs(t *testing.T) {
	node, err := NewNode("mesh-iwant-suppressed", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	for _, id := range []string{"msg-muted", "msg-fresh"} {
		node.cache.Store(gossip.Envelope{
			Type:      gossip.TypePublish,
			Channel:   "alpha",
			MessageID: id,
			Payload:   []byte("payload"),
		})
	}
	node.mu.Lock()
	node.suppress["peer-mute"] = map[string]time.Time{"msg-muted": time.Now()}
	node.mu.Unlock()

	asker := newRecordedSession(t)
	peer := &peerConn{id: "peer-mute", session: asker.session}
	node.handleIWant(peer, gossip.Envelope{
		Type:       gossip.TypeIWant,
		MessageIDs: []string{"msg-muted", "msg-fresh"},
	})

	if got := asker.writeCount(); got != 1 {
		t.Fatalf("expected only the unsuppressed id to be served, got %d packets", got)
	}
	// The dedup phase records the serve marker before the suppression
	// check runs, so the muted id consumes its ask-cooldown too — the
	// marker is the ask-side dedup, not a claim that a packet left.
	node.mu.RLock()
	served := node.iwantServes["peer-mute"]
	_, mutedMarked := served["msg-muted"]
	_, freshMarked := served["msg-fresh"]
	node.mu.RUnlock()
	if !mutedMarked || !freshMarked {
		t.Fatalf("expected both ids to keep their serve markers, got muted=%v fresh=%v",
			mutedMarked, freshMarked)
	}
}

// A suppression claim older than the expiry TTL frees the id on read: the
// peer said "don't send" once, not forever. The reaping must survive the
// batching — an expired entry still leaves the map, so its slot in the
// per-peer suppression cap is reusable.
func TestSuppressionExpiresAfterTTL(t *testing.T) {
	node, err := NewNode("mesh-suppress-expiry", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	node.cache.Store(gossip.Envelope{
		Type:      gossip.TypePublish,
		Channel:   "alpha",
		MessageID: "msg-old",
		Payload:   []byte("payload"),
	})
	node.mu.Lock()
	node.suppress["peer-mute"] = map[string]time.Time{
		"msg-old": time.Now().Add(-suppressionExpiryTTL - time.Minute),
	}
	node.mu.Unlock()

	asker := newRecordedSession(t)
	peer := &peerConn{id: "peer-mute", session: asker.session}
	node.handleIWant(peer, gossip.Envelope{
		Type:       gossip.TypeIWant,
		MessageIDs: []string{"msg-old"},
	})

	if got := asker.writeCount(); got != 1 {
		t.Fatalf("expected an expired suppression to serve the id, got %d packets", got)
	}
	node.mu.RLock()
	stillHeld := len(node.suppress["peer-mute"])
	node.mu.RUnlock()
	if stillHeld != 0 {
		t.Fatalf("expected the expired suppression entry to be reaped on read, %d entries remain", stillHeld)
	}
}

// A serve that cannot enqueue defers the whole remainder and rolls back the
// serve markers for exactly the ids it did not serve: the asker's next IWANT
// re-serves them from scratch instead of sitting out a cooldown for
// envelopes that never left. Ids already served keep their markers.
func TestHandleIWantRollsBackMarkersOnDeferredServe(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	node, err := NewNode("mesh-iwant-deferred", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}

	release := make(chan struct{})
	stalled := newBlockingSession(t, release)
	stalledPeer := &peerConn{id: "peer-stalled", session: stalled.session}
	node.mu.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	node.rootCtx = ctx
	node.started = true
	node.peers["peer-stalled"] = stalledPeer
	node.mu.Unlock()
	defer func() {
		cancel()
		close(release)
	}()
	filler := gossip.Envelope{Type: gossip.TypePublish, Channel: "alpha", MessageID: "fill", Payload: []byte("x")}
	node.sendOrEnqueue(stalledPeer, filler)
	deadline := time.Now().Add(5 * time.Second)
	for {
		node.outboundMu.Lock()
		buffered := len(node.outboundQueues["peer-stalled"])
		node.outboundMu.Unlock()
		if buffered == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("worker never picked up the first filler, %d still buffered", buffered)
		}
		time.Sleep(time.Millisecond)
	}
	// The buffer now holds exactly outboundQueueDepth-1 fillers with the
	// worker parked on the first: one free slot for the serve below to
	// spend on msg-first, then the refusal of msg-second rolls back only
	// the refused tail.
	for range outboundQueueDepth - 1 {
		node.sendOrEnqueue(stalledPeer, filler)
	}

	for _, id := range []string{"msg-first", "msg-second"} {
		node.cache.Store(gossip.Envelope{
			Type:      gossip.TypePublish,
			Channel:   "alpha",
			MessageID: id,
			Payload:   []byte("payload"),
		})
	}

	node.handleIWant(stalledPeer, gossip.Envelope{
		Type:       gossip.TypeIWant,
		MessageIDs: []string{"msg-first", "msg-second"},
	})

	if got := counterValue(node, "__iwant_deferred__"); got != 1 {
		t.Fatalf("expected one deferred serve, got %d", got)
	}
	node.mu.RLock()
	served := node.iwantServes["peer-stalled"]
	_, firstMarked := served["msg-first"]
	_, secondMarked := served["msg-second"]
	node.mu.RUnlock()
	if !firstMarked {
		t.Fatal("expected the served id to keep its marker")
	}
	if secondMarked {
		t.Fatal("expected the refused id's marker to be rolled back")
	}

	// The heal path: the queue is still full, so the re-ask defers again —
	// but the rolled-back id is re-examined, not throttled by a marker for
	// an envelope that never left. Its serve marker is gone after the
	// second rollback too, so the next ask on a drained queue starts
	// clean rather than sitting out the cooldown.
	node.handleIWant(stalledPeer, gossip.Envelope{
		Type:       gossip.TypeIWant,
		MessageIDs: []string{"msg-first", "msg-second"},
	})
	if got := counterValue(node, "__iwant_deferred__"); got != 2 {
		t.Fatalf("expected the re-ask to defer again, got %d", got)
	}
	node.mu.RLock()
	served = node.iwantServes["peer-stalled"]
	_, secondMarked = served["msg-second"]
	node.mu.RUnlock()
	if secondMarked {
		t.Fatal("expected the re-deferred id's marker to be rolled back again")
	}
}
