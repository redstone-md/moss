package mesh

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
)

// sendToPeers marshals the envelope ONCE for the whole fan-out. This pins the
// contract behind that: every peer's queue receives a wire buffer that is the
// same backing array, not a fresh marshal per peer.
//
// Determinism comes from parking each worker FIRST: one warm-up send parks
// every worker inside blockingCarrier.WritePacket, so the measured fan-out
// afterwards provably queues (nothing drains it) and the buffered values are
// the exact ones sendToPeers produced. The queue read below is then a
// non-blocking handoff, not a race with the worker.
func TestSendToPeersMarshalsOnceForWholeFanout(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	node, err := NewNode("mesh-marshal-once", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	release := make(chan struct{})
	defer close(release)

	const fanout = 3
	peerIDs := make([]string, 0, fanout)
	node.mu.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node.rootCtx = ctx
	node.started = true
	for i := range fanout {
		id := "peer-" + strings.Repeat("a", i+1)
		session := newBlockingSession(t, release)
		node.peers[id] = &peerConn{id: id, session: session.session}
		peerIDs = append(peerIDs, id)
	}
	node.mu.Unlock()

	// Warm-up: one envelope per peer parks its worker in WritePacket.
	// These take the synchronous-by-queue path too, so the queues exist and
	// their workers are mid-send when the measured fan-out runs.
	warmup := gossip.Envelope{Type: gossip.TypePing, RequestID: "warmup"}
	for _, id := range peerIDs {
		if !node.sendOrEnqueue(node.peers[id], warmup) {
			t.Fatalf("warm-up enqueue for %s failed", id)
		}
	}
	// Wait until every worker has taken its warm-up envelope — each parked
	// worker holds exactly one taken item, so the queue it leaves behind is
	// empty and the next fan-out's entries are all still buffered.
	deadline := time.Now().Add(5 * time.Second)
	for _, id := range peerIDs {
		for {
			node.outboundMu.Lock()
			parked := len(node.outboundQueues[id]) == 0
			node.outboundMu.Unlock()
			if parked {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("worker for %s never drained its warm-up envelope", id)
			}
			time.Sleep(time.Millisecond)
		}
	}

	env := gossip.Envelope{Type: gossip.TypePublish, Channel: "alpha", MessageID: "msg-1", Payload: []byte("x")}
	if !node.sendToPeers(peerIDs, env) {
		t.Fatal("expected the fan-out to queue for every peer")
	}

	node.outboundMu.Lock()
	var first []byte
	for _, id := range peerIDs {
		queue := node.outboundQueues[id]
		if queue == nil {
			node.outboundMu.Unlock()
			t.Fatalf("no outbound queue for %s", id)
		}
		// Every worker is parked, so this receive drains the one entry the
		// fan-out buffered for this peer — the exact wire sendToPeers
		// produced.
		e, ok := <-queue
		if !ok {
			node.outboundMu.Unlock()
			t.Fatalf("queue for %s was closed", id)
		}
		if e.wire == nil {
			node.outboundMu.Unlock()
			t.Fatalf("queue entry for %s carries no pre-marshaled wire", id)
		}
		if first == nil {
			first = e.wire
			continue
		}
		if &e.wire[0] != &first[0] {
			node.outboundMu.Unlock()
			t.Fatalf("%s received a fresh marshal instead of the shared wire", id)
		}
	}
	node.outboundMu.Unlock()

	// The shared wire form must be exactly the envelope's JSON — the worker
	// hands it to WritePacket untouched, so a peer that reads it back parses
	// the same envelope.
	want, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal reference: %v", err)
	}
	if string(first) != string(want) {
		t.Fatalf("shared wire %s != fresh marshal %s", first, want)
	}
}

// A synchronous direct send through sendEnvelopeWire must reuse the caller's
// wire bytes rather than re-marshaling, and a nil wire must marshal in place.
// A recorded session's carrier would be needed to observe the bytes; the
// written-packet identity is checked via writeCount instead: one send is one
// packet either way.
func TestSendEnvelopeWireReusesWireAndNilMarshalsOnce(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	node, err := NewNode("mesh-send-wire", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	session := newRecordedSession(t)
	peer := &peerConn{id: "peer-recorded", session: session.session}

	env := gossip.Envelope{Type: gossip.TypeGraft, Channel: "alpha"}
	wire, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !node.sendEnvelopeWire(peer, env, wire) {
		t.Fatal("pre-marshaled wire send failed")
	}
	if session.writeCount() != 1 {
		t.Fatalf("expected 1 write for the pre-marshaled send, got %d", session.writeCount())
	}
	if !node.sendEnvelopeWire(peer, env, nil) {
		t.Fatal("nil-wire send failed")
	}
	if session.writeCount() != 2 {
		t.Fatalf("expected 2 writes after the nil-wire send, got %d", session.writeCount())
	}
	// Graylisted peers are refused regardless of the wire form.
	node.scoring.SetApplicationScore("peer-recorded", 2*gossip.GraylistThreshold)
	if node.sendEnvelopeWire(peer, env, wire) {
		t.Fatal("graylisted peer received a direct send")
	}
}

// The relay AEAD cache turns the per-envelope X25519 DH + HKDF + AEAD
// construction into a hit. A hit must return the SAME AEAD, and a rotated
// remote static must re-derive rather than keep sealing with a dead peer's key.
func TestRelayGossipAEADCacheHitsAndRotates(t *testing.T) {
	nodeA, err := NewNode("mesh-relay-aead-a", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode nodeA: %v", err)
	}
	nodeB, err := NewNode("mesh-relay-aead-b", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode nodeB: %v", err)
	}
	targetID := nodeB.localPeerID()
	nodeA.knownPeers[targetID] = knownPeer{id: targetID, noiseStatic: nodeB.identity.NoiseStaticPublic()}

	aead1, err := nodeA.relayGossipAEAD("session-1", nodeA.localPeerID(), targetID)
	if err != nil {
		t.Fatalf("first relayGossipAEAD: %v", err)
	}
	aead2, err := nodeA.relayGossipAEAD("session-1", nodeA.localPeerID(), targetID)
	if err != nil {
		t.Fatalf("second relayGossipAEAD: %v", err)
	}
	if aead1 != aead2 {
		t.Fatal("second call re-derived instead of hitting the cache")
	}
	// Same remote, different session: a different derivation.
	aead3, err := nodeA.relayGossipAEAD("session-2", nodeA.localPeerID(), targetID)
	if err != nil {
		t.Fatalf("other-session relayGossipAEAD: %v", err)
	}
	if aead1 == aead3 {
		t.Fatal("different session hit the same cache entry")
	}

	// Rotate the remote's static: the cached AEAD is stale and the next call
	// must re-derive under the new static.
	nodeC, err := NewNode("mesh-relay-aead-c", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode nodeC: %v", err)
	}
	nodeA.knownPeers[targetID] = knownPeer{id: targetID, noiseStatic: nodeC.identity.NoiseStaticPublic()}
	aead4, err := nodeA.relayGossipAEAD("session-1", nodeA.localPeerID(), targetID)
	if err != nil {
		t.Fatalf("post-rotation relayGossipAEAD: %v", err)
	}
	if aead1 == aead4 {
		t.Fatal("rotated remote static kept the stale AEAD")
	}

	// The re-derived AEAD is now the cached one: a follow-up call hits it.
	aead5, err := nodeA.relayGossipAEAD("session-1", nodeA.localPeerID(), targetID)
	if err != nil {
		t.Fatalf("post-rotation cache-hit relayGossipAEAD: %v", err)
	}
	if aead4 != aead5 {
		t.Fatal("the call after a rotation did not hit the re-derived entry")
	}
	if aead5 == aead1 {
		t.Fatal("the re-derived entry is the stale pre-rotation AEAD")
	}

	// The rotation must re-derive EVERY session's entry for that remote —
	// the static recheck is per-hit, so session-2's next call sees the new
	// static too and re-derives rather than keeping a key the OLD static
	// owner could still open.
	aead6, err := nodeA.relayGossipAEAD("session-2", nodeA.localPeerID(), targetID)
	if err != nil {
		t.Fatalf("session-2 post-rotation relayGossipAEAD: %v", err)
	}
	if aead6 == aead3 {
		t.Fatal("session-2 kept its pre-rotation entry after the remote's static rotated")
	}
	// And the re-derived session-2 entry is now cached.
	aead7, err := nodeA.relayGossipAEAD("session-2", nodeA.localPeerID(), targetID)
	if err != nil {
		t.Fatalf("session-2 post-rotation cache-hit relayGossipAEAD: %v", err)
	}
	if aead6 != aead7 {
		t.Fatal("the call after session-2's re-derivation did not hit the new entry")
	}
}

// The cached relay AEAD must still open what a peer sealed — the cache must
// not change the derived key material, only memoize its construction.
func TestRelayGossipAEADCacheRoundtrip(t *testing.T) {
	nodeA, err := NewNode("mesh-relay-cache-rt-a", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode nodeA: %v", err)
	}
	nodeB, err := NewNode("mesh-relay-cache-rt-b", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode nodeB: %v", err)
	}
	targetID := nodeB.localPeerID()
	nodeA.knownPeers[targetID] = knownPeer{id: targetID, noiseStatic: nodeB.identity.NoiseStaticPublic()}
	nodeB.knownPeers[nodeA.localPeerID()] = knownPeer{id: nodeA.localPeerID(), noiseStatic: nodeA.identity.NoiseStaticPublic()}

	// Seal several envelopes so a second call goes through the cache.
	for i := range 3 {
		sealed, err := nodeA.sealRelayGossipEnvelope("session-1", targetID, gossip.Envelope{Type: gossip.TypeGraft, Channel: "alpha"})
		if err != nil {
			t.Fatalf("seal %d: %v", i, err)
		}
		session := relayLocalSession{sessionID: "session-1", remotePeerID: nodeA.localPeerID()}
		opened, err := nodeB.openRelayGossipEnvelope(session, nodeA.localPeerID(), sealed)
		if err != nil {
			t.Fatalf("open %d failed through the cache: %v", i, err)
		}
		if opened.Type != gossip.TypeGraft || opened.Channel != "alpha" {
			t.Fatalf("envelope %d came back wrong: %#v", i, opened)
		}
	}
}

// The room AEAD cache keys by meshID and drops the entry when the room's key
// changes (re-join with a different PSK) or the room is left. A stale entry
// would seal with a dead key — silently unreadable for the room's members.
func TestRoomAEADCacheHitAndPSKRotationInvalidates(t *testing.T) {
	nodeA, err := NewNode("mesh-room-aead-a", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode nodeA: %v", err)
	}
	if code := nodeA.JoinRoom("room-x", []byte("psk-1")); code != MOSS_OK {
		t.Fatalf("JoinRoom psk-1: %d", code)
	}
	key1 := nodeA.roomKeyFor("room-x")
	aead1, err := nodeA.roomAEADFor("room-x", key1)
	if err != nil {
		t.Fatalf("roomAEADFor psk-1: %v", err)
	}
	aead2, err := nodeA.roomAEADFor("room-x", key1)
	if err != nil {
		t.Fatalf("second roomAEADFor psk-1: %v", err)
	}
	if aead1 != aead2 {
		t.Fatal("second call re-constructed the room AEAD instead of hitting the cache")
	}

	// Same room, new PSK: a rotation. The stored-key guard must refuse the
	// stale entry and the AEAD must change.
	if code := nodeA.JoinRoom("room-x", []byte("psk-2")); code != MOSS_OK {
		t.Fatalf("JoinRoom psk-2: %d", code)
	}
	key2 := nodeA.roomKeyFor("room-x")
	if string(key1) == string(key2) {
		t.Fatal("re-join with a different PSK derived the same key")
	}
	aead3, err := nodeA.roomAEADFor("room-x", key2)
	if err != nil {
		t.Fatalf("roomAEADFor psk-2: %v", err)
	}
	if aead1 == aead3 {
		t.Fatal("PSK rotation kept the stale AEAD")
	}

	// What the rotated node seals must open on a peer that joined with the
	// new PSK — the cache must not leak the old key into new seals.
	nodeB, err := NewNode("mesh-room-aead-b", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode nodeB: %v", err)
	}
	if code := nodeB.JoinRoom("room-x", []byte("psk-2")); code != MOSS_OK {
		t.Fatalf("nodeB JoinRoom psk-2: %d", code)
	}
	sealed, err := nodeA.sealRoomIn("room-x", []byte("post-rotation"))
	if err != nil {
		t.Fatalf("sealRoomIn post-rotation: %v", err)
	}
	plaintext, ok := nodeB.openRoom("room-x", sealed)
	if !ok || string(plaintext) != "post-rotation" {
		t.Fatal("a payload sealed after PSK rotation did not open on a fresh psk-2 node")
	}
}

// leaveRoom must drop the cached AEAD: rejoining later (same or different
// PSK) constructs from the CURRENT room key, never a dead room's entry.
func TestRoomAEADCacheLeaveInvalidates(t *testing.T) {
	node, err := NewNode("mesh-room-aead-leave", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	if code := node.JoinRoom("room-y", []byte("psk-1")); code != MOSS_OK {
		t.Fatalf("JoinRoom: %d", code)
	}
	key := node.roomKeyFor("room-y")
	aead1, err := node.roomAEADFor("room-y", key)
	if err != nil {
		t.Fatalf("roomAEADFor: %v", err)
	}
	node.roomAEADs.mu.Lock()
	held := len(node.roomAEADs.aeads)
	node.roomAEADs.mu.Unlock()
	if held != 1 {
		t.Fatalf("expected 1 cached room AEAD, got %d", held)
	}

	if code := node.LeaveRoom("room-y"); code != MOSS_OK {
		t.Fatalf("LeaveRoom: %d", code)
	}
	node.roomAEADs.mu.Lock()
	held = len(node.roomAEADs.aeads)
	node.roomAEADs.mu.Unlock()
	if held != 0 {
		t.Fatal("leaveRoom left the room's AEAD in the cache")
	}

	// Rejoin with the same PSK: same key material, but the entry is a
	// fresh construction (the old one was dropped), and it still roundtrips.
	if code := node.JoinRoom("room-y", []byte("psk-1")); code != MOSS_OK {
		t.Fatalf("re-JoinRoom: %d", code)
	}
	aead2, err := node.roomAEADFor("room-y", node.roomKeyFor("room-y"))
	if err != nil {
		t.Fatalf("post-rejoin roomAEADFor: %v", err)
	}
	if aead1 == aead2 {
		t.Fatal("rejoined room reused the pre-leave AEAD object")
	}
	sealed, err := node.sealRoomIn("room-y", []byte("hello"))
	if err != nil {
		t.Fatalf("sealRoomIn: %v", err)
	}
	plaintext, ok := node.openRoom("room-y", sealed)
	if !ok || string(plaintext) != "hello" {
		t.Fatal("sealed payload did not open after the leave/rejoin cycle")
	}
}
