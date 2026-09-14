package mesh

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/transport"
)

// directCaptureCount snapshots how many ciphertexts a capturingCarrier has
// recorded. capturingCarrier lives in readpeer_carrier_test.go; this only
// reads its capture slice under its own mutex.
func directCaptureCount(c *capturingCarrier) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.capture)
}

// TestSendToPeerDirectConnectedSendsWithoutRelay pins the direct half of the
// transport choice: a target with a live session receives ONE TypeDirect
// envelope over that session, and the relay path is never consulted — no
// relay session exists afterwards, and the wire bytes decrypt (through a
// cipher-matched far session) to exactly the envelope SendToPeer built.
//
// The node is deliberately UNSTARTED: sendOrEnqueue then takes the
// synchronous path (no queue worker), so the write is observable the moment
// SendToPeer returns — no race with a maintenance ping for the carrier.
func TestSendToPeerDirectConnectedSendsWithoutRelay(t *testing.T) {
	node, err := NewNode("mesh-sendto-direct", nil, isolatedTestConfig("sendto-direct"))
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	// The peer's session over a capturing carrier: the node's writes land
	// in carrier.capture as ciphertext, sealed with the standard test
	// cipher states a cipher-matched far session can open.
	carrier := newCapturingCarrier()
	t.Cleanup(func() { _ = carrier.Close() })
	sess := mustCipherSession(carrier)
	t.Cleanup(func() { _ = sess.Close() })
	node.mu.Lock()
	node.peers["peer-direct"] = &peerConn{id: "peer-direct", session: sess, outbound: true, connectedAt: time.Now()}
	node.mu.Unlock()

	payload := []byte("direct-payload")
	if err := node.SendToPeer("peer-direct", payload, time.Second); err != nil {
		t.Fatalf("SendToPeer failed: %v", err)
	}

	if got := directCaptureCount(carrier); got != 1 {
		t.Fatalf("expected exactly one carrier write, got %d", got)
	}

	// Open the written ciphertext through a cipher-matched far session and
	// read the plaintext back: it must be the TypeDirect envelope.
	farEnd := newCapturingCarrier()
	farSess := cipherMatchedSession(t, farEnd)
	farEnd.reads <- carrier.lastWrite()
	plain, err := farSess.ReadPacket()
	if err != nil {
		t.Fatalf("far-end read failed: %v", err)
	}
	var env gossip.Envelope
	if err := json.Unmarshal(plain, &env); err != nil {
		t.Fatalf("far-end plaintext is not an envelope: %v", err)
	}
	if env.Type != gossip.TypeDirect {
		t.Fatalf("expected a %s envelope on the wire, got %s", gossip.TypeDirect, env.Type)
	}
	if string(env.Payload) != string(payload) {
		t.Fatalf("payload mismatch: %q != %q", env.Payload, payload)
	}
	wantSender := node.identity.PublicKeyBytes()
	if string(env.SenderID) != string(wantSender) {
		t.Fatalf("sender mismatch: %x != %x", env.SenderID, wantSender)
	}

	// The relay path must be untouched: no session was opened for a
	// direct-connected target.
	node.mu.RLock()
	relayCount := len(node.relayLocals)
	node.mu.RUnlock()
	if relayCount != 0 {
		t.Fatalf("direct-connected send opened %d relay session(s)", relayCount)
	}
}

// TestSendToPeerRelayedFallbackForDisconnectedPeer pins the fallback half:
// a target with NO peer entry delegates to the relay path. With no
// relay-capable candidate either, the error comes from the relay selector —
// proof the send attempted RelaySendTo rather than failing on the direct
// path.
func TestSendToPeerRelayedFallbackForDisconnectedPeer(t *testing.T) {
	node, err := NewNode("mesh-sendto-relayed", nil, isolatedTestConfig("sendto-relayed"))
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	err = node.SendToPeer("peer-absent", []byte("x"), 100*time.Millisecond)
	if err == nil {
		t.Fatal("expected an error for a target with neither a session nor a relay candidate")
	}
	if !strings.Contains(err.Error(), "no relay-capable peer is connected") {
		t.Fatalf("expected the relay-path error, got %q", err.Error())
	}
}

// TestSendToPeerEnforcesSizeGate pins the send-side size cap: a payload over
// Security.MaxMessageSizeBytes is rejected before any transport is touched —
// no carrier write, no dispatch, no relay attempt.
func TestSendToPeerEnforcesSizeGate(t *testing.T) {
	cfg := isolatedTestConfig("sendto-sizegate")
	cfg.Security.MaxMessageSizeBytes = 16
	node, err := NewNode("mesh-sendto-sizegate", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	carrier := newCapturingCarrier()
	t.Cleanup(func() { _ = carrier.Close() })
	sess := mustCipherSession(carrier)
	t.Cleanup(func() { _ = sess.Close() })
	node.mu.Lock()
	node.peers["peer-gate"] = &peerConn{id: "peer-gate", session: sess, connectedAt: time.Now()}
	node.mu.Unlock()

	err = node.SendToPeer("peer-gate", make([]byte, 17), time.Second)
	if err == nil {
		t.Fatal("expected the size gate to reject an oversized payload")
	}
	if !strings.Contains(err.Error(), "exceeds the maximum message size") {
		t.Fatalf("expected the size-gate error, got %q", err.Error())
	}
	if got := directCaptureCount(carrier); got != 0 {
		t.Fatalf("oversized payload wrote %d packet(s) to the wire", got)
	}
	if got := len(node.dispatchCh); got != 0 {
		t.Fatalf("oversized payload enqueued %d dispatch item(s)", got)
	}
}

// TestHandleDirectPacketDeliversToPacketCallbackAndGatesSize pins the
// receive half: a TypeDirect envelope from a peer reaches the registered
// packet callback with sender and payload intact; an oversized one is
// dropped and penalized, never delivered; a nil peer is survived.
func TestHandleDirectPacketDeliversToPacketCallbackAndGatesSize(t *testing.T) {
	node, err := NewNode("mesh-direct-recv", nil, isolatedTestConfig("direct-recv"))
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	if !transport.RunningGoTest() {
		t.Fatal("test build flag not set: the node would dial real discovery")
	}
	if code := node.Start(); code != MOSS_OK {
		t.Fatalf("Start: %d", code)
	}
	t.Cleanup(func() { node.Stop() })

	type packet struct {
		sender [32]byte
		data   []byte
	}
	received := make(chan packet, 4)
	node.SetPacketCallback(func(senderID [32]byte, data []byte) {
		received <- packet{sender: senderID, data: append([]byte(nil), data...)}
	})

	peer := &peerConn{id: "peer-src"}
	wantSender := node.PublicKey()

	// Nil peer must be survived, not panicked on.
	node.handleDirectPacket(nil, gossip.Envelope{Type: gossip.TypeDirect, Payload: []byte("x")})

	node.handleDirectPacket(peer, gossip.Envelope{
		Type:     gossip.TypeDirect,
		SenderID: node.identity.PublicKeyBytes(),
		Payload:  []byte("hello"),
	})
	select {
	case got := <-received:
		if got.sender != wantSender {
			t.Fatalf("sender mismatch: %v != %v", got.sender, wantSender)
		}
		if string(got.data) != "hello" {
			t.Fatalf("payload mismatch: %q", got.data)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("direct packet was never delivered to the packet callback")
	}

	// Oversized: dropped with a penalty, never delivered.
	over := node.config.Security.MaxMessageSizeBytes + 1
	scoreBefore := node.scoring.Score(peer.id)
	node.handleDirectPacket(peer, gossip.Envelope{
		Type:     gossip.TypeDirect,
		SenderID: node.identity.PublicKeyBytes(),
		Payload:  make([]byte, over),
	})
	select {
	case <-received:
		t.Fatal("oversized direct packet was delivered")
	case <-time.After(200 * time.Millisecond):
	}
	if after := node.scoring.Score(peer.id); after >= scoreBefore || after >= 0 {
		t.Fatalf("oversized direct packet was not penalized: score %v -> %v", scoreBefore, after)
	}
}

// TestPeerRTTUpdatesAfterPong pins the RTT getter against the real pong
// handler: zero before any probe, the ping→pong interval after a matching
// pong, still zero for a stale pong (wrong request id) and for an unknown
// peer.
func TestPeerRTTUpdatesAfterPong(t *testing.T) {
	node := &Node{
		peers: map[string]*peerConn{"peer-rtt": {id: "peer-rtt"}},
	}

	if rtt := node.PeerRTT("peer-rtt"); rtt != 0 {
		t.Fatalf("expected zero RTT before any probe, got %v", rtt)
	}

	// Arm the ping exactly as collectPingTargetsLocked does.
	node.mu.Lock()
	node.peers["peer-rtt"].pingPending = "probe-1"
	node.peers["peer-rtt"].pingSentAt = time.Now()
	node.mu.Unlock()

	// A stale pong (wrong request id) must be ignored.
	node.handlePong(node.peers["peer-rtt"], gossip.Envelope{Type: gossip.TypePong, RequestID: "probe-stale"})
	if rtt := node.PeerRTT("peer-rtt"); rtt != 0 {
		t.Fatalf("stale pong updated the RTT to %v", rtt)
	}

	time.Sleep(30 * time.Millisecond)
	node.handlePong(node.peers["peer-rtt"], gossip.Envelope{Type: gossip.TypePong, RequestID: "probe-1"})

	rtt := node.PeerRTT("peer-rtt")
	if rtt < 25*time.Millisecond {
		t.Fatalf("expected RTT ≈ the ping→pong interval, got %v", rtt)
	}
	if rtt := node.PeerRTT("peer-unknown"); rtt != 0 {
		t.Fatalf("expected zero RTT for an unknown peer, got %v", rtt)
	}
}

// TestDispatchRelayPrefersPacketCallback pins the unified sink wiring in the
// dispatch loop: a raw relayed payload reaches the packet callback when one
// is registered — and the legacy relay callback when it is not — never both.
func TestDispatchRelayPrefersPacketCallback(t *testing.T) {
	node, err := NewNode("mesh-dispatch-packet", nil, isolatedTestConfig("dispatch-packet"))
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	if !transport.RunningGoTest() {
		t.Fatal("test build flag not set: the node would dial real discovery")
	}
	if code := node.Start(); code != MOSS_OK {
		t.Fatalf("Start: %d", code)
	}
	t.Cleanup(func() { node.Stop() })

	relayReceived := make(chan []byte, 2)
	packetReceived := make(chan []byte, 2)
	node.SetRelayCallback(func(senderID [32]byte, data []byte) {
		relayReceived <- append([]byte(nil), data...)
	})
	node.SetPacketCallback(func(senderID [32]byte, data []byte) {
		packetReceived <- append([]byte(nil), data...)
	})

	// Both callbacks are registered: only the packet one may fire.
	node.dispatchCh <- dispatchRelay{sender: [32]byte{1}, data: []byte("via-relay")}
	select {
	case got := <-packetReceived:
		if string(got) != "via-relay" {
			t.Fatalf("packet callback payload mismatch: %q", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("relayed payload never reached the packet callback")
	}
	select {
	case <-relayReceived:
		t.Fatal("relayed payload fired BOTH callbacks — the unified sink must take precedence")
	case <-time.After(200 * time.Millisecond):
	}

	// Only the legacy callback registered: it fires as before.
	node.SetPacketCallback(nil)
	node.dispatchCh <- dispatchRelay{sender: [32]byte{1}, data: []byte("legacy")}
	select {
	case got := <-relayReceived:
		if string(got) != "legacy" {
			t.Fatalf("relay callback payload mismatch: %q", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("relayed payload never reached the legacy relay callback")
	}
}
