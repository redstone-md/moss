package mesh

import (
	"testing"
	"time"

	mcrypto "moss/internal/crypto"
	"moss/internal/gossip"
	"moss/internal/nat"
)

func TestHandleRelayRequestRejectsSpoofedLocalRelaySource(t *testing.T) {
	identity, err := mcrypto.NewIdentity()
	if err != nil {
		t.Fatalf("new identity: %v", err)
	}
	node := &Node{
		identity:    identity,
		relayLocals: map[string]relayLocalSession{},
	}
	peer := &peerConn{id: "real-peer-id"}
	env := gossip.Envelope{
		RelaySession: "session-1",
		RelaySource:  "spoofed-peer-id",
		RelayTarget:  node.localPeerID(),
	}

	node.handleRelayRequest(peer, env)

	node.mu.RLock()
	_, ok := node.relayLocals[env.RelaySession]
	node.mu.RUnlock()
	if ok {
		t.Fatalf("expected spoofed relay source to be rejected")
	}
}

func TestVerifyRelayRequestAllowsForwardedSender(t *testing.T) {
	sourceIdentity, err := mcrypto.NewIdentity()
	if err != nil {
		t.Fatalf("new source identity: %v", err)
	}
	targetIdentity, err := mcrypto.NewIdentity()
	if err != nil {
		t.Fatalf("new target identity: %v", err)
	}
	source := &Node{identity: sourceIdentity}
	target := &Node{
		identity:    targetIdentity,
		relayLocals: map[string]relayLocalSession{},
	}
	env := source.signRelayRequestEnvelope(gossip.Envelope{
		Type:         gossip.TypeRelayRequest,
		RelaySession: "session-1",
		RelaySource:  source.localPeerID(),
		RelayTarget:  target.localPeerID(),
	})

	if !verifyRelayRequestEnvelope(env) {
		t.Fatalf("expected signed forwarded relay request to verify")
	}
	target.handleRelayRequest(&peerConn{id: "relay-peer-id"}, env)
	target.mu.RLock()
	session, ok := target.relayLocals[env.RelaySession]
	target.mu.RUnlock()
	if !ok || session.viaPeerID != "relay-peer-id" || session.remotePeerID != source.localPeerID() {
		t.Fatalf("expected forwarded relay request to establish local session")
	}

	env.RelaySource = "spoofed-peer-id"
	if verifyRelayRequestEnvelope(env) {
		t.Fatalf("expected tampered relay source to be rejected")
	}
}

func TestHandleRelayDataRejectsUnroutedPayloadBeforeOverload(t *testing.T) {
	identity, err := mcrypto.NewIdentity()
	if err != nil {
		t.Fatalf("new identity: %v", err)
	}
	const targetPeerID = "target-peer-id"
	node := &Node{
		identity:      identity,
		config:        DefaultConfig(),
		peers:         map[string]*peerConn{targetPeerID: {id: targetPeerID}},
		relayRoutes:   map[string]relayRoute{},
		relayBuckets:  map[string]*nat.TokenBucket{},
		relaySessions: nat.NewSessionManager(1, time.Minute),
		dispatchCh:    make(chan any, 1),
	}
	peer := &peerConn{id: "attacker-peer-id"}
	env := gossip.Envelope{
		Type:         gossip.TypeRelayData,
		RelaySession: "forged-session",
		RelaySource:  peer.id,
		RelayTarget:  targetPeerID,
		Payload:      make([]byte, 2048),
	}

	node.handleRelayData(peer, env)

	node.mu.RLock()
	overloadedUntil := node.overloadedUntil
	bucketCount := len(node.relayBuckets)
	routeCount := len(node.relayRoutes)
	node.mu.RUnlock()
	if !overloadedUntil.IsZero() {
		t.Fatalf("expected forged relay data to be rejected before overload, got %v", overloadedUntil)
	}
	if bucketCount != 0 {
		t.Fatalf("expected forged relay data to avoid relay bucket accounting, got %d buckets", bucketCount)
	}
	if routeCount != 0 || node.relaySessions.Count() != 0 {
		t.Fatalf("expected no relay route/session state, got routes=%d sessions=%d", routeCount, node.relaySessions.Count())
	}
}
