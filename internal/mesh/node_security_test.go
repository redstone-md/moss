package mesh

import (
	"testing"

	mcrypto "moss/internal/crypto"
	"moss/internal/gossip"
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
