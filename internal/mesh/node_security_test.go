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
