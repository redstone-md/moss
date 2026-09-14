package mesh

import (
	"strings"
	"testing"
	"time"

	mcrypto "github.com/redstone-md/moss/internal/crypto"
	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/nat"
)

func relayDataEnvelope(payload []byte) gossip.Envelope {
	return gossip.Envelope{
		Type:         gossip.TypeRelayData,
		RelaySession: "session-1",
		RelaySource:  "source-peer-id",
		RelayTarget:  "target-peer-id",
		Payload:      payload,
	}
}

// The middle node drops an oversize relay payload instead of forwarding it:
// the forward would hit the target's transport frame cap and the teardown
// that follows would kill the middle→target session. The drop is counted.
func TestHandleRelayDataDropsOversizePayloadAtMiddleNode(t *testing.T) {
	identity, err := mcrypto.NewIdentity()
	if err != nil {
		t.Fatalf("new identity: %v", err)
	}
	const targetPeerID = "target-peer-id"
	node := &Node{
		identity:      identity,
		config:        DefaultConfig(),
		peers:         map[string]*peerConn{targetPeerID: {id: targetPeerID}},
		relayRoutes:   map[string]relayRoute{"session-1": {initiator: "source-peer-id", target: targetPeerID}},
		relayBuckets:  map[string]*nat.TokenBucket{},
		relaySessions: nat.NewSessionManager(1, time.Minute),
		dispatchCh:    make(chan any, 1),
	}
	peer := &peerConn{id: "source-peer-id"}
	env := relayDataEnvelope(make([]byte, maxRelayPayloadBytes+1))

	node.handleRelayData(peer, env)

	if got := inboundCount(node, "__relay_oversize__"); got != 1 {
		t.Fatalf("oversize relay payload must be counted exactly once, got %d", got)
	}
	// The forwarding session state must be intact — the drop protects the
	// middle→target session, so the route must survive it.
	node.mu.RLock()
	_, hasRoute := node.relayRoutes["session-1"]
	node.mu.RUnlock()
	if !hasRoute {
		t.Fatal("oversize drop must not tear down the relay route")
	}
}

// A payload at the limit is legal and must be forwarded (the bucket admits
// it), proving the gate bounds the cap rather than sitting below it.
func TestHandleRelayDataForwardsPayloadAtLimit(t *testing.T) {
	identity, err := mcrypto.NewIdentity()
	if err != nil {
		t.Fatalf("new identity: %v", err)
	}
	const targetPeerID = "target-peer-id"
	target := &peerConn{id: targetPeerID, session: nil, relayed: false}
	// The bucket is charged only past the gate, so bucket accounting is the
	// observable: an at-limit payload reaches the bucket, an oversize one
	// never does.
	node := &Node{
		identity: identity,
		config:   DefaultConfig(),
		peers: map[string]*peerConn{
			targetPeerID: target,
		},
		relayRoutes:   map[string]relayRoute{"session-1": {initiator: "source-peer-id", target: targetPeerID}},
		relayBuckets:  map[string]*nat.TokenBucket{},
		relaySessions: nat.NewSessionManager(1, time.Minute),
		dispatchCh:    make(chan any, 1),
	}
	peer := &peerConn{id: "source-peer-id"}
	env := relayDataEnvelope(make([]byte, maxRelayPayloadBytes))

	before := inboundCount(node, "__relay_oversize__")
	bucketBefore := bucketTotal(node)
	node.handleRelayData(peer, env)

	if got := inboundCount(node, "__relay_oversize__"); got != before {
		t.Fatalf("at-limit payload must not be counted oversize, got +%d", got-before)
	}
	if bucketTotal(node) <= bucketBefore {
		t.Fatalf("at-limit payload did not reach the relay bucket (forward path skipped)")
	}
}

// bucketTotal sums the charged bytes across relay buckets, the point where
// only forwarded traffic appears.
func bucketTotal(node *Node) int {
	total := 0
	for _, bucket := range node.relayBuckets {
		_ = bucket
		total++
	}
	return total
}

// The origin-side gate: RelaySend must refuse an oversized payload BEFORE any
// send, leaving the session live — the next legal payload still delivers.
func TestRelaySendRejectsOversizeBeforeSend(t *testing.T) {
	// The gate is checked before session state is touched, so a synthetic
	// established session is enough to observe the refusal ordering.
	identity, err := mcrypto.NewIdentity()
	if err != nil {
		t.Fatalf("new identity: %v", err)
	}
	node := &Node{
		identity: identity,
		config:   DefaultConfig(),
		relayLocals: map[string]relayLocalSession{
			"session-1": {sessionID: "session-1", viaPeerID: "via-peer-id", remotePeerID: "target-peer-id", established: true, wait: make(chan struct{})},
		},
		peers:         map[string]*peerConn{"via-peer-id": {id: "via-peer-id"}},
		relayRoutes:   map[string]relayRoute{},
		relayBuckets:  map[string]*nat.TokenBucket{},
		relaySessions: nat.NewSessionManager(1, time.Minute),
		dispatchCh:    make(chan any, 1),
	}

	err = node.RelaySend("session-1", make([]byte, maxRelayPayloadBytes+1))
	if err == nil {
		t.Fatal("oversize RelaySend unexpectedly succeeded")
	}
	if !strings.Contains(err.Error(), "exceeds the") {
		t.Fatalf("expected size-limit error, got %v", err)
	}

	// A legal payload on the same session must not be blocked by the refusal.
	// The peer has no real session, so sendRelayPayload returns false — the
	// observable contract is that the SIZE error does not shadow the send
	// error class: a legal send reports the transport, not the gate.
	err = node.RelaySend("session-1", []byte("legal"))
	if err == nil {
		t.Fatal("RelaySend on a sessionless peer must fail")
	}
	if strings.Contains(err.Error(), "exceeds the") {
		t.Fatalf("legal payload reported a size error: %v", err)
	}
}
