package mesh

import (
	"encoding/json"
	"testing"
	"time"

	mcrypto "github.com/redstone-md/moss/internal/crypto"
	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/nat"
)

// TestHandleRelayDataDispatchDropsInsteadOfBlocking: a relayed DM that opens
// at the target must never park the transport read loop on a full dispatch
// channel — the send is select/default with a drop counter, the same contract
// as handleDirectPacket. Before the fix this send blocked, so an application
// that stopped draining froze the whole session's reader.
func TestHandleRelayDataDispatchDropsInsteadOfBlocking(t *testing.T) {
	origin, err := NewNode("mesh-relay-dispatch-origin", nil, isolatedTestConfig("relay-dispatch"))
	if err != nil {
		t.Fatalf("NewNode origin: %v", err)
	}
	target, err := NewNode("mesh-relay-dispatch-target", nil, isolatedTestConfig("relay-dispatch"))
	if err != nil {
		t.Fatalf("NewNode target: %v", err)
	}
	targetID := target.localPeerID()
	knownPeerStatic(t, origin, targetID, target.identity.NoiseStaticPublic())
	knownPeerStatic(t, target, origin.localPeerID(), origin.identity.NoiseStaticPublic())

	// The target holds an established relay session whose via peer handed it
	// this DM, and a dispatch channel one item deep, already full: the very
	// first delivered relayed DM hits the default branch.
	target.mu.Lock()
	target.relayLocals = map[string]relayLocalSession{
		"session-1": {sessionID: "session-1", viaPeerID: "via-peer-id", remotePeerID: origin.localPeerID(), established: true},
	}
	target.dispatchCh = make(chan any, 1)
	target.dispatchCh <- dispatchRelay{}
	target.mu.Unlock()

	sealed, err := origin.sealDMPayload(targetID, []byte("relay dm"))
	if err != nil {
		t.Fatalf("sealDMPayload: %v", err)
	}
	env := gossip.Envelope{
		Type:         gossip.TypeRelayData,
		RelaySession: "session-1",
		RelaySource:  origin.localPeerID(),
		RelayTarget:  targetID,
		Payload:      sealed,
	}

	done := make(chan struct{})
	go func() {
		target.handleRelayData(&peerConn{id: "via-peer-id"}, env)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handleRelayData blocked on a full dispatch channel")
	}

	if got := inboundCount(target, "__relay_dispatch_dropped__"); got != 1 {
		t.Fatalf("dropped relayed DM must be counted exactly once, got %d", got)
	}
	select {
	case v := <-target.dispatchCh:
		if _, ok := v.(dispatchRelay); !ok {
			t.Fatalf("dispatch channel must still hold only the pre-filled stub, got %T", v)
		}
	default:
		t.Fatal("the pre-filled stub must still be in the dispatch channel")
	}
	select {
	case <-target.dispatchCh:
		t.Fatal("the dropped relayed DM must not have entered the dispatch channel")
	default:
	}
}

// TestPruneStaleRelayRoutesCountsExpiryAndTombstones: reaping an idle route
// must count it as expired and leave a tombstone behind — the tombstone is
// what turns the next RelayData from a silent blackhole into a close reply.
func TestPruneStaleRelayRoutesCountsExpiryAndTombstones(t *testing.T) {
	const sessionTTL = 600 * time.Millisecond
	const settle = 800 * time.Millisecond // single dead route: idle age 800ms > TTL 600ms, 200ms margin
	node := &Node{
		relayRoutes:   map[string]relayRoute{"dead": {initiator: "c", target: "d"}},
		relaySessions: nat.NewSessionManager(100, sessionTTL),
	}
	// A non-ready NAT profile makes refreshSupernodeStatus a no-op (it reads
	// the profile and returns early when the ready state is unchanged).
	node.natProfile.Store(nat.Profile{Type: nat.TypeSymmetric})

	node.relaySessions.Acquire("dead")
	time.Sleep(settle)

	node.pruneStaleRelayRoutes()

	if got := inboundCount(node, "__relay_route_expired__"); got != 1 {
		t.Fatalf("each reaped route must be counted exactly once, got %d", got)
	}
	node.mu.RLock()
	_, reaped := node.relayRoutes["dead"]
	ts, tombstoned := node.relayRouteExpiry["dead"]
	node.mu.RUnlock()
	if reaped {
		t.Fatal("route with an expired session must be reaped")
	}
	if !tombstoned {
		t.Fatal("reaped route must leave an expiry tombstone for the close reply")
	}
	if ts.IsZero() {
		t.Fatal("tombstone must record the reap time")
	}
}

// TestHandleRelayDataRepliesCloseForExpiredRoute: the first RelayData after
// TTL GC reaped the route is answered with a RelayClose on the wire — one
// per expired session (the tombstone is consumed), none for a session that
// was never routed.
func TestHandleRelayDataRepliesCloseForExpiredRoute(t *testing.T) {
	identity, err := mcrypto.NewIdentity()
	if err != nil {
		t.Fatalf("new identity: %v", err)
	}
	const sourcePeerID = "source-peer-id"

	carrier := newCapturingCarrier()
	t.Cleanup(func() { _ = carrier.Close() })
	sess := mustCipherSession(carrier)
	t.Cleanup(func() { _ = sess.Close() })

	peer := &peerConn{id: sourcePeerID, session: sess}
	node := &Node{
		identity:      identity,
		config:        DefaultConfig(),
		peers:         map[string]*peerConn{sourcePeerID: peer},
		relayRoutes:   map[string]relayRoute{},
		relayBuckets:  map[string]*nat.TokenBucket{},
		relaySessions: nat.NewSessionManager(1, time.Minute),
		dispatchCh:    make(chan any, 1),
	}
	node.mu.Lock()
	node.relayRouteExpiry = map[string]time.Time{"session-1": time.Now()}
	node.mu.Unlock()

	node.handleRelayData(peer, relayDataEnvelope([]byte("first after expiry")))

	// Unwrap the wire: the reply must be a RelayClose mirroring the Data.
	farEnd := newCapturingCarrier()
	t.Cleanup(func() { _ = farEnd.Close() })
	farSess := cipherMatchedSession(t, farEnd)
	farEnd.reads <- carrier.lastWrite()
	raw, err := farSess.ReadPacket()
	if err != nil {
		t.Fatalf("far-end read failed: %v", err)
	}
	var closeEnv gossip.Envelope
	if err := json.Unmarshal(raw, &closeEnv); err != nil {
		t.Fatalf("wire plaintext is not an envelope: %v", err)
	}
	if closeEnv.Type != gossip.TypeRelayClose {
		t.Fatalf("expected %s for an expired route, got %s", gossip.TypeRelayClose, closeEnv.Type)
	}
	if closeEnv.RelaySession != "session-1" || closeEnv.RelaySource != sourcePeerID || closeEnv.RelayTarget != "target-peer-id" {
		t.Fatalf("close must mirror the answered Data, got session=%q source=%q target=%q",
			closeEnv.RelaySession, closeEnv.RelaySource, closeEnv.RelayTarget)
	}

	// One-shot: the tombstone is consumed, so a second Data for the same
	// expired session — and a Data for a session that never had a route —
	// produce no further wire writes.
	node.handleRelayData(peer, relayDataEnvelope([]byte("second")))
	unrouted := relayDataEnvelope([]byte("never routed"))
	unrouted.RelaySession = "never-routed"
	node.handleRelayData(peer, unrouted)

	node.mu.RLock()
	tombstones := len(node.relayRouteExpiry)
	node.mu.RUnlock()
	if tombstones != 0 {
		t.Fatalf("the close reply must consume the tombstone one-shot, %d left", tombstones)
	}
	carrier.mu.Lock()
	writes := len(carrier.capture)
	carrier.mu.Unlock()
	if writes != 1 {
		t.Fatalf("exactly one close reply must reach the wire, got %d writes", writes)
	}
}
