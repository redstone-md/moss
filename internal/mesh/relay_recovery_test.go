package mesh

import (
	"testing"
	"time"

	"moss/internal/gossip"
	"moss/internal/nat"
	"moss/internal/transport"
)

func TestRemovePeerClearsRelaySessionsUsingDisconnectedViaPeer(t *testing.T) {
	relaySession := &transport.Session{}
	otherSession := &transport.Session{}

	node := &Node{
		peers: map[string]*peerConn{
			"relay-a": {id: "relay-a", session: relaySession},
			"relay-b": {id: "relay-b", session: otherSession},
		},
		suppress:     map[string]map[string]time.Time{},
		relayBuckets: map[string]*nat.TokenBucket{},
		directProbes: map[string]time.Time{
			"target-a": time.Now(),
			"target-b": time.Now(),
		},
		peerDials: map[string]time.Time{},
		knownPeers: map[string]knownPeer{
			"relay-a": {id: "relay-a", direct: true},
			"relay-b": {id: "relay-b", direct: true},
		},
		pubsub:      gossip.NewManager(),
		scoring:     gossip.NewEngine(),
		relayLocals: map[string]relayLocalSession{},
		dispatchCh:  make(chan any, 1),
	}

	node.relayLocals["stale-a"] = relayLocalSession{
		sessionID:    "stale-a",
		viaPeerID:    "relay-a",
		remotePeerID: "target-a",
		established:  true,
	}
	node.relayLocals["keep-b"] = relayLocalSession{
		sessionID:    "keep-b",
		viaPeerID:    "relay-b",
		remotePeerID: "target-b",
		established:  true,
	}

	node.removePeer("relay-a", relaySession)

	node.mu.RLock()
	defer node.mu.RUnlock()

	if _, ok := node.relayLocals["stale-a"]; ok {
		t.Fatal("expected relay session through disconnected peer to be removed")
	}
	if _, ok := node.relayLocals["keep-b"]; !ok {
		t.Fatal("expected relay session through remaining peer to be preserved")
	}
	if _, ok := node.directProbes["target-a"]; ok {
		t.Fatal("expected direct probe state for stale relay target to be removed")
	}
	if _, ok := node.directProbes["target-b"]; !ok {
		t.Fatal("expected unrelated direct probe state to be preserved")
	}
	if info := node.knownPeers["relay-a"]; info.direct {
		t.Fatal("expected disconnected peer to be marked non-direct")
	}
}

func TestRemovePeerClearsRelaySessionsTargetingDisconnectedPeer(t *testing.T) {
	targetSession := &transport.Session{}
	node := &Node{
		peers: map[string]*peerConn{
			"target": {id: "target", session: targetSession},
		},
		suppress:     map[string]map[string]time.Time{},
		relayBuckets: map[string]*nat.TokenBucket{},
		directProbes: map[string]time.Time{
			"target": time.Now(),
		},
		peerDials: map[string]time.Time{},
		knownPeers: map[string]knownPeer{
			"target": {id: "target", direct: true},
		},
		pubsub:      gossip.NewManager(),
		scoring:     gossip.NewEngine(),
		relayLocals: map[string]relayLocalSession{},
		dispatchCh:  make(chan any, 1),
	}
	node.relayLocals["stale-target"] = relayLocalSession{
		sessionID:    "stale-target",
		viaPeerID:    "relay-a",
		remotePeerID: "target",
		established:  true,
	}

	node.removePeer("target", targetSession)

	node.mu.RLock()
	defer node.mu.RUnlock()
	if _, ok := node.relayLocals["stale-target"]; ok {
		t.Fatal("expected relay session targeting disconnected peer to be removed")
	}
	if _, ok := node.directProbes["target"]; ok {
		t.Fatal("expected direct probe state for disconnected relay target to be removed")
	}
}

func TestRemovePeerClearsTransitRelayRoutesForDisconnectedPeer(t *testing.T) {
	peerSession := &transport.Session{}
	sessionManager := nat.NewSessionManager(4, time.Minute)
	if !sessionManager.Acquire("route-1") {
		t.Fatal("expected relay session manager acquisition to succeed")
	}

	node := &Node{
		peers: map[string]*peerConn{
			"target": {id: "target", session: peerSession},
		},
		suppress:      map[string]map[string]time.Time{},
		relayBuckets:  map[string]*nat.TokenBucket{},
		directProbes:  map[string]time.Time{},
		peerDials:     map[string]time.Time{},
		knownPeers:    map[string]knownPeer{"target": {id: "target", direct: true}},
		pubsub:        gossip.NewManager(),
		scoring:       gossip.NewEngine(),
		relayLocals:   map[string]relayLocalSession{},
		relayRoutes:   map[string]relayRoute{"route-1": {initiator: "origin", target: "target"}},
		relaySessions: sessionManager,
		dispatchCh:    make(chan any, 1),
	}

	node.removePeer("target", peerSession)

	node.mu.RLock()
	defer node.mu.RUnlock()
	if _, ok := node.relayRoutes["route-1"]; ok {
		t.Fatal("expected relay route for disconnected peer to be removed")
	}
	if node.relaySessions.Count() != 0 {
		t.Fatalf("expected relay session manager to release removed route, got count=%d", node.relaySessions.Count())
	}
}
