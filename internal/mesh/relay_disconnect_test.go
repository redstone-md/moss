package mesh

import (
	"fmt"
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/nat"
)

func TestDisconnectedRelayTargetAnswersStaleTrafficOnce(t *testing.T) {
	node, origin, sent := topicPrunePeer(t)
	targetWire := newRecordedSession(t)
	target := &peerConn{id: "departed-target", session: targetWire.session}
	node.peers[target.id] = target
	node.relayRoutes["old-route"] = relayRoute{initiator: origin.id, target: target.id}
	node.relaySessions.Acquire("old-route")
	node.removePeer(target.id, target.session)
	stale := gossip.Envelope{Type: gossip.TypeRelayData, RelaySession: "old-route",
		RelaySource: origin.id, RelayTarget: target.id, Payload: []byte("stale")}
	node.handleRelayData(origin, stale)
	if sent.writeCount() != 1 {
		t.Fatal("relay silently discarded traffic after its target disconnected")
	}
	// A TTL sweep can finish after disconnect and consumption of its close.
	// Retiring that absent route must not resurrect the one-shot reply.
	node.mu.Lock()
	node.retireRelayRouteLocked("old-route", time.Now())
	node.mu.Unlock()
	node.handleRelayData(origin, stale)
	if sent.writeCount() != 1 {
		t.Fatal("the relay sent repeated closes for one retired route")
	}
}

func TestDisconnectedRelayRoutesKeepBoundedRetirementState(t *testing.T) {
	node, target, _ := topicPrunePeer(t)
	node.relaySessions = nat.NewSessionManager(1100, time.Minute)
	for index := range 1040 {
		id := fmt.Sprintf("lost-route-%d", index)
		node.relayRoutes[id] = relayRoute{initiator: "origin", target: target.id}
		if !node.relaySessions.Acquire(id) {
			t.Fatal("fixture could not allocate transit route")
		}
	}
	node.removePeer(target.id, target.session)
	if len(node.relayRouteExpiry) != 1024 || node.relaySessions.Count() != 0 {
		t.Fatal("disconnect leaked relay slots or exceeded the documented tombstone cap")
	}
}
