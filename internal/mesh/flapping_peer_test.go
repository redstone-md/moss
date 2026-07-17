package mesh

import (
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/nat"
)

// A peer that answers the dial and then never speaks must be backed off from.
//
// The dial succeeding is not proof of a working path. A node drowning in its own
// flood accepts the connection and then drops every ping it is sent, so the
// session dies at six misses ~37s later — and because removePeer cleared the
// cooldown, the redial went out at once, succeeded again, and died again.
// Forever, with the backoff never once engaging, because as far as it knew every
// connect had worked. Players feel that loop as entering a lobby on the fourth or
// fifth attempt.
//
// The fleet still holds nodes that do this and cannot be reached to fix. The
// point is not to keep proving it every 37 seconds.
func TestAPeerThatDiesOnMissedPingsIsBackedOffFrom(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	node, err := NewNode("mesh-flap", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	node.mu.Lock()
	node.knownPeers["flapper"] = knownPeer{id: "flapper", addr: "203.0.113.9:4001", verified: true}
	node.peers["flapper"] = &peerConn{
		id:             "flapper",
		addr:           "203.0.113.9:4001",
		connectedAt:    time.Now().Add(-37 * time.Second),
		pingMisses:     peerDisconnectMissLimit,
		announceBudget: nat.NewTokenBucket(announceBurst, announceRatePerSecond),
	}
	session := node.peers["flapper"].session
	node.mu.Unlock()

	node.removePeer("flapper", session)

	node.mu.RLock()
	failures := node.peerDialFailures["flapper"]
	node.mu.RUnlock()
	if failures == 0 {
		t.Fatal("a session that died on six missed pings was recorded as a working path: " +
			"the redial goes out immediately and the flap repeats forever")
	}
	if targeted(node.discoveredPeerTargets(), "flapper") {
		t.Fatal("a peer that just died on missed pings was redialled at once")
	}
}

// A peer that simply went away — a clean disconnect, no misses — must be
// redialled immediately. Backing off from those would turn every ordinary
// reconnect into a stall.
func TestACleanDisconnectIsRedialledAtOnce(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	node, err := NewNode("mesh-clean-drop", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	node.mu.Lock()
	node.knownPeers["leaver"] = knownPeer{id: "leaver", addr: "203.0.113.10:4001", verified: true}
	node.peers["leaver"] = &peerConn{
		id:             "leaver",
		addr:           "203.0.113.10:4001",
		connectedAt:    time.Now().Add(-5 * time.Minute),
		pingMisses:     0,
		announceBudget: nat.NewTokenBucket(announceBurst, announceRatePerSecond),
	}
	node.peerDialFailures["leaver"] = 3 // stale history from an earlier bad patch
	session := node.peers["leaver"].session
	node.mu.Unlock()

	node.removePeer("leaver", session)

	node.mu.RLock()
	failures := node.peerDialFailures["leaver"]
	node.mu.RUnlock()
	if failures != 0 {
		t.Fatalf("a healthy session that ended cleanly left %d failures against the peer: "+
			"a working path must clear its history", failures)
	}
	if !targeted(node.discoveredPeerTargets(), "leaver") {
		t.Fatal("a peer that disconnected cleanly was not redialled immediately")
	}
}
