package mesh

import (
	"testing"

	"github.com/redstone-md/moss/internal/gossip"
)

func TestWaitingPreviousCarrierRetryRequiresCurrentPolicyAndIntent(t *testing.T) {
	for _, scope := range []string{"valid", "revoked", "forged", "no-address", "not-allowed", "no-intent", "live", "active", "stopped"} {
		t.Run(scope, func(t *testing.T) {
			n, err := NewNode("previous-policy", nil, DefaultConfig())
			if err != nil {
				t.Fatal(err)
			}
			n.started = true
			n.activity.requested.Store(int32(ActivityWaiting))
			n.activity.attachment = "old"
			n.activity.recovering = map[string]*peerConn{"application": {id: "application", viaPeerID: "old", relayed: true}}
			for id, addr := range map[string]string{"old": "198.51.100.1:1", "a": "198.51.100.2:1", "b": "198.51.100.3:1", "c": "198.51.100.4:1"} {
				n.knownPeers[id] = knownPeer{id: id, addr: addr, natTrusted: true, relayCapable: true, publicReachable: true, verified: true}
			}
			old := n.knownPeers["old"]
			switch scope {
			case "revoked":
				old.relayCapable = false
			case "forged":
				old.natTrusted = false
			case "no-address":
				old.addr = ""
			case "not-allowed":
				n.allowlist = map[string]struct{}{"a": {}, "b": {}, "c": {}}
			case "no-intent":
				n.activity.recovering = nil
			case "live":
				n.peers["old"] = &peerConn{id: "old"}
			case "active":
				n.activity.requested.Store(int32(ActivityActive))
			case "stopped":
				n.started = false
			}
			n.knownPeers["old"] = old
			batch := n.waitingAttemptCandidatesLocked("a", n.knownPeers["a"].addr)
			if len(batch) != waitingCandidatesPerAttempt || batch[0].id != "a" || batch[1].id != "b" {
				t.Fatal("previous retry expanded or reordered normal alternatives")
			}
			previous := batch[2].id == "old" && batch[2].previous
			if previous != (scope == "valid") {
				t.Fatalf("previous carrier preference bypassed current policy: scope=%s preferred=%t", scope, previous)
			}
		})
	}
}

func TestWaitingDiscardKeepsUsefulCurrentCarrier(t *testing.T) {
	for _, state := range []string{"ready", "partial"} {
		t.Run(state, func(t *testing.T) {
			carrier := &peerConn{id: "current"}
			n := &Node{started: true, peers: map[string]*peerConn{carrier.id: carrier}, pubsub: gossip.NewManager()}
			n.activity.requested.Store(int32(ActivityWaiting))
			n.activity.attachment, n.activity.candidate = carrier.id, carrier.id
			n.activity.attempt = 1
			n.activity.reachability = "ready"
			if state == "partial" {
				n.activity.reachability = "attaching"
				app := &peerConn{id: "app", relayed: true, viaPeerID: carrier.id}
				app.applicationUsed.Store(true)
				n.peers[app.id] = app
			}
			n.discardWaitingCandidate("rotation-cursor")
			if n.peers[carrier.id] != carrier || n.activity.attachment != carrier.id {
				t.Fatal("failed-candidate cleanup retired a current ready or useful partial path")
			}
		})
	}
}
