package mesh

import (
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/nat"
	"github.com/redstone-md/moss/internal/transport"
)

// Cleanup of an old/pending route must not change ordinary direct-first
// recovery. Only loss of the currently established explicit route is evidence
// that an application target may have moved to another waiting carrier.
func TestRelayLossIdentityRecoveryScope(t *testing.T) {
	for _, tc := range []struct {
		name                                  string
		peer                                  *peerConn
		established, registered, wantIdentity bool
	}{
		{"established-explicit", &peerConn{relayed: true, relaySessionID: "current"}, true, true, true},
		{"pending", &peerConn{relayed: true, relaySessionID: "current"}, false, true, false},
		{"replaced", &peerConn{relayed: true, relaySessionID: "replacement"}, true, true, false},
		{"direct", &peerConn{}, true, true, false},
		{"absent", nil, true, true, false},
		{"unregistered", &peerConn{relayed: true, relaySessionID: "current"}, true, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n := &Node{peers: map[string]*peerConn{"target": tc.peer},
				explicitTargets: map[string]*explicitTarget{},
				knownPeers:      map[string]knownPeer{"target": {addr: "127.0.0.1:1234"}}}
			if tc.registered {
				n.explicitTargets["target"] = &explicitTarget{}
			}
			n.removeRelayedPeerLocked(relayLocalSession{remotePeerID: "target", sessionID: "current", established: tc.established})
			if got := n.activityIdentityFirst("target"); got != tc.wantIdentity {
				t.Fatalf("identity-first retry=%t, want %t", got, tc.wantIdentity)
			}
		})
	}
}

func TestOrdinaryRecoveryConsumesOnlyConfirmedPaths(t *testing.T) {
	for _, opted := range []bool{false, true} {
		for _, inbound := range []uint64{0, 1} {
			n := recoveryScopeNode(opted)
			peer := &peerConn{id: "target", session: &transport.Session{}, connectedAt: time.Now()}
			peer.inboundPackets.Store(inbound)
			n.peers["target"] = peer
			n.resetExplicitTargetLocked("target") // speculative Noise admission
			if !n.explicitTargets["target"].identityRetry {
				t.Fatal("speculative admission consumed recovery")
			}
			n.removePeer("target", peer.session)
			if got, want := n.explicitTargets["target"].identityRetry, opted || inbound == 0; got != want {
				t.Fatalf("direct teardown opted=%t inbound=%d retry=%t, want %t", opted, inbound, got, want)
			}
		}
		n := recoveryScopeNode(opted)
		peer, refused := n.registerRelayedPeerLocked(relayLocalSession{remotePeerID: "target", viaPeerID: "carrier", sessionID: "replacement", established: true})
		if refused || peer == nil {
			t.Fatal("replacement relay refused")
		}
		if !n.explicitTargets["target"].identityRetry {
			t.Fatal("old fallback registration consumed identity recovery")
		}
	}
}

func recoveryScopeNode(opted bool) *Node {
	n := &Node{
		peers: map[string]*peerConn{}, explicitTargets: map[string]*explicitTarget{"target": {identityRetry: true}},
		knownPeers: map[string]knownPeer{"target": {noiseStatic: make([]byte, 32)}},
		peerDials:  map[string]time.Time{}, peerDialFailures: map[string]int{},
		suppress: map[string]map[string]time.Time{}, relayBuckets: map[string]*nat.TokenBucket{},
		directProbes: map[string]time.Time{}, relayLocals: map[string]relayLocalSession{},
		pubsub: gossip.NewManager(), scoring: gossip.NewEngine(), dispatchCh: make(chan any, 1),
	}
	n.activity.optedIn = opted
	return n
}
