package mesh

import (
	"testing"

	"github.com/redstone-md/moss/internal/gossip"
)

func TestWaitingRecoveryCompletionCannotConsumeNewerCarrierLoss(t *testing.T) {
	old, latest := &peerConn{id: "target", relayed: true}, &peerConn{id: "target", relayed: true}
	old.applicationUsed.Store(true)
	fresh := &peerConn{id: "target", relayed: true, viaPeerID: "fresh"}
	n := &Node{peers: map[string]*peerConn{"target": fresh}}
	n.activity.recovering = map[string]*peerConn{"target": latest}
	if !n.completeWaitingRecovery("target", "fresh", old) || n.activity.recovering["target"] != latest {
		t.Fatal("old migration completion discarded a later carrier loss")
	}
	if !fresh.applicationUsed.Load() {
		t.Fatal("migrated application path lost its usage across carrier loss")
	}
	if !n.completeWaitingRecovery("target", "fresh", latest) || len(n.activity.recovering) != 0 {
		t.Fatal("verified current migration did not consume its own recovery intent")
	}
}

func TestWaitingRecoveryRetainsOnlyEstablishedPathsFromExactSession(t *testing.T) {
	peer := &peerConn{id: "used", relayed: true, relaySessionID: "live"}
	peer.applicationUsed.Store(true)
	n := &Node{peers: map[string]*peerConn{"used": peer}}
	for _, s := range []relayLocalSession{
		{remotePeerID: "used", sessionID: "live"},
		{remotePeerID: "used", sessionID: "obsolete", established: true},
		{remotePeerID: "absent", sessionID: "live", established: true},
	} {
		n.retainWaitingApplicationPathLocked(s)
	}
	if len(n.activity.recovering) != 0 {
		t.Fatal("unestablished or stale path became a Waiting recovery barrier")
	}
	n.retainWaitingApplicationPathLocked(relayLocalSession{remotePeerID: "used", sessionID: "live", established: true})
	if n.activity.recovering["used"] != peer {
		t.Fatal("established application path was not retained")
	}
}

func TestWaitingRecoveryPreservesSubscribedPathsAndAllowsCarrierAsTarget(t *testing.T) {
	peer := &peerConn{id: "member", relayed: true, relaySessionID: "old"}
	n := &Node{peers: map[string]*peerConn{"member": peer}, pubsub: gossip.NewManager()}
	n.pubsub.Subscribe("room")
	n.pubsub.SetPeerSubscription("member", "room", true)
	n.pubsub.SetMeshPeer("room", "member", true)
	n.retainWaitingApplicationPathLocked(relayLocalSession{remotePeerID: "member", sessionID: "old", established: true})
	if n.activity.recovering["member"] != peer {
		t.Fatal("subscribed-only established room path was discarded")
	}
	// The chosen carrier may itself be the authenticated application target;
	// its physical session satisfies the intent without a relay to itself.
	n.peers["member"] = &peerConn{id: "member"}
	if !n.completeWaitingRecovery("member", "member", peer) || len(n.activity.recovering) != 0 {
		t.Fatal("a retained target selected as carrier tried to relay to itself")
	}
}
