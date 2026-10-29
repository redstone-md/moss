package mesh

import (
	"testing"

	"github.com/redstone-md/moss/internal/gossip"
)

func TestWaitingMigrationRefusalRetiresOnlyUnusedCandidate(t *testing.T) {
	for _, used := range []bool{false, true} {
		old, fresh := startCappedOverlayNode(t, "refusal-core"), startCappedOverlayNode(t, "refusal-core")
		n := startCappedOverlayNode(t, "refusal-leaf")
		waitingRelayFixture(t, n, old)
		waitingRelayFixture(t, n, fresh)
		candidate := n.peerByID(fresh.localPeerID())
		candidate.applicationUsed.Store(used)
		// Isolate the refusal boundary without arming the retry loop: both
		// physical candidates and their authenticated sessions are real.
		n.mu.Lock()
		n.activity.requested.Store(int32(ActivityWaiting))
		n.activity.attachment = old.localPeerID()
		n.activity.candidate = fresh.localPeerID()
		n.activity.recovering = map[string]*peerConn{"lost-application": {id: "lost-application"}}
		n.mu.Unlock()
		n.refuseWaitingAttempt(nodeAddr(fresh), func() {})
		if kept := n.peerByID(fresh.localPeerID()) != nil; kept != used {
			t.Fatalf("failed handover candidate retained=%t application_used=%t", kept, used)
		}
		if n.peerByID(old.localPeerID()) == nil {
			t.Fatal("refusal removed the retained attachment")
		}
	}
}

func TestWaitingUnusedCandidateCleanupRevalidatesGenerationAndApplicationUse(t *testing.T) {
	for _, protect := range []string{"new-attempt", "new-use", "partial-migration"} {
		peer := &peerConn{id: "candidate"}
		n := &Node{started: true, peers: map[string]*peerConn{peer.id: peer}, pubsub: gossip.NewManager()}
		n.activity.requested.Store(int32(ActivityWaiting))
		n.activity.attempt = 2
		captured := uint64(1)
		if protect == "new-use" {
			captured = 2
			peer.applicationUsed.Store(true)
		}
		if protect == "partial-migration" {
			captured = 2
			app := &peerConn{id: "application", relayed: true, viaPeerID: peer.id}
			app.applicationUsed.Store(true)
			n.peers[app.id] = app
		}
		if n.removePeerWithPolicy(peer.id, nil, &captured, true) || n.peers[peer.id] != peer {
			t.Fatalf("refusal cleanup removed protected candidate: %s", protect)
		}
	}
}
