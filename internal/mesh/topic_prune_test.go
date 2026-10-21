package mesh

import (
	"runtime"
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
)

func topicPrunePeer(t *testing.T, channels ...string) (*Node, *peerConn, *recordedSession) {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Trackers = nil
	node, err := NewNode("topic-prune-regression", nil, cfg)
	if err != nil {
		t.Fatal(err)
	}
	sent := newRecordedSession(t)
	peer := &peerConn{id: "topic-peer", session: sent.session}
	node.peers[peer.id] = peer
	for _, channel := range channels {
		node.pubsub.Subscribe(channel)
	}
	return node, peer, sent
}

func TestPruneRetainsStrangerRetryWindow(t *testing.T) {
	node, peer, _ := topicPrunePeer(t, "alpha")
	node.announceLocalSubscription("alpha")
	node.handleEnvelope(peer, gossip.Envelope{Type: gossip.TypePrune, Channel: "alpha"})
	if node.pubsub.HasPeerSubscription(peer.id, "alpha") {
		t.Fatal("the refusing stranger never claimed this topic")
	}
	if node.meshGraftEligible(peer.id, "alpha", time.Now().Add(3*time.Second)) {
		t.Fatal("PRUNE shortened the stranger retry window to less than 30 seconds")
	}
}

func TestPruneRetainsConfiguredRetryWindow(t *testing.T) {
	node, peer, _ := topicPrunePeer(t, "alpha")
	node.config.GossipSub.HeartbeatMS = 90000
	node.announceLocalSubscription("alpha")
	node.handleEnvelope(peer, gossip.Envelope{Type: gossip.TypePrune, Channel: "alpha"})
	if node.meshGraftEligible(peer.id, "alpha", time.Now().Add(45*time.Second)) {
		t.Fatal("PRUNE shortened the configured 90-second retry window")
	}
	if !node.meshGraftEligible(peer.id, "alpha", time.Now().Add(91*time.Second)) {
		t.Fatal("PRUNE must release the peer after the configured retry window")
	}
}

func TestSelectedPeerRefusalPreventsGraft(t *testing.T) {
	node, peer, sent := topicPrunePeer(t, "alpha")
	// The candidate was already selected when its refusal arrived.
	node.handleEnvelope(peer, gossip.Envelope{Type: gossip.TypePrune, Channel: "alpha"})
	node.graftTopicPeer(peer, "alpha", time.Now())
	if sent.writeCount() != 0 || len(peer.graftedAt) != 0 {
		t.Fatal("a rejected mesh admission still sent or recorded a GRAFT")
	}
}

func TestSelectedPeerCannotOutliveTopicOrConnection(t *testing.T) {
	for _, ended := range []string{"topic", "connection", "health"} {
		node, peer, sent := topicPrunePeer(t, "alpha")
		switch ended {
		case "topic":
			node.unsubscribeTopic("alpha")
		case "connection":
			delete(node.peers, peer.id)
		case "health":
			peer.meshBlocked = time.Now().Add(time.Minute)
		}
		node.graftTopicPeer(peer, "alpha", time.Now())
		if node.pubsub.InMesh("alpha", peer.id) || sent.writeCount() != 0 {
			t.Fatalf("selected candidate was admitted after %s ended", ended)
		}
	}
}

func TestPruneDoesNotBlockAnotherTopic(t *testing.T) {
	node, peer, sent := topicPrunePeer(t, "alpha", "beta")
	node.announceLocalSubscription("alpha")
	node.handleEnvelope(peer, gossip.Envelope{Type: gossip.TypePrune, Channel: "alpha"})
	before := sent.writeCount()
	node.maintainTopicMesh("beta")
	if sent.writeCount() <= before || !node.pubsub.InMesh("beta", peer.id) {
		t.Fatal("a refusal on alpha blocked the independent beta topic")
	}
}

func TestGraftOnAnotherTopicDoesNotClearRefusal(t *testing.T) {
	node, peer, _ := topicPrunePeer(t, "alpha", "beta")
	node.announceLocalSubscription("alpha")
	node.handleEnvelope(peer, gossip.Envelope{Type: gossip.TypePrune, Channel: "alpha"})
	node.handleEnvelope(peer, gossip.Envelope{Type: gossip.TypeGraft, Channel: "beta"})
	if !node.pubsub.InMesh("beta", peer.id) {
		t.Fatal("the beta subscriber must join immediately")
	}
	if node.meshGraftEligible(peer.id, "alpha", time.Now().Add(3*time.Second)) {
		t.Fatal("an unrelated GRAFT cleared alpha's refusal")
	}
}

func TestFreshSubscriberJoinsAfterRefusal(t *testing.T) {
	node, peer, _ := topicPrunePeer(t, "alpha")
	node.announceLocalSubscription("alpha")
	node.handleEnvelope(peer, gossip.Envelope{Type: gossip.TypePrune, Channel: "alpha"})
	node.handleEnvelope(peer, gossip.Envelope{Type: gossip.TypeGraft, Channel: "alpha"})
	if !node.pubsub.InMesh("alpha", peer.id) {
		t.Fatal("a first positive topic claim must admit the late subscriber immediately")
	}
}

func TestSubscriptionDoesNotClearHealthExclusion(t *testing.T) {
	node, peer, _ := topicPrunePeer(t, "alpha")
	peer.meshBlocked = time.Now().Add(time.Minute)
	node.handleEnvelope(peer, gossip.Envelope{Type: gossip.TypeGraft, Channel: "alpha"})
	if node.pubsub.InMesh("alpha", peer.id) || node.eligibleForMeshCandidate(peer.id) {
		t.Fatal("a subscription claim cleared the peer-wide health exclusion")
	}
}

func TestClaimedSubscriberStillHonorsRefusal(t *testing.T) {
	node, peer, _ := topicPrunePeer(t, "alpha")
	node.handleEnvelope(peer, gossip.Envelope{Type: gossip.TypeGraft, Channel: "alpha"})
	node.handleEnvelope(peer, gossip.Envelope{Type: gossip.TypePrune, Channel: "alpha"})
	if node.meshGraftEligible(peer.id, "alpha", time.Now().Add(3*time.Second)) {
		t.Fatal("a previous subscription claim bypassed the newer refusal")
	}
}

func TestUnsubscribeReleasesRetryBeforeResubscribe(t *testing.T) {
	node, peer, sent := topicPrunePeer(t)
	if node.Subscribe("alpha") != MOSS_OK {
		t.Fatal("subscribe failed")
	}
	if len(peer.graftedAt) == 0 {
		t.Fatal("setup: subscription must record a sent announcement")
	}
	if node.Unsubscribe("alpha") != MOSS_OK {
		t.Fatal("unsubscribe failed")
	}
	if len(peer.graftedAt) != 0 {
		t.Fatal("unsubscribe retained the old topic retry marker")
	}
	before := sent.writeCount()
	if node.Subscribe("alpha") != MOSS_OK || sent.writeCount() <= before {
		t.Fatal("the fresh subscription announcement was suppressed by old retry state")
	}
}

func TestOldConnectionCannotRecreateRefusal(t *testing.T) {
	for _, replaced := range []bool{false, true} {
		node, old, _ := topicPrunePeer(t, "alpha")
		delete(node.peers, old.id)
		node.pubsub.RemovePeer(old.id)
		if replaced {
			node.peers[old.id] = &peerConn{id: old.id, session: newRecordedSession(t).session}
		}
		node.handleEnvelope(old, gossip.Envelope{Type: gossip.TypePrune, Channel: "alpha"})
		if node.pubsub.InBackoff("alpha", old.id, time.Now()) {
			t.Fatalf("stale connection recreated refusal after removal (replaced=%v)", replaced)
		}
	}
}

func TestOldConnectionCannotClaimTopic(t *testing.T) {
	node, old, _ := topicPrunePeer(t, "alpha")
	delete(node.peers, old.id)
	node.pubsub.RemovePeer(old.id)
	node.handleEnvelope(old, gossip.Envelope{Type: gossip.TypeGraft, Channel: "alpha"})
	if node.pubsub.HasPeerSubscription(old.id, "alpha") || node.pubsub.InMesh("alpha", old.id) {
		t.Fatal("stale connection recreated subscription or membership after removal")
	}
}

func TestOldConnectionCannotThrottleReplacement(t *testing.T) {
	node, old, _ := topicPrunePeer(t, "alpha")
	replacement := &peerConn{id: old.id, session: newRecordedSession(t).session}
	node.peers[old.id] = replacement
	node.markMeshGrafted(old, "alpha", time.Now())
	if len(replacement.graftedAt) != 0 {
		t.Fatal("an old connection's send throttled the replacement's fresh announcement")
	}
}

func TestMeshCandidatesSkipRefusedPeerBeforeApplyingLimit(t *testing.T) {
	node, refused, _ := topicPrunePeer(t, "alpha")
	available := &peerConn{id: "available-peer", session: newRecordedSession(t).session}
	node.peers[available.id] = available
	node.pubsub.SetPeerSubscription(refused.id, "alpha", true)
	node.pubsub.SetPeerSubscription(available.id, "alpha", true)
	node.scoring.SetApplicationScore(refused.id, 10)
	node.scoring.SetApplicationScore(available.id, 5)
	node.handleEnvelope(refused, gossip.Envelope{Type: gossip.TypePrune, Channel: "alpha"})
	selected := node.selectMeshCandidates("alpha", 1)
	if len(selected) != 1 || selected[0] != available.id {
		t.Fatalf("refused high-scoring peer crowded out the available candidate: %v", selected)
	}
}

func TestGraftAdmissionUsesCurrentHealth(t *testing.T) {
	node, peer, _ := topicPrunePeer(t, "alpha")
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 20000; i++ {
			node.mu.Lock()
			peer.meshBlocked = time.Time{}
			node.pubsub.SetMeshPeer("alpha", peer.id, false)
			node.mu.Unlock()
			runtime.Gosched()
			node.mu.Lock()
			peer.meshBlocked = time.Now().Add(time.Minute)
			node.pubsub.SetMeshPeer("alpha", peer.id, false)
			node.mu.Unlock()
			runtime.Gosched()
		}
	}()
	t.Cleanup(func() { <-done })
	for {
		select {
		case <-done:
			return
		default:
		}
		node.handleEnvelope(peer, gossip.Envelope{Type: gossip.TypeGraft, Channel: "alpha"})
		node.mu.RLock()
		violated := time.Now().Before(peer.meshBlocked) && node.pubsub.InMesh("alpha", peer.id)
		node.mu.RUnlock()
		if violated {
			t.Fatal("cached eligibility admitted a peer after its health exclusion took effect")
		}
	}
}

func TestAlternateLiveRelayCanClaimAndPruneTopic(t *testing.T) {
	for _, relayed := range []bool{false, true} {
		node, current, _ := topicPrunePeer(t, "alpha")
		current.relayed = relayed
		current.relaySessionID = "preferred"
		origin := &peerConn{id: current.id, relayed: true, viaPeerID: "via", relaySessionID: "alternate"}
		node.relayLocals["alternate"] = relayLocalSession{
			sessionID: "alternate", remotePeerID: current.id, viaPeerID: "via", established: true,
		}
		node.handleEnvelope(origin, gossip.Envelope{Type: gossip.TypeGraft, Channel: "alpha"})
		if !node.pubsub.InMesh("alpha", current.id) {
			t.Fatalf("a live alternate relay was rejected (preferred relayed=%v)", relayed)
		}
		node.handleEnvelope(origin, gossip.Envelope{Type: gossip.TypePrune, Channel: "alpha"})
		if !node.pubsub.InBackoff("alpha", current.id, time.Now()) {
			t.Fatal("a live alternate relay must be able to refuse the topic")
		}
		delete(node.relayLocals, "alternate")
		node.pubsub.RemovePeer(current.id)
		node.handleEnvelope(origin, gossip.Envelope{Type: gossip.TypeGraft, Channel: "alpha"})
		node.handleEnvelope(origin, gossip.Envelope{Type: gossip.TypePrune, Channel: "alpha"})
		if node.pubsub.HasPeerSubscription(current.id, "alpha") ||
			node.pubsub.InBackoff("alpha", current.id, time.Now()) {
			t.Fatal("a retired relay recreated topic state")
		}
	}
}

func TestRetiredPreferredRelayCannotChangeTopic(t *testing.T) {
	node, peer, _ := topicPrunePeer(t, "alpha")
	peer.relayed = true
	peer.relaySessionID = "preferred"
	peer.viaPeerID = "via"
	node.relayLocals["preferred"] = relayLocalSession{
		sessionID: "preferred", remotePeerID: peer.id, viaPeerID: "via", established: true,
	}
	node.handleEnvelope(peer, gossip.Envelope{Type: gossip.TypeGraft, Channel: "alpha"})
	if !node.pubsub.InMesh("alpha", peer.id) {
		t.Fatal("a live preferred relay must be admitted")
	}
	delete(node.relayLocals, "preferred")
	node.pubsub.RemovePeer(peer.id)
	node.handleEnvelope(peer, gossip.Envelope{Type: gossip.TypeGraft, Channel: "alpha"})
	node.handleEnvelope(peer, gossip.Envelope{Type: gossip.TypePrune, Channel: "alpha"})
	node.markMeshGrafted(peer, "alpha", time.Now())
	if node.pubsub.HasPeerSubscription(peer.id, "alpha") ||
		node.pubsub.InBackoff("alpha", peer.id, time.Now()) || len(peer.graftedAt) != 0 {
		t.Fatal("a retired preferred relay recreated topic state")
	}
}
