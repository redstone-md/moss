package mesh

import (
	"testing"
	"time"
)

func TestActivityLateDirectCleanupPreservesNewRelaySubscription(t *testing.T) {
	n, err := NewNode("late-direct-cleanup", nil, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	const id, via, topic = "returning-peer", "carrier", "messages"
	old := newRecordedSession(t)
	n.peers[id] = &peerConn{id: id, session: old.session, connectedAt: time.Now().Add(-time.Minute)}
	n.knownPeers[id] = knownPeer{id: id, noiseStatic: make([]byte, 32)}
	n.peers[via] = &peerConn{id: via}
	n.pubsub.Subscribe(topic)
	// The departing session has logically left, while its old outbound queue
	// is still draining. A relay acceptance can arrive in that real window.
	n.outboundMu.Lock()
	locked := true
	defer func() {
		if locked {
			n.outboundMu.Unlock()
		}
	}()
	done := make(chan struct{})
	go func() { n.removePeer(id, old.session); close(done) }()
	session := relayLocalSession{sessionID: "replacement", remotePeerID: id, viaPeerID: via, established: true}
	admitted := make(chan bool, 1)
	go func() {
		for {
			n.mu.Lock()
			if n.peers[id] == nil {
				break
			}
			n.mu.Unlock()
			time.Sleep(time.Millisecond)
		}
		n.relayLocals[session.sessionID] = session
		peer, refused := n.registerRelayedPeerLocked(session)
		n.mu.Unlock()
		if !refused && peer != nil {
			n.handleTopicGraft(peer, topic)
		}
		admitted <- !refused && peer != nil
	}()
	// Correctly serialized cleanup keeps admission behind this barrier.
	// Previously admission/GRAFT completed before the old cleanup resumed.
	var replacementReady bool
	select {
	case replacementReady = <-admitted:
	case <-time.After(50 * time.Millisecond):
	}
	n.outboundMu.Unlock()
	locked = false
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("old cleanup did not finish")
	}
	if !replacementReady {
		select {
		case replacementReady = <-admitted:
		case <-time.After(time.Second):
			t.Fatal("replacement relay remained blocked")
		}
	}
	if !replacementReady {
		t.Fatal("replacement relay was not admitted")
	}
	if !n.pubsub.HasPeerSubscription(id, topic) || !n.pubsub.InMesh(topic, id) {
		t.Fatal("old direct teardown removed the new relay's room subscription")
	}
}
