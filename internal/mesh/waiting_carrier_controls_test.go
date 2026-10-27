package mesh

import (
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/transport"
)

func TestCarrierReplacementRefusalPreservesWorkingPredecessor(t *testing.T) {
	f := startHeldCarrierCommit(t)
	tentative := f.phone.peerByID(f.core.localPeerID())
	state := tentative.replacement.Load()
	f.session.Close()
	waitFor(t, func() bool { return f.phone.peerByID(f.core.localPeerID()) == state.old }, "refused replacement did not restore exact old carrier")
	if state.old.readClosed.Load() {
		t.Fatal("refused replacement closed retained old carrier")
	}
	if f.phone.confirmCarrierReplacement(tentative, state.request) {
		t.Fatal("stale replacement committed after rollback")
	}
	assertWaitingAlternativePackets(t, f.origin, f.phone)
}

func TestCarrierReplacementDeadlinePreservesWorkingPredecessor(t *testing.T) {
	f := startHeldCarrierCommit(t)
	tentative := f.phone.peerByID(f.core.localPeerID())
	state := tentative.replacement.Load()
	select {
	case <-state.done:
	case <-time.After(f.phone.config.HandshakeTimeout() + time.Second):
		t.Fatal("tentative replacement extended its original deadline")
	}
	if state.committed.Load() || f.phone.peerByID(f.core.localPeerID()) != state.old || state.old.readClosed.Load() {
		t.Fatal("unconfirmed deadline retired the working predecessor")
	}
	assertWaitingAlternativePackets(t, f.origin, f.phone)
}

func TestCarrierReplacementRequiresExactSessionAndPong(t *testing.T) {
	f := startHeldCarrierCommit(t)
	tentative := f.phone.peerByID(f.core.localPeerID())
	state := tentative.replacement.Load()
	f.phone.handleEnvelope(tentative, gossip.Envelope{Type: gossip.TypePong, RequestID: "unrelated-probe"})
	if f.phone.confirmCarrierReplacement(state.old, state.request) || state.committed.Load() {
		t.Fatal("unrelated Pong committed physical replacement")
	}
	f.phone.mu.RLock()
	count, attachments := f.phone.physicalPeerCountLocked(), f.phone.waitingAttachmentCountLocked()
	f.phone.mu.RUnlock()
	if count != 2 || attachments != 2 {
		t.Fatalf("retained sockets missing from physical budget: direct=%d attachments=%d", count, attachments)
	}
	assertWaitingAlternativePackets(t, f.origin, f.phone)
}

func TestCarrierReplacementStopClosesBothOwnedSessions(t *testing.T) {
	f := startHeldCarrierCommit(t)
	tentative := f.phone.peerByID(f.core.localPeerID())
	old := tentative.replacement.Load().old
	done := make(chan struct{})
	go func() { f.phone.Stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Stop left a retained carrier reader or confirmation owner blocked")
	}
	if !old.readClosed.Load() || !tentative.readClosed.Load() {
		t.Fatal("Stop omitted an owned physical session")
	}
}

func TestCarrierReplacementPreservesOrdinaryActiveAndStreams(t *testing.T) {
	const streamID transport.StreamID = 7
	received := make(chan string, 4)
	f := startHeldCarrierCommitMode(t, false, func(core, phone *Node) {
		core.OnStream(streamID, func(id string, data []byte) {
			if id == phone.localPeerID() {
				received <- string(data)
			}
		})
	})
	if f.phone.activity.optedIn {
		t.Fatal("ordinary compatibility fixture opted into activity policy")
	}
	send := func(body string) {
		t.Helper()
		if result := f.phone.SendStream(f.core.localPeerID(), streamID, []byte(body)); result != MOSS_OK {
			t.Fatalf("SendStream refused working carrier: %d", result)
		}
		select {
		case got := <-received:
			if got != body {
				t.Fatal("stream payload changed")
			}
		case <-time.After(time.Second):
			t.Fatal("stream bypassed retained working carrier")
		}
	}
	send("before")
	if !f.core.registerPeerFromPacket(f.ctx, f.session, true, originDialTCP, 0, f.captured) {
		t.Fatal("ordinary registered peer did not answer existing Ping/Pong")
	}
	waitFor(t, func() bool { return f.phone.peerByID(f.core.localPeerID()).replacement.Load() == nil }, "ordinary carrier did not commit")
	send("after")
	assertWaitingAlternativePackets(t, f.origin, f.phone)
}
