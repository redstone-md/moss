package mesh

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestCarrierReplacementCancellationRestoresExactPredecessor(t *testing.T) {
	var cancel context.CancelFunc
	f := startHeldCarrierCommit(t, func(_, phone *Node) { cancel = cancellableCarrierInbound(t, phone) })
	tentative := f.phone.peerByID(f.core.localPeerID())
	state := tentative.replacement.Load()
	cancel()
	waitFor(t, func() bool { return f.phone.peerByID(f.core.localPeerID()) == state.old }, "canceled replacement did not restore its predecessor")
	if state.old.readClosed.Load() || state.committed.Load() {
		t.Fatal("cancellation closed the predecessor or reported commitment")
	}
	assertWaitingAlternativePackets(t, f.origin, f.phone)
}

func cancellableCarrierInbound(t *testing.T, phone *Node) context.CancelFunc {
	t.Helper()
	phone.listener.Close()
	listener, err := net.Listen("tcp", nodeAddr(phone))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(phone.rootCtx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := listener.Accept()
		if err == nil {
			phone.wg.Add(1)
			phone.handleInbound(ctx, conn)
		}
	}()
	t.Cleanup(func() { cancel(); listener.Close(); <-done })
	return cancel
}

func TestCarrierReplacementRevocationClosesBothOwners(t *testing.T) {
	f := startHeldCarrierCommit(t, func(core, phone *Node) {
		phone.AllowPeer(core.localPeerID())
	})
	tentative := f.phone.peerByID(f.core.localPeerID())
	old := tentative.replacement.Load().old
	f.phone.DisallowPeer(f.core.localPeerID())
	waitFor(t, func() bool { return tentative.readClosed.Load() }, "revocation did not retire new physical owner")
	if f.phone.peerByID(f.core.localPeerID()) != nil || !old.readClosed.Load() {
		t.Fatal("revocation retained or restored the revoked predecessor")
	}
}

func TestCarrierReplacementRetainedSocketPreventsThirdAdmission(t *testing.T) {
	f := startHeldCarrierCommit(t)
	extra := startCappedOverlayNode(t, "carrier-commit-third")
	f.phone.mu.Lock()
	f.phone.activity.candidate = extra.localPeerID()
	f.phone.activity.candidateAddr = nodeAddr(extra)
	f.phone.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = f.phone.connectPeerTCPWithHint(ctx, nodeAddr(extra), extra.localPeerID())
	waitFor(t, func() bool { return extra.peerByID(f.phone.localPeerID()) == nil }, "refused third carrier retained a local ghost")
	if f.phone.peerByID(extra.localPeerID()) != nil {
		t.Fatal("third physical carrier entered the directory")
	}
	assertWaitingAlternativePackets(t, f.origin, f.phone)
}

func TestCarrierReplacementKeepsOrdinaryMaxPeersIdentitySlots(t *testing.T) {
	f := startHeldCarrierCommitMode(t, false, func(_, phone *Node) { phone.config.MaxPeers = 2 })
	extra := startCappedOverlayNode(t, "ordinary-replacement-extra")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := extra.connectPeerTCPWithHint(ctx, nodeAddr(f.phone), f.phone.localPeerID()); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for f.phone.peerByID(extra.localPeerID()) == nil && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if f.phone.peerByID(extra.localPeerID()) == nil {
		t.Fatal("temporary same-ID replacement consumed an ordinary admitted identity slot")
	}
	assertWaitingAlternativePackets(t, f.origin, f.phone)
}
