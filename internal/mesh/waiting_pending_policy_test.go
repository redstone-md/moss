package mesh

import (
	"context"
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
)

func TestPendingRelayControlExpiresWithoutReplay(t *testing.T) {
	for _, reason := range []string{"cancel", "deadline", "stop"} {
		t.Run(reason, func(t *testing.T) {
			f := heldPendingControl(t)
			switch reason {
			case "cancel":
				f.cancel()
			case "deadline":
				<-f.ctx.Done()
			case "stop":
				f.origin.Stop()
			}
			select {
			case got := <-f.result:
				if got.err == nil {
					t.Fatal("cancelled pending relay reported success")
				}
			case <-time.After(time.Second):
				t.Fatal("pending API did not stop within inherited bound")
			}
			if reason == "stop" {
				f.origin.Start()
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := f.core.connectPeerTCPWithHint(ctx, nodeAddr(f.origin), f.origin.localPeerID()); err != nil {
				t.Fatal(err)
			}
			f.origin.mu.RLock()
			_, local := f.origin.relayLocals[f.request.RelaySession]
			f.origin.mu.RUnlock()
			f.core.mu.RLock()
			_, route := f.core.relayRoutes[f.request.RelaySession]
			f.core.mu.RUnlock()
			if local || route || f.receiver.peerByID(f.origin.localPeerID()) != nil {
				t.Fatal("physical admission replayed an expired pending operation")
			}
		})
	}
}

func TestPendingRelayControlChangesCarrierOnlyAfterCommit(t *testing.T) {
	f := startHeldCarrierCommit(t)
	tentative := f.phone.peerByID(f.core.localPeerID())
	state := tentative.replacement.Load()
	f.origin.scoringMu.Lock()
	locked := true
	defer func() {
		if locked {
			f.origin.scoringMu.Unlock()
		}
	}()
	ctx, cancel := context.WithTimeout(f.ctx, time.Second)
	defer cancel()
	result := make(chan relayOpenResult, 1)
	go func() {
		sid, err := f.phone.openRelaySession(ctx, f.core.localPeerID(), f.origin.localPeerID(), time.Second)
		result <- relayOpenResult{sid, err}
	}()
	var sid string
	waitFor(t, func() bool {
		f.phone.mu.RLock()
		defer f.phone.mu.RUnlock()
		for id, s := range f.phone.relayLocals {
			if !s.established && s.pending != nil {
				sid = id
				return s.pending.carrier == state.old
			}
		}
		return false
	}, "pending Request did not stay on the verified predecessor")
	if !f.core.registerPeerFromPacket(f.ctx, f.session, true, originDialTCP, 0, f.captured) {
		t.Fatal("actual counterpart did not commit")
	}
	waitFor(t, func() bool {
		f.phone.mu.RLock()
		defer f.phone.mu.RUnlock()
		s := f.phone.relayLocals[sid]
		return state.committed.Load() && s.pending != nil && s.pending.carrier == tentative
	}, "pending Request did not follow committed exact physical carrier")
	f.origin.scoringMu.Unlock()
	locked = false
	got := assertPendingControlResult(t, f.phone, f.origin, result, ctx)
	if got != sid {
		t.Fatal("handover changed pending signed ownership")
	}
}

func TestPendingRelayControlRetainsClaimAcrossOldLossAndCommit(t *testing.T) {
	f := startHeldCarrierCommit(t)
	tentative := f.phone.peerByID(f.core.localPeerID())
	owner := tentative.replacement.Load()
	f.phone.scoringMu.Lock()
	senderHeld := true
	defer func() {
		if senderHeld {
			f.phone.scoringMu.Unlock()
		}
	}()
	f.origin.scoringMu.Lock()
	targetHeld := true
	defer func() {
		if targetHeld {
			f.origin.scoringMu.Unlock()
		}
	}()
	ctx, cancel := context.WithTimeout(f.ctx, time.Second)
	defer cancel()
	result := make(chan relayOpenResult, 1)
	go func() {
		sid, err := f.phone.openRelaySession(ctx, f.core.localPeerID(), f.origin.localPeerID(), time.Second)
		result <- relayOpenResult{sid, err}
	}()
	var sid string
	waitFor(t, func() bool {
		f.phone.mu.RLock()
		defer f.phone.mu.RUnlock()
		for id, s := range f.phone.relayLocals {
			if !s.established && s.pending != nil {
				sid = id
				return true
			}
		}
		return false
	}, "pending operation did not capture held predecessor")
	f.old.closeSession()
	waitFor(t, func() bool { return owner.old.readClosed.Load() }, "predecessor EOF not observed")
	f.phone.scoringMu.Unlock()
	senderHeld = false
	select {
	case got := <-result:
		t.Fatalf("pending control abandoned held replacement: %v", got.err)
	case <-time.After(20 * time.Millisecond):
	}
	if !f.core.registerPeerFromPacket(f.ctx, f.session, true, originDialTCP, 0, f.captured) {
		t.Fatal("normal replacement did not commit")
	}
	waitFor(t, func() bool { return owner.committed.Load() }, "replacement did not confirm")
	select {
	case got := <-result:
		t.Fatalf("old failed sender consumed reowned pending operation: %v", got.err)
	case <-time.After(20 * time.Millisecond):
	}
	f.phone.mu.RLock()
	session, retained := f.phone.relayLocals[sid]
	f.phone.mu.RUnlock()
	if !retained || session.pending == nil {
		t.Fatal("old failed claim erased newer pending owner")
	}
	f.origin.scoringMu.Unlock()
	targetHeld = false
	if got := assertPendingControlResult(t, f.phone, f.origin, result, ctx); got != sid {
		t.Fatal("pending SID changed")
	}
}

func TestPendingRelayControlSurvivesActualCarrierEOF(t *testing.T) {
	f := heldPendingControl(t)
	f.held.Close()
	waitFor(t, func() bool { return f.origin.peerByID(f.core.localPeerID()) == nil }, "old physical carrier did not actually leave")
	if err := f.core.connectPeerTCPWithHint(f.ctx, nodeAddr(f.origin), f.origin.localPeerID()); err != nil {
		t.Fatal(err)
	}
	if sid := assertPendingControlResult(t, f.origin, f.receiver, f.result, f.ctx); sid != f.request.RelaySession {
		t.Fatal("genuine carrier EOF changed pending signed operation")
	}
}

func TestPendingRelayControlFailsFastOnCurrentSendRefusal(t *testing.T) {
	core, origin, receiver := pendingControlFixture(t)
	waitingUDPRelayFixture(t, origin, core)
	current := origin.peerByID(core.localPeerID())
	origin.SetScoringCallback(func(_ [32]byte, _ float64) float64 { return gossip.GraylistThreshold - 1 })
	started := time.Now()
	_, err := origin.OpenRelaySession(core.localPeerID(), receiver.localPeerID(), time.Second)
	if err == nil {
		t.Fatal("refused current carrier reported relay admission")
	}
	if time.Since(started) >= 200*time.Millisecond {
		t.Fatal("known failed current write consumed relay attempt deadline")
	}
	if origin.peerByID(core.localPeerID()) != current {
		t.Fatal("negative control replaced the physical carrier")
	}
	origin.mu.RLock()
	defer origin.mu.RUnlock()
	for _, local := range origin.relayLocals {
		if local.pending != nil {
			t.Fatal("known failed send retained a pending claim")
		}
	}
}

func TestPendingRelayControlRequiresCurrentTargetPolicy(t *testing.T) {
	core, origin, receiver := pendingControlFixture(t)
	waitingUDPRelayFixture(t, origin, core)
	origin.AllowPeer(core.localPeerID())
	if _, err := origin.OpenRelaySession(core.localPeerID(), receiver.localPeerID(), time.Second); err == nil {
		t.Fatal("pending Request bypassed target allowlist")
	}
	if origin.peerByID(receiver.localPeerID()) != nil {
		t.Fatal("rejected target was admitted")
	}
	origin.mu.RLock()
	defer origin.mu.RUnlock()
	if len(origin.relayLocals) != 0 {
		t.Fatal("rejected target retained pending ownership")
	}
}

func TestPendingRelayControlCancellationWhileReplacementHeld(t *testing.T) {
	f := startHeldCarrierCommit(t)
	state := f.phone.peerByID(f.core.localPeerID()).replacement.Load()
	f.old.closeSession()
	waitFor(t, func() bool { return state.old.readClosed.Load() }, "held predecessor did not close")
	ctx, cancel := context.WithTimeout(f.ctx, 20*time.Millisecond)
	defer cancel()
	if _, err := f.phone.openRelaySession(ctx, f.core.localPeerID(), f.origin.localPeerID(), time.Second); err == nil {
		t.Fatal("cancelled pending claim reported success")
	}
	f.phone.mu.RLock()
	defer f.phone.mu.RUnlock()
	for _, local := range f.phone.relayLocals {
		if local.pending != nil {
			t.Fatal("cancelled held replacement retained pending claim")
		}
	}
}
