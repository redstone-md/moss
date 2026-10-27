package mesh

import (
	"context"
	"testing"
	"time"
)

// A genuine old provider record survives its leaf moving to another carrier.
// Losing only the origin's new carrier must still leave route time to reuse it.
func TestOrdinaryRecoveryReservesPreviousCarrierPastStaleHint(t *testing.T) {
	origin, receiver, old, fresh, _ := staleWaitingProviderFixture(t)
	await := time.Now().Add(time.Second)
	for {
		p := origin.peerByID(receiver.localPeerID())
		if p != nil && p.relayed && p.viaPeerID == fresh.localPeerID() {
			break
		}
		if time.Now().After(await) {
			t.Fatal("fixture did not establish the fresh application carrier")
		}
		time.Sleep(time.Millisecond)
	}
	key := mustOverlayID(t, receiver.localPeerID())

	// Keep the old provider genuinely stale. The Waiting recipient can now
	// repair through a known old core; that healthy readmission would invalidate
	// this fixture's intended stale-hint/live-previous-carrier comparison.
	receiver.DisallowPeer(old.localPeerID())
	if len(old.overlayStore.Get(key, time.Now())) == 0 {
		t.Fatal("old authenticated provider did not retain its stale attachment")
	}
	if old.peerByID(origin.localPeerID()) == nil {
		t.Fatal("old provider lost the origin connection")
	}
	lost := origin.peerByID(fresh.localPeerID())
	if lost == nil || lost.relayed {
		t.Fatal("fixture needs the origin's live common physical carrier")
	}
	lost.closeSession()
	deadline := time.Now().Add(2500 * time.Millisecond)
	for origin.peerByID(fresh.localPeerID()) == lost {
		if time.Now().After(deadline) {
			t.Fatal("origin physical carrier did not retire")
		}
		time.Sleep(time.Millisecond)
	}
	if origin.ConnectToPeer(receiver.localPeerID()) != MOSS_OK {
		t.Fatal("managed explicit recovery request was refused")
	}
	for {
		a, b := origin.peerByID(receiver.localPeerID()), receiver.peerByID(origin.localPeerID())
		if a != nil && b != nil && a.relayed && b.relayed && a.viaPeerID == fresh.localPeerID() && a.relaySessionID == b.relaySessionID {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("stale advertised hint consumed the route budget before the live previous carrier")
		}
		time.Sleep(time.Millisecond)
	}
	assertWaitingBidirectionalPackets(t, origin, receiver)
	if state := readActivity(t, receiver); state.Mode != "waiting" || state.Direct != 1 || state.Attachments != 1 {
		t.Fatal("previous carrier recovery expanded recipient waiting topology")
	}
}

func TestPreviousCarrierFreshHintUsesOneAuthenticatedAttempt(t *testing.T) {
	core, _, origin, receiver := ordinaryPreviousViaFixture(t)
	lost := origin.peerByID(core.localPeerID())
	lost.closeSession()
	deadline := time.Now().Add(time.Second)
	for origin.peerByID(core.localPeerID()) == lost {
		if time.Now().After(deadline) {
			t.Fatal("origin physical carrier did not retire")
		}
		time.Sleep(time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := origin.connectPeerWithHint(ctx, nodeAddr(core), core.localPeerID()); err != nil {
		t.Fatal("live previous carrier did not reconnect")
	}
	// The same carrier now returns its fresh authenticated provider record.
	// Delay the recipient beyond half the route phase: duplicate reservations
	// would expire one legitimate attempt and create another signed session.
	receiver.mu.Lock()
	beforeAdmissions, beforeRetired := receiver.relayAdmission, len(receiver.relayRetired)
	time.AfterFunc(1500*time.Millisecond, receiver.mu.Unlock)
	started := time.Now()
	if origin.ConnectToPeer(receiver.localPeerID()) != MOSS_OK {
		t.Fatal("managed explicit recovery request failed")
	}
	deadline = started.Add(2500 * time.Millisecond)
	for {
		a, b := origin.peerByID(receiver.localPeerID()), receiver.peerByID(origin.localPeerID())
		if a != nil && b != nil && a.relayed && b.relayed && a.relaySessionID == b.relaySessionID {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("fresh matching carrier hint lost its normal route budget")
		}
		time.Sleep(time.Millisecond)
	}
	assertWaitingBidirectionalPackets(t, origin, receiver)
	receiver.mu.RLock()
	admissions := receiver.relayAdmission - beforeAdmissions - uint64(len(receiver.relayRetired)-beforeRetired)
	receiver.mu.RUnlock()
	if admissions != 1 {
		t.Fatalf("same advertised/remembered carrier created%d signed admissions; want1", admissions)
	}
}
