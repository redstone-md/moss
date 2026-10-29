package mesh

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
)

func TestCarrierReplacementBufferedPongCannotCommitKnownDeadSession(t *testing.T) {
	f := startHeldCarrierCommit(t)
	captured := capturedHeldCarrierNonce(t, f)
	phone := f.phone
	tentative := phone.peerByID(f.core.localPeerID())
	state := tentative.replacement.Load()
	before := tentative.inboundPackets.Load()
	phone.mu.Lock()
	locked := true
	defer func() {
		if locked {
			phone.mu.Unlock()
		}
	}()
	registered := make(chan bool, 1)
	go func() {
		registered <- f.core.registerPeerFromPacket(f.ctx, f.session, true, originDialTCP, 0, captured)
	}()
	waitFor(t, func() bool { return tentative.inboundPackets.Load() > before }, "actual registered counterpart Pong did not reach new reader")
	f.session.Close()
	waitFor(t, func() bool { return tentative.readClosed.Load() }, "new reader did not observe EOF before Pong dispatch")
	phone.mu.Unlock()
	locked = false
	select {
	case <-state.done:
	case <-time.After(time.Second):
		t.Fatal("closed tentative owner did not finish")
	}
	if state.committed.Load() || state.old.readClosed.Load() {
		t.Fatal("buffered Pong committed a known-dead session and retired the healthy predecessor")
	}
	waitFor(t, func() bool { return phone.peerByID(f.core.localPeerID()) == state.old }, "closed tentative session did not restore exact predecessor")
	select {
	case <-registered:
	case <-time.After(time.Second):
		t.Fatal("counterpart rollback remained blocked")
	}
	assertWaitingAlternativePackets(t, f.origin, phone)
}

func TestCarrierReplacementEOFBeforeRollbackCannotBypassPendingOwner(t *testing.T) {
	// TCP transport priority admits an unpreferred first direction over UDP;
	// the next preferred outbound TCP would otherwise win during EOF cleanup.
	f := startHeldCarrierTransport(t, false, true)
	phone := f.phone
	tentative := phone.peerByID(f.core.localPeerID())
	state := tentative.replacement.Load()
	before := tentative.inboundPackets.Load()
	phone.scoringMu.Lock()
	locked := true
	defer func() {
		if locked {
			phone.scoringMu.Unlock()
		}
	}()
	wire, _ := json.Marshal(gossip.Envelope{Type: gossip.TypePing, RequestID: "hold-tentative-worker"})
	if err := f.session.WritePacket(wire); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return tentative.inboundPackets.Load() > before }, "new reader did not queue held control")
	f.session.Close()
	waitFor(t, func() bool { return tentative.readClosed.Load() }, "tentative EOF not observed")
	_ = phone.connectPeerTCPWithHint(f.ctx, nodeAddr(f.core), f.core.localPeerID())
	if phone.peerByID(f.core.localPeerID()) != tentative || state.old.readClosed.Load() {
		t.Fatal("known-dead tentative fast path discarded its still-live pending predecessor")
	}
	phone.scoringMu.Unlock()
	locked = false
	waitFor(t, func() bool { return phone.peerByID(f.core.localPeerID()) == state.old }, "pending predecessor did not restore after worker drain")
	assertWaitingAlternativePackets(t, f.origin, phone)
}

func capturedHeldCarrierNonce(t *testing.T, f heldCarrierCommit) []byte {
	t.Helper()
	if f.captured != nil {
		return f.captured
	}
	result := make(chan []byte, 1)
	go func() {
		for {
			wire, err := f.session.ReadPacket()
			if err != nil {
				result <- nil
				return
			}
			var env gossip.Envelope
			if json.Unmarshal(wire, &env) == nil && env.Type == gossip.TypePing {
				result <- wire
				return
			}
		}
	}()
	select {
	case wire := <-result:
		if wire != nil {
			return wire
		}
	case <-time.After(time.Second):
	}
	t.Fatal("tentative carrier did not send existing correlated Ping")
	return nil
}

func TestCarrierReplacementKeepsOrdinaryDirectOnlyStartup(t *testing.T) {
	core, phone := startCappedOverlayNode(t, "direct-only-core"), startCappedOverlayNode(t, "direct-only-phone")
	if core.localPeerID() > phone.localPeerID() {
		core, phone = phone, core
	}
	attachLeaf(t, phone, core)
	old := phone.peerByID(core.localPeerID())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := core.connectPeerTCPWithHint(ctx, nodeAddr(phone), phone.localPeerID()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		current := phone.peerByID(core.localPeerID())
		remote := core.peerByID(phone.localPeerID())
		return current != nil && current != old && remote != nil && !remote.relayed
	}, "preferred ordinary direct session was not admitted")
	assertWaitingAlternativePackets(t, core, phone)
}
