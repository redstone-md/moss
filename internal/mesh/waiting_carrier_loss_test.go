package mesh

import (
	"github.com/redstone-md/moss/internal/gossip"
	"testing"
	"time"
)

func TestCarrierReplacementRecoversRemoteRetiredRelayIncarnation(t *testing.T) {
	f := startHeldCarrierCommit(t)
	tentative := f.phone.peerByID(f.core.localPeerID())
	state := tentative.replacement.Load()
	if state == nil {
		t.Fatal("fixture needs an uncommitted physical replacement")
	}
	f.old.closeSession()
	waitFor(t, func() bool { return state.old.readClosed.Load() }, "old carrier did not report genuine loss")
	// The core retires the route when its own reader removes the lost peer,
	// which can trail the phone's view of the same loss.
	waitFor(t, func() bool {
		f.core.mu.RLock()
		defer f.core.mu.RUnlock()
		_, routed := f.core.relayRoutes[f.sid]
		return !routed
	}, "core did not retire the lost forwarding route")
	if !f.core.registerPeerFromPacket(f.ctx, f.session, true, originDialTCP, 0, f.captured) {
		t.Fatal("authenticated new physical carrier was discarded after old loss")
	}
	waitFor(t, func() bool { return tentative.replacement.Load() == nil }, "new carrier did not commit")
	if current := f.phone.peerByID(f.core.localPeerID()); current != tentative {
		t.Fatal("valid replacement carrier was unnecessarily retired")
	}
	// The remote core really lost this forwarding route before admitting its
	// new carrier. Existing authenticated expiry feedback must replace S; the
	// physical Pong alone must not be mistaken for end-to-end route evidence.
	f.core.mu.RLock()
	_, routed := f.core.relayRoutes[f.sid]
	f.core.mu.RUnlock()
	if routed {
		t.Fatal("fixture did not retire remote forwarding incarnation")
	}
	peer := f.phone.peerByID(f.origin.localPeerID())
	if peer != nil && peer.relaySessionID == f.sid {
		if !f.phone.sendEnvelope(peer, gossip.Envelope{Type: gossip.TypePing, RequestID: "lost-route-existing-probe"}) {
			t.Fatal("existing route probe could not reach authenticated carrier")
		}
	}
	deadline := time.Now().Add(f.phone.config.HandshakeTimeout() + time.Second)
	for time.Now().Before(deadline) {
		local, remote := f.phone.peerByID(f.origin.localPeerID()), f.origin.peerByID(f.phone.localPeerID())
		if local != nil && remote != nil && local.relaySessionID != f.sid && local.relaySessionID == remote.relaySessionID {
			assertWaitingAlternativePackets(t, f.origin, f.phone)
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("authenticated expiry feedback did not recover the lost relay incarnation")
}
