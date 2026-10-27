package mesh

import (
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
)

func TestRelayAdmissionOrderSurvivesOldControlsAndPromotion(t *testing.T) {
	for _, replay := range []string{"accept", "request", "promotion", "retired-request"} {
		t.Run(replay, func(t *testing.T) {
			old, origin, phone, oldSession, freshSession := admittedRelayPair(t, false)
			origin.ConnectToPeer(phone.localPeerID())
			origin.mu.Lock()
			origin.explicitTargets[phone.localPeerID()].identityRetry = true
			first := origin.relayLocals[oldSession].admission
			origin.mu.Unlock()
			switch replay {
			case "accept":
				old.sendEnvelope(old.peerByID(origin.localPeerID()), phone.signRelayAcceptEnvelope(gossip.Envelope{
					Type: gossip.TypeRelayAccept, RelaySession: oldSession, RelaySource: phone.localPeerID(), RelayTarget: origin.localPeerID(),
				}))
			case "request":
				old.sendEnvelope(old.peerByID(phone.localPeerID()), origin.signRelayRequestEnvelope(gossip.Envelope{
					Type: gossip.TypeRelayRequest, RelaySession: oldSession, RelaySource: origin.localPeerID(), RelayTarget: phone.localPeerID(),
				}))
			case "promotion":
				origin.mu.Lock()
				origin.registerRelayedPeerLocked(origin.relayLocals[oldSession])
				origin.mu.Unlock()
			case "retired-request":
				old.closeRelayRouteWithClose(oldSession, origin.localPeerID(), phone.localPeerID())
				awaitRelayRetirement(t, origin, oldSession)
				awaitRelayRetirement(t, phone, oldSession)
				// The same signed control can arrive on its original carrier
				// after a close forwarded over the fresh carrier retired it.
				peer := phone.peerByID(old.localPeerID())
				phone.handleRelayRequest(peer, origin.signRelayRequestEnvelope(gossip.Envelope{
					Type: gossip.TypeRelayRequest, RelaySession: oldSession, RelaySource: origin.localPeerID(), RelayTarget: phone.localPeerID(),
				}))
				phone.handleRelayClose(peer, gossip.Envelope{Type: gossip.TypeRelayClose, RelaySession: oldSession, RelaySource: origin.localPeerID(), RelayTarget: phone.localPeerID()})
			}
			old.closeRelayRouteWithClose(oldSession, origin.localPeerID(), phone.localPeerID())
			awaitRelayRetirement(t, origin, oldSession)
			awaitRelayRetirement(t, phone, oldSession)
			for _, pair := range [][2]*Node{{origin, phone}, {phone, origin}} {
				peer := pair[0].peerByID(pair[1].localPeerID())
				if peer == nil || !peer.relayed || peer.relaySessionID != freshSession || !peer.applicationUsed.Load() {
					t.Fatal("old control or promotion displaced the newer authenticated application path")
				}
			}
			origin.mu.RLock()
			retry := origin.explicitTargets[phone.localPeerID()].identityRetry
			origin.mu.RUnlock()
			if !retry || first == 0 {
				t.Fatal("old replay consumed identity recovery or lacked its original admission")
			}
			assertWaitingAlternativePackets(t, origin, phone)
		})
	}
}

func TestDirectTeardownKeepsNewestRelayForFirstPayload(t *testing.T) {
	old, origin, phone, oldSession, freshSession := admittedRelayPair(t, true)
	origin.peerByID(phone.localPeerID()).closeSession()
	deadline := time.Now().Add(time.Second)
	for {
		a, b := origin.peerByID(phone.localPeerID()), phone.peerByID(origin.localPeerID())
		if a != nil && b != nil && a.relayed && b.relayed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("real direct teardown did not restore its established relay paths")
		}
		time.Sleep(time.Millisecond)
	}
	// Reproduce the old map iteration's last, older directory promotion exactly.
	for _, n := range []*Node{origin, phone} {
		n.mu.Lock()
		n.registerRelayedPeerLocked(n.relayLocals[oldSession])
		n.mu.Unlock()
	}
	old.closeRelayRouteWithClose(oldSession, origin.localPeerID(), phone.localPeerID())
	awaitRelayRetirement(t, origin, oldSession)
	awaitRelayRetirement(t, phone, oldSession)
	for _, pair := range [][2]*Node{{origin, phone}, {phone, origin}} {
		peer := pair[0].peerByID(pair[1].localPeerID())
		if peer == nil || peer.relaySessionID != freshSession {
			t.Fatal("direct teardown selected the older relay and lost its replacement")
		}
	}
	assertWaitingAlternativePackets(t, origin, phone)
}

func admittedRelayPair(t *testing.T, direct bool) (*Node, *Node, *Node, string, string) {
	t.Helper()
	old, fresh := startCappedOverlayNode(t, "admission-old"), startCappedOverlayNode(t, "admission-fresh")
	origin, phone := startCappedOverlayNode(t, "admission-origin"), startCappedOverlayNode(t, "admission-phone")
	for _, leaf := range []*Node{origin, phone} {
		waitingRelayFixture(t, leaf, old)
		waitingRelayFixture(t, leaf, fresh)
	}
	if direct {
		attachLeaf(t, origin, phone)
		deadline := time.Now().Add(time.Second)
		for phone.peerByID(origin.localPeerID()) == nil {
			if time.Now().After(deadline) {
				t.Fatal("direct fixture did not finish real admission")
			}
			time.Sleep(time.Millisecond)
		}
		assertWaitingAlternativePackets(t, origin, phone)
	}
	oldSession, err := origin.OpenRelaySession(old.localPeerID(), phone.localPeerID(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	freshSession, err := origin.OpenRelaySession(fresh.localPeerID(), phone.localPeerID(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	assertWaitingAlternativePackets(t, origin, phone)
	return old, origin, phone, oldSession, freshSession
}

func awaitRelayRetirement(t *testing.T, n *Node, session string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		n.mu.RLock()
		_, exists := n.relayLocals[session]
		n.mu.RUnlock()
		if !exists {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("authenticated close did not retire its original session")
		}
		time.Sleep(time.Millisecond)
	}
}
