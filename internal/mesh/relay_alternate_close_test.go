package mesh

import (
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
)

func TestRelayClosePreservesAnotherEstablishedApplicationPath(t *testing.T) {
	for _, active := range []bool{true, false} {
		t.Run(map[bool]string{true: "active-close", false: "stale-close"}[active], func(t *testing.T) {
			old, fresh := startCappedOverlayNode(t, "alternate-old"), startCappedOverlayNode(t, "alternate-fresh")
			origin, phone := startCappedOverlayNode(t, "alternate-origin"), startCappedOverlayNode(t, "alternate-phone")
			for _, leaf := range []*Node{origin, phone} {
				waitingRelayFixture(t, leaf, old)
				waitingRelayFixture(t, leaf, fresh)
			}
			oldSession, err := origin.OpenRelaySession(old.localPeerID(), phone.localPeerID(), time.Second)
			if err != nil {
				t.Fatal(err)
			}
			freshSession, err := origin.OpenRelaySession(fresh.localPeerID(), phone.localPeerID(), time.Second)
			if err != nil {
				t.Fatal(err)
			}
			assertWaitingBidirectionalPackets(t, origin, phone)
			if active {
				// An authenticated accept may be queued behind a newer route.
				old.sendEnvelope(old.peerByID(origin.localPeerID()), phone.signRelayAcceptEnvelope(gossip.Envelope{
					Type: gossip.TypeRelayAccept, RelaySession: oldSession, RelaySource: phone.localPeerID(), RelayTarget: origin.localPeerID(),
				}))
				awaitAlternateRelaySession(t, origin, phone.localPeerID(), oldSession)
			}
			old.closeRelayRouteWithClose(oldSession, origin.localPeerID(), phone.localPeerID())
			deadline := time.Now().Add(time.Second)
			for {
				origin.mu.RLock()
				_, closing := origin.relayLocals[oldSession]
				origin.mu.RUnlock()
				if !closing {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("authenticated close did not retire its original session")
				}
				time.Sleep(time.Millisecond)
			}
			peer := origin.peerByID(phone.localPeerID())
			if peer == nil || !peer.relayed || peer.relaySessionID != freshSession {
				t.Fatal("authenticated current-session close erased another established application route")
			}
			if !peer.applicationUsed.Load() {
				t.Fatal("alternate route lost application interest")
			}
			assertWaitingBidirectionalPackets(t, origin, phone)
		})
	}
}

func awaitAlternateRelaySession(t *testing.T, n *Node, peerID, session string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		peer := n.peerByID(peerID)
		if peer != nil && peer.relayed && peer.relaySessionID == session {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("authenticated relay accept did not select its established route")
		}
		time.Sleep(time.Millisecond)
	}
}
