package mesh

import (
	"context"
	"testing"
	"time"
)

func TestActivityWaitingRefusesUDPBeforeConfirmingAFalseUpgrade(t *testing.T) {
	core := startCappedOverlayNode(t, "udp-policy-carrier")
	active := startCappedOverlayNode(t, "udp-policy-leaf")
	waiting := startCappedOverlayNode(t, "udp-policy-leaf")
	waitingRelayFixture(t, active, core)
	waitingRelayFixture(t, waiting, core)
	waiting.SetActivityMode(ActivityWaiting)
	awaitActivity(t, waiting, "waiting", "ready")
	if _, err := active.OpenRelaySession(core.localPeerID(), waiting.localPeerID(), time.Second); err != nil {
		t.Fatal(err)
	}
	old := active.peerByID(waiting.localPeerID())
	if old == nil || !old.relayed {
		t.Fatal("fixture needs an established relay before probing direct UDP")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err := active.connectPeerUDPWithHint(ctx, waiting.localPeerID(), nodeAddr(waiting))
	if err == nil {
		t.Fatal("waiting peer confirmed a direct upgrade it then refused to admit")
	}
	assertRelayMessages(t, active, waiting)
}
