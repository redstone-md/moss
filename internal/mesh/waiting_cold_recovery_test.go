package mesh

import (
	"context"
	"encoding/hex"
	"testing"
	"time"
)

func TestColdRelayRecoveryConfirmationIsTargetScoped(t *testing.T) {
	for _, scope := range []string{"ordinary", "direct-only", "lost-relay", "another-target"} {
		t.Run(scope, func(t *testing.T) {
			n, err := NewNode("cold-recovery-scope", nil, DefaultConfig())
			if err != nil {
				t.Fatal(err)
			}
			id := [32]byte{1}
			peerID := hex.EncodeToString(id[:])
			if scope == "direct-only" || scope == "lost-relay" {
				n.explicitTargets[peerID] = &explicitTarget{identityRetry: scope == "lost-relay"}
			}
			if scope == "another-target" {
				n.explicitTargets["unrelated"] = &explicitTarget{identityRetry: true}
			}
			session := newRecordedSessionForPeer(t, id, [32]byte{2})
			if got := n.relayUpgradeNeedsConfirmation(session.session, 0); got != (scope == "lost-relay") {
				t.Fatalf("cold confirmation changed unrelated startup policy: scope=%s required=%t", scope, got)
			}
		})
	}
}

func TestOrdinaryLostRelayDoesNotAdmitNoiseOnlyColdTarget(t *testing.T) {
	_, carrier, receiver := waitingRecipient(t)
	origin := startCappedOverlayNode(t, "cold-recovery-origin")
	waitingRelayFixture(t, origin, carrier)
	openUpgradeFixtureRelay(t, carrier, origin, receiver)
	assertWaitingBidirectionalPackets(t, origin, receiver)
	origin.ConnectToPeer(receiver.localPeerID())
	old := origin.peerByID(receiver.localPeerID())
	carrier.closeRelayRouteWithClose(old.relaySessionID, origin.localPeerID(), receiver.localPeerID())
	deadline := time.Now().Add(time.Second)
	for origin.peerByID(receiver.localPeerID()) != nil {
		if time.Now().After(deadline) {
			t.Fatal("established route did not retire")
		}
		time.Sleep(time.Millisecond)
	}
	origin.mu.RLock()
	retrying := origin.explicitTargets[receiver.localPeerID()].identityRetry
	opted := origin.activity.optedIn
	origin.mu.RUnlock()
	if !retrying || opted {
		t.Fatal("fixture requires ordinary explicit recovery after a real established relay loss")
	}
	addr, negotiated, release := heldWaitingPolicyEndpoint(t, receiver)
	defer release()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- origin.connectPeerTCPWithHint(ctx, addr, receiver.localPeerID()) }()
	select {
	case <-negotiated:
	case <-ctx.Done():
		t.Fatal("real cold TCP recovery did not negotiate Noise")
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("lost-relay recovery reported a direct success before mesh admission")
		}
	case <-time.After(25 * time.Millisecond):
	}
	if peer := origin.peerByID(receiver.localPeerID()); peer != nil && !peer.relayed {
		t.Fatal("Noise-only recovery exposed a false direct application path")
	}
	release()
	var refusal error
	select {
	case err := <-done:
		refusal = err
	case <-ctx.Done():
		// The cached-static fallback may make a second Noise attempt. Its
		// completion can race the same inherited deadline; no timer is renewed.
		select {
		case refusal = <-done:
		case <-time.After(250 * time.Millisecond):
			t.Fatal("cancelled cold recovery outlived its inherited deadline")
		}
	}
	if refusal == nil {
		t.Fatal("Waiting refusal became a cold direct success")
	}
	openUpgradeFixtureRelay(t, carrier, origin, receiver)
	assertWaitingBidirectionalPackets(t, origin, receiver)
}
