package mesh

import (
	"bytes"
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/transport"
)

func TestTCPUpgradeKeepsLiveRelayUntilRemoteAdmission(t *testing.T) {
	_, carrier, receiver := waitingRecipient(t)
	origin := startCappedOverlayNode(t, "upgrade-origin")
	waitingRelayFixture(t, origin, carrier)
	if _, err := origin.OpenRelaySession(carrier.localPeerID(), receiver.localPeerID(), time.Second); err != nil {
		t.Fatal(err)
	}
	assertWaitingBidirectionalPackets(t, origin, receiver)
	addr, negotiated, release := heldWaitingPolicyEndpoint(t, receiver)
	done := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { done <- origin.connectPeerTCPWithHint(ctx, addr, receiver.localPeerID()) }()
	select {
	case <-negotiated:
	case <-ctx.Done():
		t.Fatal("actual Noise upgrade did not negotiate")
	}
	// Give the local tail a chance to admit Noise; the refusing remote has not
	// admitted mesh traffic. A known working relay must carry sends throughout.
	select {
	case <-done:
	case <-time.After(25 * time.Millisecond):
	}
	got := make(chan bool, 1)
	receiver.SetPacketCallback(func(_ [32]byte, p []byte) { got <- bytes.Equal(p, []byte("upgrade-proof")) })
	if err := origin.SendToPeer(receiver.localPeerID(), []byte("upgrade-proof"), time.Second); err != nil {
		t.Fatal(err)
	}
	release()
	select {
	case ok := <-got:
		if !ok {
			t.Fatal("unexpected packet")
		}
	case <-time.After(time.Second):
		t.Fatal("application send vanished during refused TCP replacement of a live relay")
	}
	if p := origin.peerByID(receiver.localPeerID()); p == nil || !p.relayed {
		t.Fatal("refused upgrade removed working virtual peer")
	}
}

// Only scheduling is controlled: both Noise and the recipient's actual
// Waiting admission policy are real. No application frame is fabricated.
func heldWaitingPolicyEndpoint(t *testing.T, receiver *Node) (string, <-chan struct{}, func()) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	negotiated, decision := make(chan struct{}), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(decision) }) }
	finished := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(func() { release(); cancel(); l.Close(); <-finished })
	go func() {
		defer close(finished)
		c, err := l.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		session, err := transport.ServerHandshake(ctx, c, transport.HandshakeConfig{MeshID: receiver.networkID, PSK: receiver.transportHandshakePSK(), Identity: receiver.identity})
		if err != nil {
			return
		}
		defer session.Close()
		close(negotiated)
		select {
		case <-decision:
		case <-ctx.Done():
			return
		}
		receiver.registerPeerFrom(session, false, originInboundTCP)
	}()
	return l.Addr().String(), negotiated, release
}

func TestFallbackDoesNotConsumeOrdinaryIdentityRecovery(t *testing.T) {
	n := recoveryScopeNode(false)
	_, refused := n.registerRelayedPeerLocked(relayLocalSession{remotePeerID: "target", viaPeerID: "old-carrier", sessionID: "old-established", established: true})
	if refused {
		t.Fatal("fallback refused")
	}
	if !n.explicitTargets["target"].identityRetry {
		t.Fatal("restoring an old fallback consumed identity recovery without fresh acceptance")
	}
}

func TestRelayTCPConfirmationReplaysFirstFrame(t *testing.T) {
	carrier := startCappedOverlayNode(t, "replay-core")
	origin, remote := startCappedOverlayNode(t, "replay-leaf"), startCappedOverlayNode(t, "replay-leaf")
	waitingRelayFixture(t, origin, carrier)
	waitingRelayFixture(t, remote, carrier)
	if _, err := origin.OpenRelaySession(carrier.localPeerID(), remote.localPeerID(), time.Second); err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	server := make(chan error, 1)
	go func() {
		c, err := l.Accept()
		if err != nil {
			server <- err
			return
		}
		s, err := transport.ServerHandshake(ctx, c, transport.HandshakeConfig{MeshID: remote.networkID, PSK: remote.transportHandshakePSK(), Identity: remote.identity})
		if err != nil {
			c.Close()
			server <- err
			return
		}
		remote.sendDirectEnvelope(&peerConn{session: s}, gossip.Envelope{Type: gossip.TypeDirect, SenderID: remote.identity.PublicKeyBytes(), Payload: []byte("first-authenticated-frame")})
		remote.registerPeerFrom(s, false, originInboundTCP)
		server <- nil
	}()
	got := make(chan bool, 2)
	origin.SetPacketCallback(func(_ [32]byte, p []byte) { got <- bytes.Equal(p, []byte("first-authenticated-frame")) })
	if err := origin.connectPeerTCPWithHint(ctx, l.Addr().String(), remote.localPeerID()); err != nil {
		t.Fatal(err)
	}
	if err := <-server; err != nil {
		t.Fatal(err)
	}
	select {
	case ok := <-got:
		if !ok {
			t.Fatal("first authenticated frame changed")
		}
	case <-ctx.Done():
		t.Fatal("confirmation consumed application frame")
	}
}

func TestRelayTCPConfirmationCancellationKeepsVirtualRoute(t *testing.T) {
	_, carrier, receiver := waitingRecipient(t)
	origin := startCappedOverlayNode(t, "cancel-upgrade")
	waitingRelayFixture(t, origin, carrier)
	if _, err := origin.OpenRelaySession(carrier.localPeerID(), receiver.localPeerID(), time.Second); err != nil {
		t.Fatal(err)
	}
	addr, negotiated, release := heldWaitingPolicyEndpoint(t, receiver)
	defer release()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- origin.connectPeerTCPWithHint(ctx, addr, receiver.localPeerID()) }()
	select {
	case <-negotiated:
	case <-time.After(time.Second):
		t.Fatal("Noise upgrade missing")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("canceled unconfirmed upgrade reported success")
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("cancellation did not release upgrade")
	}
	assertWaitingBidirectionalPackets(t, origin, receiver)
}

func TestFreshRelayControlConsumesOrdinaryRecovery(t *testing.T) {
	for _, opted := range []bool{false, true} {
		carrier := startCappedOverlayNode(t, "fresh-recovery-core")
		a, b := startCappedOverlayNode(t, "fresh-recovery-leaf"), startCappedOverlayNode(t, "fresh-recovery-leaf")
		waitingRelayFixture(t, a, carrier)
		waitingRelayFixture(t, b, carrier)
		if opted {
			a.SetActivityMode(ActivityActive)
			b.SetActivityMode(ActivityActive)
		}
		for _, n := range []*Node{a, b} {
			n.mu.Lock()
			other := a
			if n == a {
				other = b
			}
			n.explicitTargets[other.localPeerID()] = &explicitTarget{identityRetry: true}
			n.mu.Unlock()
		}
		if _, err := a.OpenRelaySession(carrier.localPeerID(), b.localPeerID(), time.Second); err != nil {
			t.Fatal(err)
		}
		for _, n := range []*Node{a, b} {
			n.mu.RLock()
			other := a
			if n == a {
				other = b
			}
			retry := n.explicitTargets[other.localPeerID()].identityRetry
			n.mu.RUnlock()
			if retry != opted {
				t.Fatalf("fresh authenticated relay control opted=%t retry=%t", opted, retry)
			}
		}
		assertWaitingBidirectionalPackets(t, a, b)
	}
}
