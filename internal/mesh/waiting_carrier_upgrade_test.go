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

func TestRelayStreamRefusalPreservesDatagramCarrier(t *testing.T) {
	carrier, origin := datagramUpgradePair(t)
	remote := startCappedOverlayNode(t, "carrier-refusal-leaf")
	waitingRelayFixture(t, remote, carrier)
	old := origin.peerByID(carrier.localPeerID())
	if old == nil || old.session == nil || isStreamNetwork(old.session.RemoteAddr()) {
		t.Fatal("fixture requires an authenticated datagram carrier")
	}
	addr, negotiated, refuse := heldCarrierRefusalEndpoint(t, carrier)
	origin.mu.Lock()
	info := origin.knownPeers[carrier.localPeerID()]
	info.addr = addr
	origin.knownPeers[carrier.localPeerID()] = info
	origin.mu.Unlock()
	openUpgradeFixtureRelay(t, carrier, origin, remote)
	received := make(chan []byte, 4)
	remote.SetPacketCallback(func(_ [32]byte, data []byte) { received <- data })
	payload := bytes.Repeat([]byte("relay-application"), 40)
	if err := origin.SendToPeer(remote.localPeerID(), payload, time.Second); err != nil {
		t.Fatal(err)
	}
	select {
	case <-negotiated:
	case <-time.After(time.Second):
		t.Fatal("real TCP Noise upgrade did not negotiate")
	}
	// Noise can finish before a legacy/over-capacity carrier refuses mesh
	// admission. Releasing that refusal must preserve the live datagram leg.
	refuse()
	select {
	case got := <-received:
		if !bytes.Equal(got, payload) {
			t.Fatal("directed application payload changed")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("application delivery lost when carrier refused TCP replacement")
	}
	if current := origin.peerByID(carrier.localPeerID()); current != old {
		t.Fatal("unconfirmed stream replaced authenticated datagram carrier")
	}
	assertWaitingBidirectionalPackets(t, origin, remote)
}

// An independent real Noise endpoint authenticates the carrier identity, then
// refuses before mesh admission. The existing UDP carrier keeps forwarding.
func heldCarrierRefusalEndpoint(t *testing.T, carrier *Node) (string, <-chan struct{}, func()) {
	t.Helper()
	// Replace only the carrier listener; its existing UDP/TCP sessions remain
	// live, and authenticated announcements keep naming this same endpoint.
	carrier.listener.Close()
	listener, err := net.Listen("tcp", nodeAddr(carrier))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	negotiated, refusal, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	refuse := func() { once.Do(func() { close(refusal); listener.Close() }) }
	t.Cleanup(func() { refuse(); cancel(); listener.Close(); <-done })
	go func() {
		defer close(done)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		session, err := transport.ServerHandshake(ctx, conn, transport.HandshakeConfig{MeshID: carrier.networkID, PSK: carrier.transportHandshakePSK(), Identity: carrier.identity})
		if err != nil {
			return
		}
		defer session.Close()
		close(negotiated)
		select {
		case <-refusal:
		case <-ctx.Done():
		}
	}()
	return listener.Addr().String(), negotiated, refuse
}

func TestDatagramCarrierTCPConfirmationCancellation(t *testing.T) {
	carrier, origin := datagramUpgradePair(t)
	old := origin.peerByID(carrier.localPeerID())
	addr, negotiated, refuse := heldCarrierRefusalEndpoint(t, carrier)
	defer refuse()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- origin.connectPeerTCPWithHint(ctx, addr, carrier.localPeerID()) }()
	select {
	case <-negotiated:
	case <-time.After(time.Second):
		t.Fatal("real TCP Noise upgrade did not negotiate")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("canceled unconfirmed upgrade reported success")
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("cancellation did not release carrier upgrade")
	}
	if origin.peerByID(carrier.localPeerID()) != old {
		t.Fatal("canceled stream replaced datagram carrier")
	}
	assertWaitingBidirectionalPackets(t, origin, carrier)
}

func TestDatagramCarrierTCPConfirmationReplaysFirstFrame(t *testing.T) {
	carrier, origin := datagramUpgradePair(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	server := make(chan error, 1)
	payload := []byte("first-datagram-carrier-upgrade-frame")
	got := make(chan []byte, 2)
	origin.SetPacketCallback(func(_ [32]byte, data []byte) { got <- data })
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			server <- err
			return
		}
		session, err := transport.ServerHandshake(ctx, conn, transport.HandshakeConfig{MeshID: carrier.networkID, PSK: carrier.transportHandshakePSK(), Identity: carrier.identity})
		if err != nil {
			conn.Close()
			server <- err
			return
		}
		carrier.sendDirectEnvelope(&peerConn{session: session}, gossip.Envelope{Type: gossip.TypeDirect, SenderID: carrier.identity.PublicKeyBytes(), Payload: payload})
		carrier.registerPeerFrom(session, false, originInboundTCP)
		server <- nil
	}()
	if err := origin.connectPeerTCPWithHint(ctx, listener.Addr().String(), carrier.localPeerID()); err != nil {
		t.Fatal(err)
	}
	if err := <-server; err != nil {
		t.Fatal(err)
	}
	select {
	case data := <-got:
		if !bytes.Equal(data, payload) {
			t.Fatal("first authenticated carrier frame changed")
		}
	case <-ctx.Done():
		t.Fatal("carrier confirmation consumed first application frame")
	}
	if peer := origin.peerByID(carrier.localPeerID()); peer == nil || !isStreamNetwork(peer.session.RemoteAddr()) {
		t.Fatal("authenticated stream did not replace datagram")
	}
	assertWaitingBidirectionalPackets(t, origin, carrier)
}

func datagramUpgradePair(t *testing.T) (*Node, *Node) {
	t.Helper()
	carrier, origin := startCappedOverlayNode(t, "carrier-core"), startCappedOverlayNode(t, "carrier-leaf")
	makeCore(carrier)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := origin.connectPeerUDPWithHint(ctx, carrier.localPeerID(), nodeAddr(carrier)); err != nil {
		t.Fatal(err)
	}
	origin.mu.RLock()
	opted := origin.activity.optedIn
	origin.mu.RUnlock()
	if opted {
		t.Fatal("carrier tests must preserve ordinary desktop lifecycle")
	}
	return carrier, origin
}
