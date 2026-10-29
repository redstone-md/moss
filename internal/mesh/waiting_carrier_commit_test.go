package mesh

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/transport"
)

func TestInboundCarrierReplacementKeepsRelayUntilCounterpartRegisters(t *testing.T) {
	f := startHeldCarrierCommit(t)
	core, phone, origin := f.core, f.phone, f.origin
	old, sid, ctx, session, captured := f.old, f.sid, f.ctx, f.session, f.captured
	core.mu.RLock()
	_, routed := core.relayRoutes[sid]
	preserved := core.peers[phone.localPeerID()] == old && routed
	core.mu.RUnlock()
	if !preserved {
		t.Fatal("pre-registration probe retired the still-working relay carrier")
	}
	assertWaitingAlternativePackets(t, origin, phone)
	if !core.registerPeerFromPacket(ctx, session, true, originDialTCP, 0, captured) {
		t.Fatal("authenticated counterpart replacement did not commit")
	}
	waitFor(t, func() bool {
		return phone.peerByID(core.localPeerID()) != nil && phone.peerByID(core.localPeerID()).session != nil && core.peerByID(phone.localPeerID()).session == session
	}, "carrier replacement did not finish")
	assertWaitingAlternativePackets(t, origin, phone)
}

type heldCarrierCommit struct {
	core, phone, origin *Node
	old                 *peerConn
	sid                 string
	ctx                 context.Context
	session             *transport.Session
	captured            []byte
}

func startHeldCarrierCommit(t *testing.T, configure ...func(*Node, *Node)) heldCarrierCommit {
	return startHeldCarrierCommitMode(t, true, configure...)
}

func startHeldCarrierCommitMode(t *testing.T, waiting bool, configure ...func(*Node, *Node)) heldCarrierCommit {
	return startHeldCarrierTransport(t, waiting, false, configure...)
}

func startHeldCarrierTransport(t *testing.T, waiting, datagram bool, configure ...func(*Node, *Node)) heldCarrierCommit {
	t.Helper()
	core, phone := startCappedOverlayNode(t, "carrier-commit-core"), startCappedOverlayNode(t, "carrier-commit-phone")
	if (core.localPeerID() > phone.localPeerID()) != datagram {
		core, phone = phone, core
	}
	origin := startCappedOverlayNode(t, "carrier-commit-origin")
	waitingRelayFixture(t, origin, core)
	if datagram {
		waitingUDPRelayFixture(t, phone, core)
	} else {
		waitingRelayFixture(t, phone, core)
	}
	if waiting {
		phone.SetActivityMode(ActivityWaiting)
		awaitActivity(t, phone, "waiting", "ready")
	}
	openUpgradeFixtureRelay(t, core, origin, phone)
	assertWaitingAlternativePackets(t, origin, phone)
	for _, setup := range configure {
		setup(core, phone)
	}
	old := core.peerByID(phone.localPeerID())
	sid := phone.peerByID(origin.localPeerID()).relaySessionID
	ctx, cancel := context.WithTimeout(context.Background(), core.config.HandshakeTimeout())
	t.Cleanup(cancel)
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", nodeAddr(phone))
	if err != nil {
		t.Fatal(err)
	}
	session, err := transport.ClientHandshake(ctx, conn, transport.HandshakeConfig{MeshID: core.networkID, PSK: core.transportHandshakePSK(), Identity: core.identity, RemoteStatic: phone.identity.NoiseStaticPublic()})
	if err != nil {
		conn.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { session.Close() })
	// A valid pre-registration probe must not commit either endpoint's old
	// carrier retirement. This core deliberately has not swapped its map yet.
	captured := awaitHeldCarrierProbe(t, session)
	return heldCarrierCommit{core, phone, origin, old, sid, ctx, session, captured}
}

func awaitHeldCarrierProbe(t *testing.T, session *transport.Session) []byte {
	t.Helper()
	const request = "held-counterpart-registration"
	wire, _ := json.Marshal(gossip.Envelope{Type: gossip.TypePing, RequestID: request})
	if err := session.WritePacket(wire); err != nil {
		t.Fatal(err)
	}
	type result struct {
		ok     bool
		packet []byte
	}
	done := make(chan result, 1)
	go func() {
		var captured []byte
		for {
			packet, err := session.ReadPacket()
			if err != nil {
				done <- result{}
				return
			}
			var env gossip.Envelope
			if json.Unmarshal(packet, &env) != nil {
				continue
			}
			if env.Type == gossip.TypePing {
				captured = packet
			}
			if env.Type == gossip.TypePong && env.RequestID == request {
				done <- result{true, captured}
				return
			}
		}
	}()
	select {
	case value := <-done:
		if !value.ok {
			t.Fatal("held authenticated peer did not answer the existing probe")
		}
		return value.packet
	case <-time.After(time.Second):
		t.Fatal("held authenticated peer did not process pre-registration probe")
	}
	return nil
}
