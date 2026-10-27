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

func TestPendingRelayControlSurvivesCapturedCarrierReplacement(t *testing.T) {
	core, origin, receiver := pendingControlFixture(t)
	waitingUDPRelayFixture(t, origin, core)
	old := origin.peerByID(core.localPeerID())
	origin.scoringMu.Lock()
	locked := true
	defer func() {
		if locked {
			origin.scoringMu.Unlock()
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result := make(chan relayOpenResult, 1)
	go func() {
		sid, err := origin.openRelaySession(ctx, core.localPeerID(), receiver.localPeerID(), time.Second)
		result <- relayOpenResult{sid, err}
	}()
	waitFor(t, func() bool {
		origin.mu.RLock()
		defer origin.mu.RUnlock()
		for _, s := range origin.relayLocals {
			if !s.established && s.remotePeerID == receiver.localPeerID() {
				return true
			}
		}
		return false
	}, "pending API did not capture carrier before real write gate")
	upgraded := make(chan error, 1)
	go func() { upgraded <- origin.connectPeerTCPWithHint(ctx, nodeAddr(core), core.localPeerID()) }()
	waitFor(t, func() bool { return old.readClosed.Load() }, "real TCP replacement did not retire captured UDP carrier")
	origin.scoringMu.Unlock()
	locked = false
	select {
	case err := <-upgraded:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("physical replacement exceeded original deadline")
	}
	assertPendingControlResult(t, origin, receiver, result, ctx)
}

func TestPendingRelayControlSurvivesUnparsedRequestReplacement(t *testing.T) {
	f := heldPendingControl(t)
	// The old carrier accepted the actual signed Request without mesh dispatch.
	if err := f.core.connectPeerTCPWithHint(f.ctx, nodeAddr(f.origin), f.origin.localPeerID()); err != nil {
		t.Fatal(err)
	}
	got := assertPendingControlResult(t, f.origin, f.receiver, f.result, f.ctx)
	if got != f.request.RelaySession {
		t.Fatal("physical replacement changed pending signed session ownership")
	}
}

type pendingControlHold struct {
	core, origin, receiver *Node
	ctx                    context.Context
	cancel                 context.CancelFunc
	result                 chan relayOpenResult
	request                gossip.Envelope
	held                   *transport.Session
}

func heldPendingControl(t *testing.T) pendingControlHold {
	t.Helper()
	core, origin, receiver := pendingControlFixture(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	t.Cleanup(cancel)
	received := make(chan gossip.Envelope, 1)
	server := make(chan *transport.Session, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		session, err := transport.ServerHandshake(ctx, conn, transport.HandshakeConfig{MeshID: core.networkID, PSK: core.transportHandshakePSK(), Identity: core.identity})
		if err != nil {
			conn.Close()
			return
		}
		server <- session
		for {
			wire, err := session.ReadPacket()
			if err != nil {
				return
			}
			var env gossip.Envelope
			if json.Unmarshal(wire, &env) == nil && env.Type == gossip.TypeRelayRequest {
				received <- env
				return
			}
		}
	}()
	if err := origin.connectPeerTCPWithHint(ctx, listener.Addr().String(), core.localPeerID()); err != nil {
		t.Fatal(err)
	}
	var held *transport.Session
	select {
	case held = <-server:
		t.Cleanup(func() { held.Close() })
	case <-ctx.Done():
		t.Fatal("held counterpart did not negotiate real Noise")
	}
	result := make(chan relayOpenResult, 1)
	go func() {
		sid, err := origin.openRelaySession(ctx, core.localPeerID(), receiver.localPeerID(), time.Second)
		result <- relayOpenResult{sid, err}
	}()
	var request gossip.Envelope
	select {
	case request = <-received:
	case <-ctx.Done():
		t.Fatal("old physical carrier did not accept actual Request")
	}
	if !verifyRelayRequestEnvelope(request) {
		t.Fatal("held control was not genuinely authenticated")
	}
	return pendingControlHold{core, origin, receiver, ctx, cancel, result, request, held}
}

type relayOpenResult struct {
	sid string
	err error
}

func pendingControlFixture(t *testing.T) (core, origin, receiver *Node) {
	t.Helper()
	core, origin = startCappedOverlayNode(t, "pending-core"), startCappedOverlayNode(t, "pending-origin")
	if origin.localPeerID() < core.localPeerID() {
		core, origin = origin, core
	}
	receiver = startCappedOverlayNode(t, "pending-waiting", 1)
	waitingRelayFixture(t, receiver, core)
	receiver.SetActivityMode(ActivityWaiting)
	awaitActivity(t, receiver, "waiting", "ready")
	return
}

func assertPendingControlResult(t *testing.T, origin, receiver *Node, result <-chan relayOpenResult, ctx context.Context) string {
	t.Helper()
	var got relayOpenResult
	select {
	case got = <-result:
	case <-time.After(time.Second):
		t.Fatal("pending relay API outlived inherited deadline")
	}
	if got.err != nil {
		t.Fatalf("same-identity physical replacement lost pending signed relay admission: %v", got.err)
	}
	if ctx.Err() != nil {
		t.Fatal("pending admission completed beyond its original deadline")
	}
	a, b := origin.peerByID(receiver.localPeerID()), receiver.peerByID(origin.localPeerID())
	if a == nil || b == nil || !a.relayed || !b.relayed || a.relaySessionID != got.sid || b.relaySessionID != got.sid {
		t.Fatal("pending API completed without matching authenticated peer incarnations")
	}
	for _, endpoint := range []*Node{origin, receiver} {
		if endpoint.waitingRequested() {
			if state := readActivity(t, endpoint); state.Mode != "waiting" || state.Reachability != "ready" || state.Attachments != 1 || state.Direct != 1 {
				t.Fatalf("pending replay expanded waiting topology: %+v", state)
			}
		}
	}
	assertWaitingAlternativePackets(t, origin, receiver)
	return got.sid
}
