package mesh

import (
	"encoding/json"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/transport"
)

// The read loop must keep draining the socket while an envelope's handler is
// stuck, and a queue that cannot take more packets must say so in a counter.
//
// readPeer used to unmarshal and dispatch inline, so one slow handler — an
// application callback writing to disk, a lock the node holds — stopped the
// session being read: the transport's 256-packet buffer filled and silently
// discarded what came next, pings included, and nothing counted the loss. This
// drives the real readPeer over a real session, parks the application's
// delivery callback, and keeps feeding packets: every one must either queue
// (the read is still running) or be counted as `__dispatch_dropped__` — none
// may stall the read itself.
func TestReadPeerKeepsReadingWhileHandlerIsParked(t *testing.T) {
	node, err := NewNode("mesh-readpeer-parked", nil, isolatedTestConfig("readpeer-parked"))
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	if !transport.RunningGoTest() {
		t.Fatal("test build flag not set: the node would dial real discovery")
	}
	if code := node.Start(); code != MOSS_OK {
		t.Fatalf("Start: %d", code)
	}
	t.Cleanup(func() { node.Stop() })

	// A channel this node subscribes to, and the sealed payload a publish
	// would carry. The far end is a SECOND session over the same cipher keys
	// (one session for the whole feed — its nonce counter must advance in
	// lockstep with the node's receive counter), built in
	// readpeer_carrier_test.go.
	channel := "parked"
	if code := node.Subscribe(channel); code != MOSS_OK {
		t.Fatalf("Subscribe: %d", code)
	}
	sealed, err := node.sealRoom([]byte("payload"))
	if err != nil {
		t.Fatalf("sealRoom: %v", err)
	}
	rec := newRecordedSession(t)
	farEnd := newCapturingCarrier()
	farSess := cipherMatchedSession(t, farEnd)

	// The gate parks the APPLICATION delivery worker (enqueueLocal never
	// blocks), which is exactly the slowness the queue must absorb.
	gate := make(chan struct{})
	var delivered atomic.Int64
	node.SetMessageCallback(func(string, [32]byte, []byte) {
		<-gate
		delivered.Add(1)
	})

	peer := &peerConn{id: "parked-source", session: rec.session}
	node.scoring.Ensure("parked-source")
	node.scoring.SetApplicationScore("parked-source", gossip.PublishThreshold+1)
	node.wg.Add(1)
	go node.readPeer(peer)
	t.Cleanup(func() { rec.carrier.Close() })

	send := func(env gossip.Envelope) {
		t.Helper()
		packet, err := json.Marshal(env)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if err := farSess.WritePacket(packet); err != nil {
			t.Fatalf("far-end write: %v", err)
		}
		rec.carrier.reads <- farEnd.lastWrite()
	}

	// Park the first publish.
	send(gossip.Envelope{
		Type:      gossip.TypePublish,
		Channel:   node.roomTopic(channel),
		MessageID: "msg-0",
		SenderID:  node.identity.PublicKeyBytes(),
		Payload:   sealed,
	})
	// Wait for the dispatch worker to have handled the publish, then for the
	// delivery worker to have taken it off the channel queue. The queue
	// drains to a parked callback, so depth is NOT the signal — the queue's
	// EXISTENCE is: enqueueLocal created it when the publish was handed to
	// the channel's worker.
	waitFor(t, func() bool { return inboundCount(node, "publish") > 0 }, "the parked publish was never received")
	waitFor(t, func() bool { return queueExists(node, channel) }, "the publish never reached the channel's delivery worker")
	// While delivery is parked, keep feeding. The dispatch queue is bounded at
	// peerDispatchQueueDepth; everything past it must be COUNTED as dropped,
	// and the feeding itself must never block — that is the read loop still
	// draining the socket.
	fed := 0
	dropsBefore := inboundCount(node, "__dispatch_dropped__")
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case rec.carrier.reads <- nextPublish(t, node, channel, sealed, farEnd):
			fed++
		case <-deadline.C:
			t.Fatalf("feeding blocked after %d packets: the read loop stopped", fed)
		}
		if inboundCount(node, "__dispatch_dropped__") > dropsBefore {
			break
		}
	}
	if got := inboundCount(node, "__dispatch_dropped__") - dropsBefore; got == 0 {
		t.Fatalf("fed %d packets and no drop was counted: the queue grew past its bound", fed)
	}

	// Unpark: the parked publish is delivered (FIFO order through the queue),
	// and the read loop never stalled.
	close(gate)
	waitFor(t, func() bool { return delivered.Load() > 0 }, "the parked publish was never delivered")
}

// nextPublish produces the next publish ciphertext through the shared far-end
// session, so nonces advance in lockstep with the node's receive counter.
func nextPublish(t *testing.T, n *Node, channel string, sealed []byte, farEnd *capturingCarrier) []byte {
	t.Helper()
	return lastCiphertext(t, farEnd, n, channel, sealed)
}

// The per-peer dispatch queue must be closed exactly once, by the sender — a
// queue closed while the read loop can still send is a send-on-closed panic,
// and moss runs inside the host process, so the test pins the teardown order:
// Stop with packets in flight must neither panic nor hang.
func TestPeerDispatchQueueSurvivesStopWithPacketsInFlight(t *testing.T) {
	node, err := NewNode("mesh-readpeer-stop", nil, isolatedTestConfig("readpeer-stop"))
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	if code := node.Start(); code != MOSS_OK {
		t.Fatalf("Start: %d", code)
	}

	rec := newRecordedSession(t)
	farEnd := newCapturingCarrier()
	farSess := cipherMatchedSession(t, farEnd)
	peer := &peerConn{id: "stopping-source", session: rec.session}
	node.scoring.Ensure("stopping-source")
	node.wg.Add(1)
	go node.readPeer(peer)
	t.Cleanup(func() { rec.carrier.Close() })

	// A burst the read loop may still be enqueueing when Stop lands. Ping
	// packets need no subscription and produce a Pong write the carrier
	// absorbs, so the burst flows end to end. The feed races the loop's own
	// lifetime: the session's teardown closes the carrier channel under the
	// feeder, and a send to it panics — the channel equivalent of writing to
	// a socket the peer just closed. That close mid-burst is part of what
	// the test exercises, so the feeder recovers it and lets Stop prove the
	// important half: nothing hangs, nothing panics inside the node.
	packet, err := json.Marshal(gossip.Envelope{Type: gossip.TypePing, RequestID: "probe"})
	if err != nil {
		t.Fatalf("marshal ping: %v", err)
	}
	feedBurst := func() (fed int) {
		defer func() {
			if recover() != nil {
				// The carrier closed mid-send: the session's own teardown
				// beat the feeder to it. A closed socket is a normal end.
			}
		}()
		for i := 0; i < peerDispatchQueueDepth; i++ {
			if err := farSess.WritePacket(packet); err != nil {
				t.Fatalf("far-end ping write: %v", err)
			}
			select {
			case rec.carrier.reads <- farEnd.lastWrite():
				fed++
			default:
				// The read loop already drained what it could take; that
				// too is fine — the assertion is that nothing panics or
				// hangs inside the node.
			}
		}
		return fed
	}
	feedBurst()
	if code := node.Stop(); code != MOSS_OK {
		t.Fatalf("Stop: %d", code)
	}
}

// lastCiphertext writes one publish through the shared far-end session and
// returns the ciphertext the node's session will decrypt.
func lastCiphertext(t *testing.T, farEnd *capturingCarrier, n *Node, channel string, sealed []byte) []byte {
	t.Helper()
	packet, err := json.Marshal(gossip.Envelope{
		Type:      gossip.TypePublish,
		Channel:   n.roomTopic(channel),
		MessageID: "msg-" + strconv.Itoa(int(time.Now().UnixNano())),
		SenderID:  n.identity.PublicKeyBytes(),
		Payload:   sealed,
	})
	if err != nil {
		t.Fatalf("marshal publish: %v", err)
	}
	if err := farEnd.session().WritePacket(packet); err != nil {
		t.Fatalf("far-end write: %v", err)
	}
	return farEnd.lastWrite()
}

func inboundCount(n *Node, key string) uint64 {
	if v, ok := n.inboundByType.Load(key); ok {
		return v.(*atomic.Uint64).Load()
	}
	return 0
}

// queueExists reports whether the channel's delivery queue exists — the
// signal that a publish reached enqueueLocal, whose worker may already have
// taken the message (a parked callback drains nothing, but the first item
// leaves the queue instantly once the worker runs).
func queueExists(n *Node, channel string) bool {
	n.localMu.Lock()
	defer n.localMu.Unlock()
	return n.localQueues[channel] != nil
}

func waitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal(msg)
}
