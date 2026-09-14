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
	peer := &peerConn{id: "stopping-source", session: rec.session, connectedAt: time.Now()}
	node.scoring.Ensure("stopping-source")
	// Registered on purpose: Stop only closes the sessions it finds in
	// n.peers, so an unregistered peer would leave readPeer parked on
	// ReadPacket forever and hang Stop's wg.Wait — the very hang this test
	// exists to pin. Registered the same way registerPeerFrom does before
	// launching readPeer, the peer dies by Stop's own hand: closeSession
	// kills it mid-flight and the queue drains inside wg.Wait, exactly as
	// for a production peer.
	node.mu.Lock()
	node.peers[peer.id] = peer
	node.mu.Unlock()
	node.wg.Add(1)
	go node.readPeer(peer)
	t.Cleanup(func() { rec.carrier.Close() })
	// A burst the read loop may still be enqueueing when the session dies.
	// Ping packets need no subscription and produce a Pong write the carrier
	// absorbs, so the burst flows end to end. The feed races the loop's own
	// lifetime, in two steps: an offered ciphertext the read loop is too busy
	// to take is dropped, and a dropped ciphertext leaves the noise nonces
	// out of step, so the next one fails to decrypt and tears the session
	// down — the carrier closing under the feeder is the channel equivalent
	// of a socket the peer just closed. That close mid-burst is part of what
	// the test exercises; offerReads serializes every offer against it under
	// the carrier's own lock, so the feeder observes the close instead of
	// racing it, and Stop still has to prove the important half: nothing
	// hangs, nothing panics inside the node.
	packet, err := json.Marshal(gossip.Envelope{Type: gossip.TypePing, RequestID: "probe"})
	if err != nil {
		t.Fatalf("marshal ping: %v", err)
	}
	feedBurst := func() (fed int) {
		for range peerDispatchQueueDepth {
			if err := farSess.WritePacket(packet); err != nil {
				t.Fatalf("far-end ping write: %v", err)
			}
			delivered, closed := offerReads(rec.carrier, farEnd.lastWrite())
			if closed {
				// The session's own teardown beat the feeder to the
				// carrier. A closed socket is a normal end to the burst.
				return fed
			}
			if delivered {
				fed++
			}
			// Undelivered: the read loop was mid-packet and the ciphertext
			// was dropped — the loss that desyncs the nonces and kills the
			// session. The next offer either feeds the dying session or
			// observes its close.
		}
		return fed
	}
	feedBurst()
	if code := node.Stop(); code != MOSS_OK {
		t.Fatalf("Stop: %d", code)
	}
}

// offerReads hands one ciphertext to the node's carrier without blocking,
// serialized against the carrier's Close under its own lock. The session the
// carrier serves can die mid-burst: a ciphertext the read loop cannot take is
// dropped, the noise nonces fall out of step, and the failed decrypt that
// follows tears the session down — closing the carrier while the burst is
// still in flight. An unsynchronized offer races that close (and can panic on
// a closed channel); under the lock the feeder observes the close instead.
// The receiving side needs no lock of its own — channel send and receive are
// their own synchronization.
func offerReads(carrier *recordingCarrier, packet []byte) (delivered, closed bool) {
	carrier.mu.Lock()
	defer carrier.mu.Unlock()
	if carrier.closed {
		return false, true
	}
	select {
	case carrier.reads <- packet:
		return true, false
	default:
		return false, false
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
