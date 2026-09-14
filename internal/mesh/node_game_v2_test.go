package mesh

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/transport"
)

// The v2 game preset tests: the delta codec's wire contract, the delta send
// path's threshold and base bookkeeping, decode validation, the OnSnapshot
// wrapper's delta merging, AOI culling on directed sends, and the Predictor
// hook. Float comparisons are bit comparisons throughout — the wire format
// is NaN-transparent and distinguishes +0.0 from -0.0, which Go's == on
// float64 cannot express (see the fuzz tests in node_game_fuzz_test.go).

// gameV2Node builds an isolated node and, when start is set, starts it with
// the gossip heartbeat stretched to 60s: the maintenance tick pings every
// peer with a session, which would race a capturing carrier's write count in
// send-path tests. A 60s first tick keeps the observation window clean (the
// overlay republish loop's first tick, at 30s, likewise stays away).
func gameV2Node(t *testing.T, name string, start bool) *Node {
	t.Helper()
	cfg := isolatedTestConfig(name)
	cfg.GossipSub.HeartbeatMS = 60_000
	node, err := NewNode(name, nil, cfg)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	if start {
		if !transport.RunningGoTest() {
			t.Fatal("test build flag not set: the node would dial real discovery")
		}
		if code := node.Start(); code != MOSS_OK {
			t.Fatalf("Start: %d", code)
		}
		t.Cleanup(func() { node.Stop() })
	}
	return node
}

// gameV2InjectPeer installs a direct peer over a fresh cipher-matched
// session, so the node's writes land in the returned carrier as ciphertext.
func gameV2InjectPeer(t *testing.T, node *Node, peerID string) (*capturingCarrier, *transport.Session) {
	t.Helper()
	carrier := newCapturingCarrier()
	t.Cleanup(func() { _ = carrier.Close() })
	sess := mustCipherSession(carrier)
	t.Cleanup(func() { _ = sess.Close() })
	node.mu.Lock()
	node.peers[peerID] = &peerConn{id: peerID, session: sess, outbound: true, connectedAt: time.Now()}
	node.mu.Unlock()
	return carrier, sess
}

// gameV2Profile is the standard v2 profile: 20 Hz ticks on the reserved
// game streams, a 2 KiB snapshot budget, AOI with the given radius.
func gameV2Profile(radius float64) GameProfile {
	return GameProfile{
		TickRateHz:        20,
		StateStreamID:     GameStateStreamID,
		InputStreamID:     GameInputStreamID,
		SnapshotMaxBytes:  2048,
		InterestRadius:    radius,
		AreaChannelPrefix: "room:area:",
	}
}

// gameV2WaitTrue polls cond until it reports true or a 5s deadline passes.
// Receive-side delivery rides the node's dispatch loop, so counter and
// channel effects settle asynchronously.
func gameV2WaitTrue(cond func() bool) bool {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return cond()
}

// gameV2FarEnd opens a node session's ciphertext through a cipher-matched
// far session in nonce lockstep: every ciphertext the carrier records is
// fed and read in write order, so the Nth nextEnvelope opens the Nth write.
// One far end per carrier; a replaced session (fresh carrier) needs a
// fresh far end.
type gameV2FarEnd struct {
	carrier *capturingCarrier
	farEnd  *capturingCarrier
	sess    *transport.Session
}

func newGameV2FarEnd(t *testing.T, carrier *capturingCarrier) *gameV2FarEnd {
	t.Helper()
	f := &gameV2FarEnd{carrier: carrier, farEnd: newCapturingCarrier()}
	f.sess = cipherMatchedSession(t, f.farEnd)
	return f
}

// nextEnvelope feeds the carrier's most recent ciphertext and returns the
// decrypted envelope. Call once per send, in send order.
func (f *gameV2FarEnd) nextEnvelope(t *testing.T) gossip.Envelope {
	t.Helper()
	f.farEnd.reads <- f.carrier.lastWrite()
	plain, err := f.sess.ReadPacket()
	if err != nil {
		t.Fatalf("far-end read failed: %v", err)
	}
	var env gossip.Envelope
	if err := json.Unmarshal(plain, &env); err != nil {
		t.Fatalf("far-end plaintext is not an envelope: %v", err)
	}
	if env.Type != gossip.TypeDirect {
		t.Fatalf("expected a %s envelope on the wire, got %s", gossip.TypeDirect, env.Type)
	}
	return env
}

// (a) The delta codec's wire contract: sizes by axis count, merged fields
// keep the base's values for unmasked axes, and change detection is by
// float bits — +0.0 vs -0.0 is a change, identical NaN payloads are not.
func TestEncodeSnapshotDeltaRoundTrip(t *testing.T) {
	base := GameSnapshot{EntityID: 7, X: 1.5, Y: -2.5, Z: 3.25, Seq: 1}

	// Identical snapshot: a zero-axis delta of the base size.
	noChange := EncodeSnapshotDelta(base, GameSnapshot{EntityID: 7, X: 1.5, Y: -2.5, Z: 3.25, Seq: 2})
	if len(noChange) != snapshotDeltaBaseSize {
		t.Fatalf("no-change delta is %d bytes, want %d", len(noChange), snapshotDeltaBaseSize)
	}
	if noChange[0] != snapshotDeltaVersion || noChange[1] != 0 {
		t.Fatalf("no-change delta header is %#x %#x, want version %d mask 0", noChange[0], noChange[1], snapshotDeltaVersion)
	}
	merged, err := DecodeSnapshotDelta(base, noChange)
	if err != nil {
		t.Fatalf("no-change delta failed to decode: %v", err)
	}
	if merged.EntityID != 7 || merged.Seq != 2 {
		t.Fatalf("no-change delta lost identity: %+v", merged)
	}
	if math.Float64bits(merged.X) != math.Float64bits(base.X) ||
		math.Float64bits(merged.Y) != math.Float64bits(base.Y) ||
		math.Float64bits(merged.Z) != math.Float64bits(base.Z) {
		t.Fatalf("no-change delta moved the entity: %+v", merged)
	}

	// One-axis delta: 22 bytes, Y and Z keep the base's bits.
	one := GameSnapshot{EntityID: 7, X: 9.75, Y: -2.5, Z: 3.25, Seq: 3}
	frame := EncodeSnapshotDelta(base, one)
	if len(frame) != snapshotDeltaBaseSize+8 {
		t.Fatalf("one-axis delta is %d bytes, want %d", len(frame), snapshotDeltaBaseSize+8)
	}
	if frame[1] != snapshotDeltaX {
		t.Fatalf("one-axis delta mask is %#x, want X %#x", frame[1], snapshotDeltaX)
	}
	merged, err = DecodeSnapshotDelta(base, frame)
	if err != nil {
		t.Fatalf("one-axis delta failed to decode: %v", err)
	}
	if merged.Seq != one.Seq || merged.EntityID != one.EntityID {
		t.Fatalf("one-axis delta identity mismatch: %+v", merged)
	}
	if math.Float64bits(merged.X) != math.Float64bits(one.X) {
		t.Fatalf("masked X not applied: %016x, want %016x", math.Float64bits(merged.X), math.Float64bits(one.X))
	}
	if math.Float64bits(merged.Y) != math.Float64bits(base.Y) || math.Float64bits(merged.Z) != math.Float64bits(base.Z) {
		t.Fatalf("unmasked axes drifted: %+v", merged)
	}

	// Two-axis delta: 30 bytes, fields in X, Y, Z order.
	two := GameSnapshot{EntityID: 7, X: 1.5, Y: 8.5, Z: -4.25, Seq: 4}
	frame = EncodeSnapshotDelta(base, two)
	if len(frame) != snapshotDeltaBaseSize+16 {
		t.Fatalf("two-axis delta is %d bytes, want %d", len(frame), snapshotDeltaBaseSize+16)
	}
	if frame[1] != snapshotDeltaY|snapshotDeltaZ {
		t.Fatalf("two-axis delta mask is %#x, want Y|Z %#x", frame[1], snapshotDeltaY|snapshotDeltaZ)
	}
	merged, err = DecodeSnapshotDelta(base, frame)
	if err != nil {
		t.Fatalf("two-axis delta failed to decode: %v", err)
	}
	if math.Float64bits(merged.X) != math.Float64bits(base.X) ||
		math.Float64bits(merged.Y) != math.Float64bits(two.Y) ||
		math.Float64bits(merged.Z) != math.Float64bits(two.Z) {
		t.Fatalf("two-axis merge wrong: %+v", merged)
	}

	// Three-axis delta: 38 bytes — bigger than the full form; the encoding
	// is still well-formed, the size threshold is the sender's.
	three := GameSnapshot{EntityID: 7, X: 1, Y: 2, Z: 4, Seq: 5}
	frame = EncodeSnapshotDelta(base, three)
	if len(frame) != snapshotDeltaBaseSize+24 {
		t.Fatalf("three-axis delta is %d bytes, want %d", len(frame), snapshotDeltaBaseSize+24)
	}
	if frame[1] != snapshotDeltaX|snapshotDeltaY|snapshotDeltaZ {
		t.Fatalf("three-axis delta mask is %#x", frame[1])
	}
	merged, err = DecodeSnapshotDelta(base, frame)
	if err != nil {
		t.Fatalf("three-axis delta failed to decode: %v", err)
	}
	if merged.EntityID != three.EntityID || merged.Seq != three.Seq ||
		math.Float64bits(merged.X) != math.Float64bits(three.X) ||
		math.Float64bits(merged.Y) != math.Float64bits(three.Y) ||
		math.Float64bits(merged.Z) != math.Float64bits(three.Z) {
		t.Fatalf("three-axis merge wrong: %+v", merged)
	}

	// +0.0 vs -0.0: == calls them equal, the bits do not — the mask must
	// carry the axis.
	negZero := GameSnapshot{EntityID: 7, X: math.Float64frombits(1 << 63), Y: -2.5, Z: 3.25, Seq: 6}
	frame = EncodeSnapshotDelta(base, negZero)
	if frame[1] != snapshotDeltaX {
		t.Fatalf("-0.0 vs +0.0 did not set the X mask: %#x", frame[1])
	}
	merged, err = DecodeSnapshotDelta(base, frame)
	if err != nil {
		t.Fatalf("-0.0 delta failed to decode: %v", err)
	}
	if math.Float64bits(merged.X) != 1<<63 {
		t.Fatalf("-0.0 did not survive: %016x", math.Float64bits(merged.X))
	}

	// NaN: identical bit payloads are not a change.
	nanBits := math.Float64bits(math.NaN())
	sameNaN := GameSnapshot{
		EntityID: 7,
		X:        math.Float64frombits(nanBits),
		Y:        math.Float64frombits(nanBits),
		Z:        math.Float64frombits(nanBits),
		Seq:      7,
	}
	baseNaN := GameSnapshot{EntityID: 7, X: math.Float64frombits(nanBits), Y: 1, Z: 1, Seq: 1}
	frame = EncodeSnapshotDelta(baseNaN, sameNaN)
	if frame[1] != snapshotDeltaY|snapshotDeltaZ {
		t.Fatalf("identical NaN bits set the X mask: %#x", frame[1])
	}
}

// (c) Decode-side validation: DecodeSnapshot reports deltas through the
// sentinel, and DecodeSnapshotDelta rejects short frames, foreign versions,
// unknown mask bits, and entity mismatches — but tolerates trailing bytes.
func TestDecodeSnapshotDeltaValidation(t *testing.T) {
	base := GameSnapshot{EntityID: 9, X: 1, Y: 2, Z: 3, Seq: 1}

	// DecodeSnapshot hands deltas back through the sentinel.
	frame := EncodeSnapshotDelta(base, GameSnapshot{EntityID: 9, X: 10, Y: 2, Z: 3, Seq: 2})
	if _, err := DecodeSnapshot(frame); !errors.Is(err, ErrSnapshotDeltaNeedsBase) {
		t.Fatalf("DecodeSnapshot(delta) = %v, want ErrSnapshotDeltaNeedsBase", err)
	}

	// Too short even for the fixed delta header.
	if _, err := DecodeSnapshotDelta(base, nil); err == nil {
		t.Fatal("empty delta decoded without error")
	}
	if _, err := DecodeSnapshotDelta(base, []byte{snapshotDeltaVersion}); err == nil {
		t.Fatal("one-byte delta decoded without error")
	}

	// Foreign version byte.
	foreign := append([]byte{1}, frame[1:]...)
	if _, err := DecodeSnapshotDelta(base, foreign); err == nil {
		t.Fatal("wrong-version delta decoded without error")
	}

	// Unknown mask bits.
	badMask := append([]byte(nil), frame[:2]...)
	badMask[1] |= 1 << 3
	badMask = append(badMask, frame[2:]...)
	if _, err := DecodeSnapshotDelta(base, badMask); err == nil {
		t.Fatal("unknown mask bits decoded without error")
	}

	// Truncated payload for the declared mask.
	short := append([]byte(nil), frame[:len(frame)-4]...)
	if _, err := DecodeSnapshotDelta(base, short); err == nil {
		t.Fatal("truncated delta decoded without error")
	}

	// Entity mismatch: the delta names a different entity than the base.
	other := EncodeSnapshotDelta(base, GameSnapshot{EntityID: 10, X: 1, Y: 2, Z: 3, Seq: 3})
	if _, err := DecodeSnapshotDelta(base, other); err == nil {
		t.Fatal("entity-mismatched delta decoded without error")
	}

	// Trailing bytes after the last masked axis are extension space.
	extended := append(append([]byte(nil), frame...), 0xAA, 0xBB)
	merged, err := DecodeSnapshotDelta(base, extended)
	if err != nil {
		t.Fatalf("extended delta failed to decode: %v", err)
	}
	if merged.EntityID != 9 || merged.Seq != 2 {
		t.Fatalf("extended delta identity wrong: %+v", merged)
	}
	if math.Float64bits(merged.X) != math.Float64bits(10) {
		t.Fatalf("extended delta X wrong: %+v", merged)
	}
}

// (b) The delta send path: the first send of an entity rides the full form;
// a smaller delta replaces it once a base exists; a three-axis change falls
// back to the full form by the size threshold; bases are per entity; and a
// failed send leaves the base intact so the retry re-deltas from the same
// state. The node is deliberately unstarted: the synchronous send path
// makes both the wire form and the failure observable the moment the call
// returns.
func TestSendSnapshotDeltaThresholdAndFallback(t *testing.T) {
	node := gameV2Node(t, "mesh-game-v2-delta", false)
	peerID := "peer-delta"
	carrier, sess := gameV2InjectPeer(t, node, peerID)
	far := newGameV2FarEnd(t, carrier)

	// First send of the entity: the full 37-byte form, no base yet.
	first := GameSnapshot{EntityID: 5, X: 1, Y: 2, Z: 3, Seq: 1}
	if err := node.SendSnapshotDelta(peerID, first, time.Second); err != nil {
		t.Fatalf("first SendSnapshotDelta failed: %v", err)
	}
	if got := directCaptureCount(carrier); got != 1 {
		t.Fatalf("after first send: %d carrier writes, want 1", got)
	}
	if env := far.nextEnvelope(t); !bytes.Equal(env.Payload, EncodeSnapshot(first)) {
		t.Fatalf("first send is not the full form: %x, want %x", env.Payload, EncodeSnapshot(first))
	}

	// One-axis change: a 22-byte delta on the wire, decodable against the
	// first send.
	oneAxis := GameSnapshot{EntityID: 5, X: 10.5, Y: 2, Z: 3, Seq: 2}
	if err := node.SendSnapshotDelta(peerID, oneAxis, time.Second); err != nil {
		t.Fatalf("second SendSnapshotDelta failed: %v", err)
	}
	env := far.nextEnvelope(t)
	if len(env.Payload) != snapshotDeltaBaseSize+8 {
		t.Fatalf("one-axis delta payload is %d bytes, want %d", len(env.Payload), snapshotDeltaBaseSize+8)
	}
	if env.Payload[0] != snapshotDeltaVersion || env.Payload[1] != snapshotDeltaX {
		t.Fatalf("delta header is %#x %#x", env.Payload[0], env.Payload[1])
	}
	merged, err := DecodeSnapshotDelta(first, env.Payload)
	if err != nil {
		t.Fatalf("wire delta failed to decode against the first snapshot: %v", err)
	}
	if merged.EntityID != 5 || merged.Seq != 2 {
		t.Fatalf("wire delta identity wrong: %+v", merged)
	}
	if math.Float64bits(merged.X) != math.Float64bits(10.5) ||
		math.Float64bits(merged.Y) != math.Float64bits(2) ||
		math.Float64bits(merged.Z) != math.Float64bits(3) {
		t.Fatalf("wire delta merged wrong: %+v", merged)
	}

	// Three-axis change: 38 bytes of delta loses to the 37-byte full form.
	threeAxis := GameSnapshot{EntityID: 5, X: 20, Y: 21, Z: 22, Seq: 3}
	if err := node.SendSnapshotDelta(peerID, threeAxis, time.Second); err != nil {
		t.Fatalf("three-axis SendSnapshotDelta failed: %v", err)
	}
	if env := far.nextEnvelope(t); !bytes.Equal(env.Payload, EncodeSnapshot(threeAxis)) {
		t.Fatalf("three-axis change did not fall back to the full form: %x, want %x", env.Payload, EncodeSnapshot(threeAxis))
	}

	// A new entity has no base: the full form, without disturbing entity
	// 5's base.
	other := GameSnapshot{EntityID: 6, X: 1, Y: 2, Z: 3, Seq: 1}
	if err := node.SendSnapshotDelta(peerID, other, time.Second); err != nil {
		t.Fatalf("new-entity SendSnapshotDelta failed: %v", err)
	}
	if env := far.nextEnvelope(t); !bytes.Equal(env.Payload, EncodeSnapshot(other)) {
		t.Fatalf("new-entity send is not the full form: %x, want %x", env.Payload, EncodeSnapshot(other))
	}

	// Unknown peer: SendToPeer's relay fallback, an error with no relay
	// candidates. The bases for peerID stay untouched.
	if err := node.SendSnapshotDelta("peer-absent", first, 100*time.Millisecond); err == nil {
		t.Fatal("expected an error for a target with neither a session nor a relay candidate")
	}

	// A failed send must not advance the base: close the session, send a
	// one-axis change (a 22-byte delta against the intact base), observe
	// the failure, then swap in a fresh session over a fresh carrier — a
	// new session restarts the nonce, so the far end is fresh too — and
	// re-send the same snapshot: the wire must still carry the 22-byte
	// X-mask delta, not a 14-byte no-change frame against a base the
	// failed send would have wrongly advanced.
	if err := sess.Close(); err != nil {
		t.Fatalf("closing the first session: %v", err)
	}
	changed := GameSnapshot{EntityID: 5, X: 30.5, Y: 21, Z: 22, Seq: 4}
	err = node.SendSnapshotDelta(peerID, changed, time.Second)
	if err == nil {
		t.Fatal("expected the closed-session send to fail")
	} else if err.Error() != "direct send failed" {
		t.Fatalf("closed-session error is %q, want %q", err.Error(), "direct send failed")
	}
	carrier2, _ := gameV2InjectPeer(t, node, peerID)
	far2 := newGameV2FarEnd(t, carrier2)
	if err := node.SendSnapshotDelta(peerID, changed, time.Second); err != nil {
		t.Fatalf("retry after failed send failed: %v", err)
	}
	env = far2.nextEnvelope(t)
	if len(env.Payload) != snapshotDeltaBaseSize+8 {
		t.Fatalf("retry payload is %d bytes, want the %d-byte one-axis delta — the failed send advanced the base",
			len(env.Payload), snapshotDeltaBaseSize+8)
	}
	if env.Payload[1] != snapshotDeltaX {
		t.Fatalf("retry delta mask is %#x, want X — the failed send advanced the base", env.Payload[1])
	}
}

// (d) The receive path: the OnSnapshot wrapper merges deltas against the
// per-sender, per-entity base, counts baseless deltas, stale-filters merged
// snapshots without refreshing the base, and chains other payloads to the
// previous callback. The node is started: packetCB delivery rides the
// dispatch loop.
func TestOnSnapshotMergesDeltaFrames(t *testing.T) {
	node := gameV2Node(t, "mesh-game-v2-merge", true)

	prevPackets := make(chan []byte, 8)
	node.SetPacketCallback(func(senderID [32]byte, data []byte) {
		prevPackets <- append([]byte(nil), data...)
	})

	snaps := make(chan gameSnapMsg, 16)
	stats := node.OnSnapshot(func(senderID [32]byte, snap GameSnapshot) {
		snaps <- gameSnapMsg{sender: senderID, snap: snap}
	})

	var sender [32]byte
	sender[0] = 0xCC
	peer := &peerConn{id: "peer-merge"}

	expectSnap := func(want GameSnapshot) {
		t.Helper()
		select {
		case got := <-snaps:
			if got.sender != sender {
				t.Fatalf("snapshot sender mismatch: %#x, want %#x", got.sender[0], sender[0])
			}
			if got.snap.EntityID != want.EntityID || got.snap.Seq != want.Seq {
				t.Fatalf("snapshot identity mismatch: %+v, want %+v", got.snap, want)
			}
			if math.Float64bits(got.snap.X) != math.Float64bits(want.X) ||
				math.Float64bits(got.snap.Y) != math.Float64bits(want.Y) ||
				math.Float64bits(got.snap.Z) != math.Float64bits(want.Z) {
				t.Fatalf("snapshot position mismatch: %+v, want %+v", got.snap, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for snapshot delivery")
		}
	}

	// A baseless delta is dropped and counted, not delivered and not
	// forwarded.
	baselessBefore := counterValue(node, snapshotDeltaBaselessCounter)
	orphan := EncodeSnapshotDelta(GameSnapshot{EntityID: 3, X: 1, Y: 1, Z: 1, Seq: 1}, GameSnapshot{EntityID: 3, X: 2, Y: 1, Z: 1, Seq: 2})
	feedGamePacket(node, peer, sender, orphan)
	if !gameV2WaitTrue(func() bool {
		return counterValue(node, snapshotDeltaBaselessCounter) == baselessBefore+1
	}) {
		t.Fatalf("baseless delta not counted: %d", counterValue(node, snapshotDeltaBaselessCounter))
	}
	select {
	case got := <-snaps:
		t.Fatalf("baseless delta was delivered: %+v", got.snap)
	default:
	}

	// A full snapshot establishes the base and is delivered.
	full := GameSnapshot{EntityID: 3, X: 1.5, Y: 2.5, Z: 3.5, Seq: 5}
	feedGamePacket(node, peer, sender, EncodeSnapshot(full))
	expectSnap(full)

	// A delta against it merges and is delivered with the base's unmasked
	// axes.
	deltaSnap := GameSnapshot{EntityID: 3, X: 9.5, Y: 2.5, Z: 3.5, Seq: 6}
	feedGamePacket(node, peer, sender, EncodeSnapshotDelta(full, deltaSnap))
	expectSnap(deltaSnap)

	// A stale delta (seq no newer than the last accepted) is dropped,
	// counted once in the stats and once in the stale counter, and does
	// NOT refresh the base.
	staleBefore := stats.StaleDrops()
	stale := EncodeSnapshotDelta(deltaSnap, GameSnapshot{EntityID: 3, X: 99, Y: 99, Z: 99, Seq: 6})
	feedGamePacket(node, peer, sender, stale)
	if !gameV2WaitTrue(func() bool {
		return stats.StaleDrops() == staleBefore+1
	}) {
		t.Fatalf("stale delta not counted in stats: %d", stats.StaleDrops())
	}
	if got := counterValue(node, snapshotStaleCounter); got != 1 {
		t.Fatalf("stale counter is %d, want 1", got)
	}
	// The base was not refreshed by the stale frame: the next valid delta
	// still merges against the accepted state, not the stale frame's
	// poisoned values.
	afterStale := GameSnapshot{EntityID: 3, X: 9.5, Y: 2.5, Z: -7.25, Seq: 7}
	feedGamePacket(node, peer, sender, EncodeSnapshotDelta(deltaSnap, afterStale))
	expectSnap(afterStale)

	// Alternation: a full snapshot after deltas re-establishes the base,
	// and a delta against the new full merges cleanly.
	full2 := GameSnapshot{EntityID: 3, X: 0.5, Y: 0.25, Z: 0.125, Seq: 8}
	feedGamePacket(node, peer, sender, EncodeSnapshot(full2))
	expectSnap(full2)
	mix := GameSnapshot{EntityID: 3, X: 0.5, Y: 8.25, Z: 0.125, Seq: 9}
	feedGamePacket(node, peer, sender, EncodeSnapshotDelta(full2, mix))
	expectSnap(mix)

	// A delta naming an entity with no base for this sender is baseless.
	otherEntity := EncodeSnapshotDelta(full2, GameSnapshot{EntityID: 4, X: 1, Y: 1, Z: 1, Seq: 10})
	feedGamePacket(node, peer, sender, otherEntity)
	if !gameV2WaitTrue(func() bool {
		return counterValue(node, snapshotDeltaBaselessCounter) == baselessBefore+2
	}) {
		t.Fatalf("other-entity delta not counted baseless: %d", counterValue(node, snapshotDeltaBaselessCounter))
	}

	// Unrelated directed payloads keep flowing to the previous callback.
	feedGamePacket(node, peer, sender, []byte("not-a-snapshot"))
	select {
	case got := <-prevPackets:
		if string(got) != "not-a-snapshot" {
			t.Fatalf("chained payload wrong: %q", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for chained payload delivery")
	}
}

// (e) AOI culling on the directed send path: with a positive InterestRadius
// applied, a target whose last known position is outside the radius is
// silently dropped and counted (both SendSnapshot and SendSnapshotDelta);
// inside the radius and exactly on it the send proceeds; an unknown
// position fails open; a zero radius disables culling. The node is started
// because the target's position arrives through the OnSnapshot receive
// path; the heartbeat is stretched so the maintenance ping cannot race
// the carrier's write count.
func TestSendSnapshotAOICulling(t *testing.T) {
	node := gameV2Node(t, "mesh-game-v2-aoi", true)
	if code := ApplyGameProfile(node, gameV2Profile(5)); code != MOSS_OK {
		t.Fatalf("ApplyGameProfile: %d", code)
	}

	snaps := make(chan gameSnapMsg, 16)
	node.OnSnapshot(func(senderID [32]byte, snap GameSnapshot) {
		snaps <- gameSnapMsg{sender: senderID, snap: snap}
	})

	// The far peer's session, keyed by the sender's hex ID so the position
	// learned from its snapshots feeds the AOI oracle for sends TO it.
	var sender [32]byte
	sender[0] = 0xEE
	peerID := hex.EncodeToString(sender[:])
	carrier, _ := gameV2InjectPeer(t, node, peerID)
	peer := &peerConn{id: "peer-aoi"}

	// Learn the target's position: (100, 0, 0), radius 5.
	target := GameSnapshot{EntityID: 1, X: 100, Y: 0, Z: 0, Seq: 1}
	feedGamePacket(node, peer, sender, EncodeSnapshot(target))
	select {
	case <-snaps:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the target's position snapshot")
	}

	// Outside: the entity is at the origin, the target at 100 — cull,
	// before the wire is ever touched.
	far := GameSnapshot{EntityID: 2, X: 0, Y: 0, Z: 0, Seq: 1}
	culledBefore := counterValue(node, snapshotAoiCulledCounter)
	if err := node.SendSnapshot(peerID, far, time.Second); err != nil {
		t.Fatalf("SendSnapshot outside radius failed: %v", err)
	}
	if got := counterValue(node, snapshotAoiCulledCounter); got != culledBefore+1 {
		t.Fatalf("outside-radius send not culled: counter %d, want %d", got, culledBefore+1)
	}
	if got := directCaptureCount(carrier); got != 0 {
		t.Fatalf("culled send reached the wire: %d carrier writes", got)
	}
	// SendSnapshotDelta culls identically.
	if err := node.SendSnapshotDelta(peerID, far, time.Second); err != nil {
		t.Fatalf("SendSnapshotDelta outside radius failed: %v", err)
	}
	if got := counterValue(node, snapshotAoiCulledCounter); got != culledBefore+2 {
		t.Fatalf("SendSnapshotDelta outside radius not culled: counter %d, want %d", got, culledBefore+2)
	}
	if got := directCaptureCount(carrier); got != 0 {
		t.Fatalf("culled delta send reached the wire: %d carrier writes", got)
	}

	// Inside: distance 2 ≤ 5 — the send reaches the wire. The node is
	// started, so the write settles asynchronously.
	near := GameSnapshot{EntityID: 2, X: 98, Y: 0, Z: 0, Seq: 2}
	if err := node.SendSnapshot(peerID, near, time.Second); err != nil {
		t.Fatalf("SendSnapshot inside radius failed: %v", err)
	}
	if !gameV2WaitTrue(func() bool {
		return directCaptureCount(carrier) == 1
	}) {
		t.Fatalf("inside-radius send did not reach the wire: %d writes", directCaptureCount(carrier))
	}

	// Exactly on the radius is inside: culling is strictly-greater.
	edge := GameSnapshot{EntityID: 2, X: 95, Y: 0, Z: 0, Seq: 3}
	if err := node.SendSnapshot(peerID, edge, time.Second); err != nil {
		t.Fatalf("SendSnapshot on the radius failed: %v", err)
	}
	if !gameV2WaitTrue(func() bool {
		return directCaptureCount(carrier) == 2
	}) {
		t.Fatalf("on-radius send was culled: %d writes", directCaptureCount(carrier))
	}
	if got := counterValue(node, snapshotAoiCulledCounter); got != culledBefore+2 {
		t.Fatalf("on-radius send was culled: counter %d, want %d", got, culledBefore+2)
	}

	// Unknown position fails open: the send proceeds down the relay path
	// (which has no candidates here — the error, not a silent nil, is the
	// proof the cull did not fire).
	if err := node.SendSnapshot("peer-unknown", far, time.Second); err == nil {
		t.Fatal("unknown-position send was silently dropped instead of failing on the relay path")
	}
	if got := counterValue(node, snapshotAoiCulledCounter); got != culledBefore+2 {
		t.Fatalf("unknown-position send was culled: counter %d, want %d", got, culledBefore+2)
	}

	// A zero-radius profile disables culling entirely: the far send goes
	// through.
	if code := ApplyGameProfile(node, gameV2Profile(0)); code != MOSS_OK {
		t.Fatalf("ApplyGameProfile(0): %d", code)
	}
	farSeq4 := GameSnapshot{EntityID: 2, X: 0, Y: 0, Z: 0, Seq: 4}
	if err := node.SendSnapshot(peerID, farSeq4, time.Second); err != nil {
		t.Fatalf("SendSnapshot with no radius failed: %v", err)
	}
	if !gameV2WaitTrue(func() bool {
		return directCaptureCount(carrier) == 3
	}) {
		t.Fatalf("no-radius send did not reach the wire: %d writes", directCaptureCount(carrier))
	}
	if got := counterValue(node, snapshotAoiCulledCounter); got != culledBefore+2 {
		t.Fatalf("no-radius send was culled: counter %d, want %d", got, culledBefore+2)
	}
}

// (f) The Predictor hook is storage only: set, retrieve, clear — and the
// transport never calls it across a profile apply and a full send/receive
// cycle. The node is started so the receive half rides the dispatch loop.
func TestSetGamePredictorHook(t *testing.T) {
	node := gameV2Node(t, "mesh-game-v2-predictor", true)

	if got := node.GamePredictor(); got != nil {
		t.Fatalf("fresh node has a predictor: %#v", got)
	}

	pred := &countingPredictor{}
	node.SetGamePredictor(pred)
	if got := node.GamePredictor(); got != pred {
		t.Fatalf("GamePredictor did not return the set predictor: %#v", got)
	}

	if code := ApplyGameProfile(node, gameV2Profile(0)); code != MOSS_OK {
		t.Fatalf("ApplyGameProfile: %d", code)
	}

	// Send side with the predictor set: a full then a delta send.
	peerID := "peer-predictor"
	carrier, _ := gameV2InjectPeer(t, node, peerID)
	first := GameSnapshot{EntityID: 1, X: 1, Y: 2, Z: 3, Seq: 1}
	if err := node.SendSnapshotDelta(peerID, first, time.Second); err != nil {
		t.Fatalf("SendSnapshotDelta failed: %v", err)
	}
	second := GameSnapshot{EntityID: 1, X: 4, Y: 2, Z: 3, Seq: 2}
	if err := node.SendSnapshotDelta(peerID, second, time.Second); err != nil {
		t.Fatalf("SendSnapshotDelta failed: %v", err)
	}
	if !gameV2WaitTrue(func() bool {
		return directCaptureCount(carrier) == 2
	}) {
		t.Fatalf("expected 2 carrier writes, got %d", directCaptureCount(carrier))
	}

	// Receive side with the predictor set: the wrapper delivers a full
	// and a merged delta snapshot.
	snaps := make(chan gameSnapMsg, 4)
	node.OnSnapshot(func(senderID [32]byte, snap GameSnapshot) {
		snaps <- gameSnapMsg{sender: senderID, snap: snap}
	})
	var sender [32]byte
	sender[0] = 0xDD
	peer := &peerConn{id: "peer-pred-recv"}
	feedGamePacket(node, peer, sender, EncodeSnapshot(first))
	feedGamePacket(node, peer, sender, EncodeSnapshotDelta(first, second))
	for i, want := range []GameSnapshot{first, second} {
		select {
		case got := <-snaps:
			if got.snap.Seq != want.Seq || math.Float64bits(got.snap.X) != math.Float64bits(want.X) {
				t.Fatalf("received snapshot %d wrong: %+v, want %+v", i, got.snap, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for received snapshot %d", i)
		}
	}

	if got := pred.Calls(); got != 0 {
		t.Fatalf("transport invoked the predictor %d times, want 0", got)
	}

	// Nil clears the hook.
	node.SetGamePredictor(nil)
	if got := node.GamePredictor(); got != nil {
		t.Fatalf("cleared predictor is not nil: %#v", got)
	}
}

// countingPredictor counts Predict calls for the hook test.
type countingPredictor struct {
	calls atomic.Uint64
}

func (p *countingPredictor) Predict(entityID uint64, dt time.Duration) {
	p.calls.Add(1)
}

func (p *countingPredictor) Calls() uint64 {
	return p.calls.Load()
}

// The delta frame's entityID/seq header is little-endian: pin it so a
// big-endian port cannot decode a v2 frame silently wrong.
func TestSnapshotDeltaWireLayoutIsLittleEndian(t *testing.T) {
	base := GameSnapshot{EntityID: 0x0102030405060708, X: 1, Y: 2, Z: 3, Seq: 0x11223344}
	frame := EncodeSnapshotDelta(base, GameSnapshot{EntityID: base.EntityID, X: 4, Y: 2, Z: 3, Seq: base.Seq})
	if frame[2] != 0x08 || frame[9] != 0x01 {
		t.Fatalf("entityID not little-endian: %#x %#x", frame[2], frame[9])
	}
	if frame[10] != 0x44 || frame[13] != 0x11 {
		t.Fatalf("seq not little-endian: %#x %#x", frame[10], frame[13])
	}
	if len(frame) != 22 {
		t.Fatalf("one-axis frame is %d bytes, want 22", len(frame))
	}
	if bits := binary.LittleEndian.Uint64(frame[14:22]); bits != math.Float64bits(4) {
		t.Fatalf("x bits not little-endian: %#x", bits)
	}
}
