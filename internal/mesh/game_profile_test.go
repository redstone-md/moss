package mesh

import (
	"bytes"
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/transport"
)

// --- codec ---

func TestSnapshotCodecRoundTrip(t *testing.T) {
	snaps := []GameSnapshot{
		{EntityID: 0, X: 0, Y: 0, Z: 0, Seq: 0},
		{EntityID: 1 << 63, X: -1.5, Y: 2.25, Z: -3.75, Seq: 1},
		{EntityID: 18446744073709551615, X: 1e-300, Y: -1e300, Z: 3.14159, Seq: 4294967295},
		{EntityID: 42, X: math.Inf(1), Y: math.Inf(-1), Z: 0.5, Seq: 7},
	}
	for _, snap := range snaps {
		enc := EncodeSnapshot(snap)
		if len(enc) != SnapshotWireSize {
			t.Fatalf("encoded snapshot is %d bytes, want %d", len(enc), SnapshotWireSize)
		}
		dec, err := DecodeSnapshot(enc)
		if err != nil {
			t.Fatalf("DecodeSnapshot(%+v): %v", snap, err)
		}
		if dec != snap {
			t.Fatalf("round-trip mismatch: %+v != %+v", dec, snap)
		}
	}
}

func TestSnapshotWireLayoutIsLittleEndian(t *testing.T) {
	enc := EncodeSnapshot(GameSnapshot{
		EntityID: 0x0102030405060708,
		X:        1.0,
		Y:        2.0,
		Z:        4.0,
		Seq:      0x11223344,
	})
	if enc[0] != 1 {
		t.Fatalf("version byte is %#x, want 1", enc[0])
	}
	if enc[1] != 0x08 || enc[8] != 0x01 {
		t.Fatalf("entityID not little-endian: %#x %#x", enc[1], enc[8])
	}
	if enc[9] != 0x00 || enc[16] != 0x3F {
		t.Fatalf("x (1.0) bits not little-endian: %#x %#x", enc[9], enc[16])
	}
	if enc[17] != 0x00 || enc[24] != 0x40 {
		t.Fatalf("y (2.0) bits not little-endian: %#x %#x", enc[17], enc[24])
	}
	if enc[25] != 0x00 || enc[32] != 0x40 {
		t.Fatalf("z (4.0) bits not little-endian: %#x %#x", enc[25], enc[32])
	}
	if enc[33] != 0x44 || enc[36] != 0x11 {
		t.Fatalf("seq not little-endian: %#x %#x", enc[33], enc[36])
	}
}

func TestDecodeSnapshotRejectsAndExtends(t *testing.T) {
	if _, err := DecodeSnapshot(nil); err == nil {
		t.Fatal("empty payload decoded without error")
	}
	if _, err := DecodeSnapshot(bytes.Repeat([]byte{1}, SnapshotWireSize-1)); err == nil {
		t.Fatal("short payload decoded without error")
	}
	wrongVersion := EncodeSnapshot(GameSnapshot{EntityID: 1, Seq: 1})
	wrongVersion[0] = 2
	if _, err := DecodeSnapshot(wrongVersion); err == nil {
		t.Fatal("unknown version decoded without error")
	}

	// Trailing bytes are extension space: the fixed header still decodes.
	snap := GameSnapshot{EntityID: 9, X: 1.5, Y: -2.5, Z: 3.25, Seq: 7}
	extended := append(EncodeSnapshot(snap), 1, 2, 3, 4, 5, 6, 7, 8)
	dec, err := DecodeSnapshot(extended)
	if err != nil {
		t.Fatalf("extended snapshot failed to decode: %v", err)
	}
	if dec != snap {
		t.Fatalf("extended round-trip mismatch: %+v != %+v", dec, snap)
	}
}

func TestAppendSnapshotReusesBuffer(t *testing.T) {
	first := GameSnapshot{EntityID: 1, X: 1, Y: 1, Z: 1, Seq: 1}
	second := GameSnapshot{EntityID: 2, X: 2, Y: 2, Z: 2, Seq: 2}

	dst := []byte("hdr:")
	out := AppendSnapshot(dst, first)
	if len(out) != len(dst)+SnapshotWireSize {
		t.Fatalf("append grew the buffer by %d, want %d", len(out)-len(dst), SnapshotWireSize)
	}
	if string(out[:4]) != "hdr:" {
		t.Fatal("append clobbered the existing prefix")
	}
	dec, err := DecodeSnapshot(out[4:])
	if err != nil || dec != first {
		t.Fatalf("decode after append: %v, %+v", err, dec)
	}

	// The send-loop pattern: reset to [:0] and the same backing array serves
	// the next tick with no new allocation.
	buf := make([]byte, 0, 64)
	buf = AppendSnapshot(buf, first)
	buf = AppendSnapshot(buf[:0], second)
	dec, err = DecodeSnapshot(buf)
	if err != nil || dec != second {
		t.Fatalf("in-place reuse decode: %v, %+v", err, dec)
	}
}

// --- profile ---

func TestDefaultGameProfileMatchesDocumentedDefaults(t *testing.T) {
	p := DefaultGameProfile()
	if p.TickRateHz != 20 || p.StateStreamID != 100 || p.InputStreamID != 101 ||
		p.SnapshotMaxBytes != 2048 || p.InterestRadius != 0 || p.AreaChannelPrefix != "room:area:" {
		t.Fatalf("documented defaults drifted: %+v", p)
	}

	// The JSON contract: snake_case keys, stable for config files.
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, key := range []string{
		"tick_rate_hz", "state_stream_id", "input_stream_id",
		"snapshot_max_bytes", "interest_radius", "area_channel_prefix",
	} {
		if _, ok := fields[key]; !ok {
			t.Fatalf("marshaled profile is missing key %q: %s", key, raw)
		}
	}
}

func TestApplyGameProfileValidation(t *testing.T) {
	valid := DefaultGameProfile()

	node := &Node{}
	if code := ApplyGameProfile(node, valid); code != MOSS_OK {
		t.Fatalf("valid profile rejected: %d", code)
	}
	node.streamMu.RLock()
	stateFlag := node.streamUnreliable[valid.StateStreamID]
	inputFlag := node.streamUnreliable[valid.InputStreamID]
	node.streamMu.RUnlock()
	if !stateFlag {
		t.Fatalf("state stream %d not marked latest-wins", valid.StateStreamID)
	}
	if inputFlag {
		t.Fatalf("input stream %d must keep the reliable policy", valid.InputStreamID)
	}

	// mod copies the valid profile and mutates one field: each invalid case
	// is a single-setting break of an otherwise valid preset.
	mod := func(f func(p *GameProfile)) GameProfile {
		p := valid
		f(&p)
		return p
	}
	invalid := map[string]GameProfile{
		"zero tick rate":             mod(func(p *GameProfile) { p.TickRateHz = 0 }),
		"negative tick rate":         mod(func(p *GameProfile) { p.TickRateHz = -1 }),
		"zero state stream":          mod(func(p *GameProfile) { p.StateStreamID = 0 }),
		"default state stream":       mod(func(p *GameProfile) { p.StateStreamID = transport.DefaultStream }),
		"zero input stream":          mod(func(p *GameProfile) { p.InputStreamID = 0 }),
		"default input stream":       mod(func(p *GameProfile) { p.InputStreamID = transport.DefaultStream }),
		"equal stream ids":           mod(func(p *GameProfile) { p.InputStreamID = p.StateStreamID }),
		"negative snapshot cap":      mod(func(p *GameProfile) { p.SnapshotMaxBytes = -1 }),
		"snapshot cap below header":  mod(func(p *GameProfile) { p.SnapshotMaxBytes = SnapshotWireSize - 1 }),
		"negative interest radius":   mod(func(p *GameProfile) { p.InterestRadius = -0.1 }),
		"aoi without channel prefix": mod(func(p *GameProfile) { p.InterestRadius = 5; p.AreaChannelPrefix = "" }),
	}
	for name, p := range invalid {
		if code := ApplyGameProfile(&Node{}, p); code != MOSS_ERR_CONFIG_INVALID {
			t.Fatalf("%s: code %d, want MOSS_ERR_CONFIG_INVALID", name, code)
		}
	}

	if code := ApplyGameProfile(nil, valid); code != MOSS_ERR_CONFIG_INVALID {
		t.Fatalf("nil node: code %d, want MOSS_ERR_CONFIG_INVALID", code)
	}
}

// TestApplyGameProfileMarksStateStreamLatestWins proves the preset's core
// transport effect end to end: after ApplyGameProfile, a state stream that
// nobody reads evicts its OLDEST ticks when full instead of refusing the
// new ones. The node is unstarted — the session's mux readLoop drains the
// carrier and enqueues; no dispatch loop is needed for stream delivery.
func TestApplyGameProfileMarksStateStreamLatestWins(t *testing.T) {
	rec := newRecordedSession(t)
	node := &Node{
		config: DefaultConfig(),
		peers: map[string]*peerConn{
			"direct": {id: "direct", session: rec.session},
		},
	}
	farEnd := newCapturingCarrier()
	farSess := cipherMatchedSession(t, farEnd)

	if code := ApplyGameProfile(node, DefaultGameProfile()); code != MOSS_OK {
		t.Fatalf("ApplyGameProfile: %d", code)
	}
	state := DefaultGameProfile().StateStreamID

	// The process-wide split is snapshotted BEFORE the feed: both
	// evictions land in other-stream drops alongside the per-stream
	// counter.
	_, otherBefore := transport.StreamDropsSplit()

	// Feed 2 more ticks than the default stream buffer (256) holds with NO
	// handler registered: nothing drains, the buffer fills, and the two
	// overflow ticks must evict the two OLDEST payloads (p0, p1).
	for i := range 258 {
		payload := []byte("p" + strconv.Itoa(i))
		if err := farSess.Stream(state).WritePacket(payload); err != nil {
			t.Fatalf("far-end write %d: %v", i, err)
		}
		rec.carrier.reads <- farEnd.lastWrite()
	}
	// The unbuffered carrier handoff returns once the readLoop has the
	// ciphertext, not once it has enqueued it — so the last evictions can
	// still be in flight. Wait for both to land; 2 is the deterministic
	// total (258 packets, 256 slots, latest-wins evicts exactly twice).
	stream := rec.session.Stream(state)
	settleDeadline := time.Now().Add(5 * time.Second)
	for stream.Drops() != 2 && time.Now().Before(settleDeadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if got := stream.Drops(); got != 2 {
		t.Fatalf("latest-wins stream recorded %d overflow drops, want exactly 2 (p0 and p1 evicted)", got)
	}

	// Registering the handler spawns the reader, which must drain the 256
	// surviving ticks in order, starting at p2.
	delivered := make(chan string, 300)
	if code := node.OnStream(state, func(peerID string, data []byte) {
		delivered <- string(data)
	}); code != MOSS_OK {
		t.Fatalf("OnStream: %d", code)
	}

	first := ""
	count := 0
	last := ""
	deadline := time.After(5 * time.Second)
	for count < 256 {
		select {
		case got := <-delivered:
			if count == 0 {
				first = got
			}
			last = got
			count++
		case <-deadline:
			t.Fatalf("reader delivered only %d of 256 buffered ticks; first=%q", count, first)
		}
	}
	if first != "p2" {
		t.Fatalf("latest-wins kept the wrong payload: first delivered is %q, want p2", first)
	}
	if last != "p257" {
		t.Fatalf("reader stopped early: last delivered is %q, want p257", last)
	}
	select {
	case got := <-delivered:
		t.Fatalf("reader delivered a 257th payload %q; the buffer only held 256", got)
	default:
	}

	// The evictions were counted process-wide under other-stream drops too.
	_, otherAfter := transport.StreamDropsSplit()
	if got := otherAfter - otherBefore; got < 2 {
		t.Fatalf("process-wide other-stream drop delta is %d, want >= 2", got)
	}
}

// --- snapshot receive path ---

// feedGamePacket hands one directed envelope to the node's receive half the
// way handleDirectPacket's real caller does.
func feedGamePacket(node *Node, peer *peerConn, sender [32]byte, payload []byte) {
	node.handleDirectPacket(peer, gossip.Envelope{
		Type:     gossip.TypeDirect,
		SenderID: sender[:],
		Payload:  payload,
	})
}

type gameSnapMsg struct {
	sender [32]byte
	snap   GameSnapshot
}

// TestOnSnapshotFiltersPerSenderAndChainsCallbacks pins the receive path:
// snapshots are sequence-filtered per sender and consumed by the snapshot
// sink; every other directed payload keeps flowing to the previously
// registered packet callback. The node is started: packetCB delivery rides
// the dispatch loop.
func TestOnSnapshotFiltersPerSenderAndChainsCallbacks(t *testing.T) {
	node, err := NewNode("mesh-game-snap", nil, isolatedTestConfig("game-snap"))
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

	prevPackets := make(chan []byte, 8)
	node.SetPacketCallback(func(senderID [32]byte, data []byte) {
		prevPackets <- append([]byte(nil), data...)
	})

	snaps := make(chan gameSnapMsg, 16)
	stats := node.OnSnapshot(func(senderID [32]byte, snap GameSnapshot) {
		snaps <- gameSnapMsg{sender: senderID, snap: snap}
	})

	var senderA, senderB [32]byte
	senderA[0], senderB[0] = 0xAA, 0xBB
	peer := &peerConn{id: "peer-game"}

	expectSnap := func(wantSender [32]byte, want GameSnapshot) {
		t.Helper()
		select {
		case got := <-snaps:
			if got.sender != wantSender {
				t.Fatalf("snapshot sender mismatch: %#x, want %#x", got.sender[0], wantSender[0])
			}
			if got.snap != want {
				t.Fatalf("snapshot mismatch: %+v, want %+v", got.snap, want)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("snapshot %+v was never delivered", want)
		}
	}
	expectPrev := func(want string) {
		t.Helper()
		select {
		case got := <-prevPackets:
			if string(got) != want {
				t.Fatalf("previous callback saw %q, want %q", got, want)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("previous callback never saw %q", want)
		}
	}

	// Non-snapshot directed traffic keeps flowing to the previous callback.
	feedGamePacket(node, peer, senderA, []byte("hello"))
	expectPrev("hello")

	// Fresh seq 5 from A is delivered with every field intact.
	fresh := GameSnapshot{EntityID: 7, X: 1.5, Y: -2.5, Z: 3.25, Seq: 5}
	feedGamePacket(node, peer, senderA, EncodeSnapshot(fresh))
	expectSnap(senderA, fresh)

	// Stale seq 3 and duplicate seq 5 from A are dropped — and the serial
	// dispatch loop guarantees both are processed before seq 6 arrives.
	feedGamePacket(node, peer, senderA, EncodeSnapshot(GameSnapshot{EntityID: 7, Seq: 3}))
	feedGamePacket(node, peer, senderA, EncodeSnapshot(GameSnapshot{EntityID: 7, Seq: 5}))

	six := GameSnapshot{EntityID: 7, X: 1.5, Y: -2.5, Z: 3.25, Seq: 6}
	feedGamePacket(node, peer, senderA, EncodeSnapshot(six))
	expectSnap(senderA, six)
	if got := stats.StaleDrops(); got != 2 {
		t.Fatalf("StaleDrops() = %d after one stale and one duplicate, want 2", got)
	}
	if got := counterValue(node, snapshotStaleCounter); got != 2 {
		t.Fatalf("__snapshot_stale__ counter = %d, want 2", got)
	}

	// Each sender is filtered independently: B's seq 1 is fresh for B.
	one := GameSnapshot{EntityID: 8, X: 0, Y: 0, Z: 0, Seq: 1}
	feedGamePacket(node, peer, senderB, EncodeSnapshot(one))
	expectSnap(senderB, one)

	// Extension space after the header rides the same path.
	ext := GameSnapshot{EntityID: 9, X: 10, Y: 20, Z: 30, Seq: 7}
	feedGamePacket(node, peer, senderA, append(EncodeSnapshot(ext), 1, 2, 3, 4, 5, 6, 7, 8))
	expectSnap(senderA, ext)

	// A long payload that does not start with the snapshot version byte is
	// ordinary directed traffic: the previous callback receives it.
	long := bytes.Repeat([]byte{'x'}, SnapshotWireSize+3)
	feedGamePacket(node, peer, senderA, long)
	expectPrev(string(long))

	// The previous callback saw ONLY the non-snapshot payloads.
	select {
	case got := <-prevPackets:
		t.Fatalf("previous callback saw %q: snapshots must never be forwarded", got)
	default:
	}

	// Re-registration is last-wins: the second OnSnapshot's sink takes over.
	snaps2 := make(chan gameSnapMsg, 4)
	stats2 := node.OnSnapshot(func(senderID [32]byte, snap GameSnapshot) {
		snaps2 <- gameSnapMsg{sender: senderID, snap: snap}
	})
	nine := GameSnapshot{EntityID: 7, X: 1.5, Y: -2.5, Z: 3.25, Seq: 9}
	feedGamePacket(node, peer, senderA, EncodeSnapshot(nine))
	select {
	case got := <-snaps2:
		if got.snap != nine {
			t.Fatalf("second registration saw %+v, want %+v", got.snap, nine)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("second OnSnapshot registration never received the snapshot")
	}
	select {
	case got := <-snaps:
		t.Fatalf("first registration still saw %+v after re-registration", got.snap)
	default:
	}
	if got := stats2.StaleDrops(); got != 0 {
		t.Fatalf("fresh registration already reports %d drops", got)
	}
	if got := stats.StaleDrops(); got != 2 {
		t.Fatalf("first registration's counter moved: %d, want 2", got)
	}
}

// TestOnSnapshotSeqWraparound pins the wraparound semantics: the per-sender
// filter compares int32 sequence distance, so a counter that runs past
// 0xFFFFFFFF is still "newer" for the next few million samples.
func TestOnSnapshotSeqWraparound(t *testing.T) {
	node, err := NewNode("mesh-game-wrap", nil, isolatedTestConfig("game-wrap"))
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

	snaps := make(chan GameSnapshot, 8)
	stats := node.OnSnapshot(func(senderID [32]byte, snap GameSnapshot) {
		snaps <- snap
	})

	var sender [32]byte
	sender[0] = 0xCC
	peer := &peerConn{id: "peer-wrap"}
	expect := func(want GameSnapshot) {
		t.Helper()
		select {
		case got := <-snaps:
			if got != want {
				t.Fatalf("delivered %+v, want %+v", got, want)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("snapshot %+v was never delivered", want)
		}
	}

	nearWrap := GameSnapshot{EntityID: 1, Seq: 0xFFFFFFFE}
	feedGamePacket(node, peer, sender, EncodeSnapshot(nearWrap))
	expect(nearWrap)

	// 2 is 4 past the wrap point: newer than 0xFFFFFFFE by int32 distance.
	wrapped := GameSnapshot{EntityID: 1, Seq: 2}
	feedGamePacket(node, peer, sender, EncodeSnapshot(wrapped))
	expect(wrapped)

	// Going back to 0xFFFFFFFE from 2 is a stale sample; so is a repeat of 2.
	feedGamePacket(node, peer, sender, EncodeSnapshot(nearWrap))
	feedGamePacket(node, peer, sender, EncodeSnapshot(wrapped))

	three := GameSnapshot{EntityID: 1, Seq: 3}
	feedGamePacket(node, peer, sender, EncodeSnapshot(three))
	expect(three)
	if got := stats.StaleDrops(); got != 2 {
		t.Fatalf("StaleDrops() = %d, want 2 (one stale, one duplicate across the wrap)", got)
	}
}

// --- peer selection ---

func TestBestPeerForTick(t *testing.T) {
	rec := newRecordedSession(t)
	direct := func(id string, rtt time.Duration) *peerConn {
		return &peerConn{id: id, session: rec.session, lastRTT: rtt}
	}

	cases := []struct {
		name  string
		peers map[string]*peerConn
		want  string
	}{
		{"no peers", nil, ""},
		{
			"relayed peers never carry ticks",
			map[string]*peerConn{"r": {id: "r", relayed: true, session: rec.session, lastRTT: time.Millisecond}},
			"",
		},
		{
			"sessionless peers are skipped",
			map[string]*peerConn{"a": {id: "a", lastRTT: 5 * time.Millisecond}},
			"",
		},
		{
			"unprobed fallback picks the smallest id",
			map[string]*peerConn{
				"b": direct("b", 0),
				"a": direct("a", 0),
			},
			"a",
		},
		{
			"a probed peer beats an unprobed one",
			map[string]*peerConn{
				"a": direct("a", 50*time.Millisecond),
				"b": direct("b", 0),
			},
			"a",
		},
		{
			"lowest rtt wins",
			map[string]*peerConn{
				"a": direct("a", 50*time.Millisecond),
				"b": direct("b", 20*time.Millisecond),
			},
			"b",
		},
		{
			"rtt ties break towards the smaller id",
			map[string]*peerConn{
				"b": direct("b", 20*time.Millisecond),
				"a": direct("a", 20*time.Millisecond),
			},
			"a",
		},
		{
			"a probed relayed peer never wins over a probed direct one",
			map[string]*peerConn{
				"r": {id: "r", relayed: true, session: rec.session, lastRTT: time.Millisecond},
				"a": direct("a", 50*time.Millisecond),
			},
			"a",
		},
	}
	for _, tc := range cases {
		node := &Node{peers: tc.peers}
		if got := node.BestPeerForTick(); got != tc.want {
			t.Fatalf("%s: BestPeerForTick() = %q, want %q", tc.name, got, tc.want)
		}
	}
}

// --- snapshot send path ---

// TestSendSnapshotRidesDirectedEnvelope pins the wire form: SendSnapshot is
// SendToPeer with an encoded snapshot — one TypeDirect envelope over the
// peer's session, relay untouched. The node is deliberately unstarted so the
// synchronous send path makes the write observable the moment it returns.
func TestSendSnapshotRidesDirectedEnvelope(t *testing.T) {
	node, err := NewNode("mesh-game-send", nil, isolatedTestConfig("game-send"))
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	carrier := newCapturingCarrier()
	t.Cleanup(func() { _ = carrier.Close() })
	sess := mustCipherSession(carrier)
	t.Cleanup(func() { _ = sess.Close() })
	node.mu.Lock()
	node.peers["peer-game"] = &peerConn{id: "peer-game", session: sess, outbound: true, connectedAt: time.Now()}
	node.mu.Unlock()

	snap := GameSnapshot{EntityID: 42, X: 1.25, Y: -2.5, Z: 3.75, Seq: 9}
	if err := node.SendSnapshot("peer-game", snap, time.Second); err != nil {
		t.Fatalf("SendSnapshot failed: %v", err)
	}
	if got := directCaptureCount(carrier); got != 1 {
		t.Fatalf("expected exactly one carrier write, got %d", got)
	}

	// The ciphertext decrypts through a cipher-matched far session to a
	// TypeDirect envelope whose payload is exactly the encoded snapshot.
	farEnd := newCapturingCarrier()
	farSess := cipherMatchedSession(t, farEnd)
	farEnd.reads <- carrier.lastWrite()
	plain, err := farSess.ReadPacket()
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
	if want := EncodeSnapshot(snap); string(env.Payload) != string(want) {
		t.Fatalf("payload mismatch: %x, want %x", env.Payload, want)
	}
	if wantSender := node.identity.PublicKeyBytes(); string(env.SenderID) != string(wantSender) {
		t.Fatalf("sender mismatch: %x != %x", env.SenderID, wantSender)
	}
}

func TestSendSnapshotUnknownPeerFallsBackToRelayPath(t *testing.T) {
	node, err := NewNode("mesh-game-relay", nil, isolatedTestConfig("game-relay"))
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	err = node.SendSnapshot("peer-absent", GameSnapshot{EntityID: 1, Seq: 1}, 100*time.Millisecond)
	if err == nil {
		t.Fatal("expected an error for a target with neither a session nor a relay candidate")
	}
	if !strings.Contains(err.Error(), "no relay-capable peer is connected") {
		t.Fatalf("expected the relay-path error, got %q", err.Error())
	}
}
