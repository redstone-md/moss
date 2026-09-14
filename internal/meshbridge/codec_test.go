package meshbridge

import (
	"bytes"
	"sync"
	"testing"
	"time"
)

// codecFrame assembles a minimal valid MBRIDGE frame by hand — the
// layout the wire constants promise, independent of Encode, so the
// decode tests are not Encode grading itself.
func codecFrame() []byte {
	frame := make([]byte, MBRIHeaderLen+4)
	frame[0] = MBRIMagic
	frame[1] = 0
	for i := 2; i < 42; i++ {
		frame[i] = byte(i)
	}
	frame[42] = 0
	frame[43] = 1
	frame[46] = 0xAA
	frame[47] = 0xBB
	frame[48] = 0xCC
	frame[49] = 0xDD
	return frame
}

// TestWireLayoutConstants pins the header geometry the whole bridge
// assumes: the magic byte, the 46-byte header, the 191-byte chunk and
// the 237-byte frame cap.
func TestWireLayoutConstants(t *testing.T) {
	if MBRIMagic != 0x9D {
		t.Fatalf("MBRIMagic: got %#x, want 0x9D", MBRIMagic)
	}
	if MBRIHeaderLen != 46 {
		t.Fatalf("MBRIHeaderLen: got %d, want 46", MBRIHeaderLen)
	}
	if MBRIPayloadMax != 191 {
		t.Fatalf("MBRIPayloadMax: got %d, want 191", MBRIPayloadMax)
	}
	if MBRIWireMax != 237 {
		t.Fatalf("MBRIWireMax: got %d, want 237", MBRIWireMax)
	}
}

// TestIsMBridge proves the classification boundary: the magic byte
// makes a buffer MBRIDGE from the exact header length up, and neither
// a shorter buffer nor a wrong magic qualifies.
func TestIsMBridge(t *testing.T) {
	if !IsMBridge(codecFrame()) {
		t.Fatal("valid frame not classified MBRIDGE")
	}
	if !IsMBridge(codecFrame()[:MBRIHeaderLen]) {
		t.Fatal("bare-header frame not classified MBRIDGE")
	}
	if IsMBridge(codecFrame()[:MBRIHeaderLen-1]) {
		t.Fatal("buffer below the header length classified MBRIDGE")
	}
	if IsMBridge([]byte{MBRIMagic, 1, 2, 3}) {
		t.Fatal("short buffer classified MBRIDGE")
	}
	foreign := codecFrame()
	foreign[0] = 0x4D
	if IsMBridge(foreign) {
		t.Fatal("wrong-magic buffer classified MBRIDGE")
	}
}

// TestEncodeSingleFrame pins the one-frame path: a payload within the
// chunk budget encodes to exactly one frame with bit0 clear, header
// fields at their wire offsets, payload bytes verbatim.
func TestEncodeSingleFrame(t *testing.T) {
	src := [32]byte{9}
	msgID := NewMsgID(src, nil)
	frames, err := Encode(src, msgID, 0x0203, 0, []byte("hello"))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(frames) != 1 {
		t.Fatalf("frames: got %d, want 1", len(frames))
	}
	fr := frames[0]
	if len(fr) != MBRIHeaderLen+5 {
		t.Fatalf("frame length: got %d, want %d", len(fr), MBRIHeaderLen+5)
	}
	if fr[0] != MBRIMagic {
		t.Fatalf("magic: got %#x", fr[0])
	}
	if fr[1] != 0 {
		t.Fatalf("flags: got %#x, want 0 (bit0 clear on a whole message)", fr[1])
	}
	if !bytes.Equal(fr[2:34], src[:]) {
		t.Fatal("srcPeerID not at offset 2")
	}
	if !bytes.Equal(fr[34:42], msgID[:]) {
		t.Fatal("msgID not at offset 34")
	}
	if fr[42] != 0 || fr[43] != 1 {
		t.Fatalf("frag: index=%d total=%d, want 0/1", fr[42], fr[43])
	}
	if fr[44] != 0x02 || fr[45] != 0x03 {
		t.Fatalf("channelHash: got %#x %#x, want 02 03", fr[44], fr[45])
	}
	if string(fr[46:]) != "hello" {
		t.Fatalf("payload: got %q", fr[46:])
	}
}

// TestEncodeEmptyPayload proves the degenerate-but-real shape: a nil
// or zero-byte payload is one whole frame, not zero frames.
func TestEncodeEmptyPayload(t *testing.T) {
	for name, payload := range map[string][]byte{"nil": nil, "empty": {}} {
		frames, err := Encode([32]byte{1}, [8]byte{2}, 0, 0, payload)
		if err != nil {
			t.Fatalf("%s: Encode: %v", name, err)
		}
		if len(frames) != 1 {
			t.Fatalf("%s: frames: got %d, want 1", name, len(frames))
		}
		if len(frames[0]) != MBRIHeaderLen {
			t.Fatalf("%s: frame length: got %d, want %d", name, len(frames[0]), MBRIHeaderLen)
		}
	}
}

// TestEncodeKeepaliveShape pins the exact frame keepalive.go builds:
// all-zero src (the domain-separation salt is a legal sender), keepalive
// flag, nil payload, one bare-header frame.
func TestEncodeKeepaliveShape(t *testing.T) {
	frames, err := Encode([32]byte{}, NewMsgID([32]byte{}, nil), ChannelHashNone, FlagKeepalive, nil)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(frames) != 1 || len(frames[0]) != MBRIHeaderLen {
		t.Fatalf("keepalive: %d frames, first %d bytes", len(frames), len(frames[0]))
	}
	fr := frames[0]
	if fr[1] != FlagKeepalive {
		t.Fatalf("flags: got %#x, want FlagKeepalive", fr[1])
	}
	for i, b := range fr[2:34] {
		if b != 0 {
			t.Fatalf("src byte %d: got %#x, want zero (salt accepted)", i, b)
		}
	}
	if fr[43] != 1 {
		t.Fatalf("fragTotal: got %d, want 1", fr[43])
	}
	if fr[44] != 0 || fr[45] != 0 {
		t.Fatalf("channelHash: got %#x %#x, want ChannelHashNone", fr[44], fr[45])
	}
}

// TestEncodeFragmentation walks the fragmentation boundary: 191 bytes
// fit one full frame; 192 bytes need two, with the exact-max first
// chunk and the 1-byte remainder second, bit0 set on every frame.
func TestEncodeFragmentation(t *testing.T) {
	exact := bytes.Repeat([]byte{0xAB}, MBRIPayloadMax)
	frames, err := Encode([32]byte{1}, [8]byte{2}, 0, 0, exact)
	if err != nil {
		t.Fatalf("Encode(max): %v", err)
	}
	if len(frames) != 1 {
		t.Fatalf("max payload: frames: got %d, want 1", len(frames))
	}
	if len(frames[0]) != MBRIWireMax {
		t.Fatalf("max frame: got %d bytes, want %d", len(frames[0]), MBRIWireMax)
	}

	over := append(append([]byte(nil), exact...), 0xCD)
	frames, err = Encode([32]byte{1}, [8]byte{2}, 0, 0, over)
	if err != nil {
		t.Fatalf("Encode(max+1): %v", err)
	}
	if len(frames) != 2 {
		t.Fatalf("max+1: frames: got %d, want 2", len(frames))
	}
	for i, fr := range frames {
		if fr[42] != byte(i) {
			t.Fatalf("frame %d: fragIndex: got %d", i, fr[42])
		}
		if fr[43] != 2 {
			t.Fatalf("frame %d: fragTotal: got %d, want 2", i, fr[43])
		}
		if fr[1]&FlagFragmented == 0 {
			t.Fatalf("frame %d: bit0 not set", i)
		}
	}
	if len(frames[0]) != MBRIWireMax {
		t.Fatalf("first chunk: got %d bytes, want %d", len(frames[0]), MBRIWireMax)
	}
	if len(frames[1]) != MBRIHeaderLen+1 {
		t.Fatalf("second chunk: got %d bytes, want %d", len(frames[1]), MBRIHeaderLen+1)
	}
	if frames[1][MBRIHeaderLen] != 0xCD {
		t.Fatal("second chunk payload byte wrong")
	}
}

// TestEncodeOversizeRejected proves the hard ceiling: a payload that
// would need 256 frames cannot be addressed by the one-byte fragTotal
// and is rejected up front.
func TestEncodeOversizeRejected(t *testing.T) {
	payload := bytes.Repeat([]byte{0x01}, 255*MBRIPayloadMax+1)
	if _, err := Encode([32]byte{1}, [8]byte{2}, 0, 0, payload); err == nil {
		t.Fatal("oversize payload accepted")
	}
	// The boundary itself is legal: 255 frames' worth fits.
	if _, err := Encode([32]byte{1}, [8]byte{2}, 0, 0, payload[:255*MBRIPayloadMax]); err != nil {
		t.Fatalf("boundary payload rejected: %v", err)
	}
}

// TestEncodeFlagPassthrough proves the codec's opacity: bits 1-4 ride
// verbatim on every frame, and bit0 is the codec's alone — set by the
// fragmentation itself, cleared when a caller set it by hand on a
// whole message.
func TestEncodeFlagPassthrough(t *testing.T) {
	flags := FlagAckRequest | FlagEncryptedRoom | FlagDirect | FlagKeepalive

	// Whole message: bits 1-4 verbatim, a hand-set bit0 normalized away.
	frames, err := Encode([32]byte{1}, [8]byte{2}, 0, flags|FlagFragmented, []byte("x"))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	for i, fr := range frames {
		if fr[1] != flags {
			t.Fatalf("frame %d: flags: got %#x, want %#x", i, fr[1], flags)
		}
	}

	// Fragmented message: bit0 comes from the fragmentation itself.
	frames, err = Encode([32]byte{1}, [8]byte{2}, 0, flags, bytes.Repeat([]byte{0x11}, 2*MBRIPayloadMax))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	for i, fr := range frames {
		if fr[1] != flags|FlagFragmented {
			t.Fatalf("frame %d: flags: got %#x, want %#x", i, fr[1], flags|FlagFragmented)
		}
	}
}

// TestDecodeErrors walks every rejection the decoder owns: short
// buffer, wrong magic, zero total, index past total, over-budget chunk.
func TestDecodeErrors(t *testing.T) {
	cases := []struct {
		name   string
		mutate func([]byte) []byte
	}{
		{"short buffer", func(b []byte) []byte { return b[:MBRIHeaderLen-1] }},
		{"wrong magic", func(b []byte) []byte { b[0] = 0x4D; return b }},
		{"zero fragTotal", func(b []byte) []byte { b[43] = 0; return b }},
		{"fragIndex past total", func(b []byte) []byte { b[42] = 1; return b }},
		{"over-budget chunk", func(b []byte) []byte { return append(b, bytes.Repeat([]byte{0}, MBRIPayloadMax)...) }},
	}
	for _, tc := range cases {
		frame := tc.mutate(append([]byte(nil), codecFrame()...))
		if _, err := Decode(frame); err == nil {
			t.Fatalf("%s: Decode accepted", tc.name)
		}
	}
	if _, err := Decode(nil); err == nil {
		t.Fatal("nil buffer accepted")
	}
	if _, err := Decode([]byte{}); err == nil {
		t.Fatal("empty buffer accepted")
	}
}

// TestDecodeHandBuiltFrame proves Decode parses a frame Encode never
// made: every header field lands where the wire format puts it.
func TestDecodeHandBuiltFrame(t *testing.T) {
	wire := codecFrame()
	wire[44] = 0xBE
	wire[45] = 0xEF
	f, err := Decode(wire)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	var wantSrc [32]byte
	for i := range wantSrc {
		wantSrc[i] = byte(i + 2)
	}
	if f.SrcPeerID != wantSrc {
		t.Fatal("SrcPeerID fields not read from offset 2")
	}
	var wantID [8]byte
	for i := range wantID {
		wantID[i] = byte(i + 34)
	}
	if f.MsgID != wantID {
		t.Fatal("MsgID not read from offset 34")
	}
	if f.Flags != 0 || f.FragIndex != 0 || f.FragTotal != 1 {
		t.Fatalf("flags/frag: %#x %d/%d", f.Flags, f.FragIndex, f.FragTotal)
	}
	if f.ChannelHash != 0xBEEF {
		t.Fatalf("channelHash: got %#x", f.ChannelHash)
	}
	if !bytes.Equal(f.Payload, []byte{0xAA, 0xBB, 0xCC, 0xDD}) {
		t.Fatalf("payload: got % x", f.Payload)
	}
}

// TestDecodeRoundTrip proves what the bridge actually runs: the frames
// Encode emits are exactly the frames Decode accepts, header fields
// landing where the wire format puts them.
func TestDecodeRoundTrip(t *testing.T) {
	src := [32]byte{7}
	msgID := NewMsgID(src, []byte("rt"))
	frames, err := Encode(src, msgID, 0xBEEF, FlagDirect, bytes.Repeat([]byte{0x5A}, 400))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(frames) != 3 {
		t.Fatalf("frames: got %d, want 3 (400 bytes / 191)", len(frames))
	}
	seen := map[uint8]bool{}
	for _, wire := range frames {
		f, err := Decode(wire)
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if f.SrcPeerID != src {
			t.Fatal("SrcPeerID mismatch")
		}
		if f.MsgID != msgID {
			t.Fatal("MsgID mismatch")
		}
		if f.Flags != FlagDirect|FlagFragmented {
			t.Fatalf("flags: got %#x, want direct|fragmented", f.Flags)
		}
		if f.ChannelHash != 0xBEEF {
			t.Fatalf("channelHash: got %#x", f.ChannelHash)
		}
		if f.FragTotal != 3 {
			t.Fatalf("fragTotal: got %d, want 3", f.FragTotal)
		}
		if seen[f.FragIndex] {
			t.Fatalf("fragIndex %d seen twice", f.FragIndex)
		}
		seen[f.FragIndex] = true
		if f.FragIndex == 2 && len(f.Payload) != 400-2*MBRIPayloadMax {
			t.Fatalf("last chunk: got %d bytes, want 18", len(f.Payload))
		}
	}
}

// TestNewMsgID proves the ID contract: deterministic, pinned to sender
// and payload, nil payload legal, and the zero-salt space disjoint from
// every real peer's — the assumption keepalive IDs rest on.
func TestNewMsgID(t *testing.T) {
	a := NewMsgID([32]byte{1}, []byte("m"))
	b := NewMsgID([32]byte{1}, []byte("m"))
	if a != b {
		t.Fatal("same inputs, different IDs")
	}
	if NewMsgID([32]byte{2}, []byte("m")) == a {
		t.Fatal("sender not mixed into the ID")
	}
	if NewMsgID([32]byte{1}, []byte("n")) == a {
		t.Fatal("payload not mixed into the ID")
	}
	if NewMsgID([32]byte{1}, nil) == a {
		t.Fatal("nil payload produced a data message's ID")
	}
	if NewMsgID([32]byte{}, nil) == NewMsgID([32]byte{1}, nil) {
		t.Fatal("zero sender collapsed into another ID space")
	}
}

// TestFeedFastPathWholeFrame proves the pump's hot path: a whole frame
// completes without touching the table.
func TestFeedFastPathWholeFrame(t *testing.T) {
	r := NewReassembler(time.Minute, 8)
	frames, err := Encode([32]byte{1}, [8]byte{2}, 0, 0, []byte("whole"))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	f, err := Decode(frames[0])
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	complete, ok := r.Feed(f, time.Now())
	if !ok {
		t.Fatal("whole frame did not complete")
	}
	if !bytes.Equal(complete.Payload, []byte("whole")) {
		t.Fatalf("payload: got %q", complete.Payload)
	}
	if r.Pending() != 0 {
		t.Fatalf("whole frame left state: %d pending", r.Pending())
	}
}

// TestFeedOutOfOrderAssembly proves the reassembly contract: fragments
// arriving in reverse order assemble byte-exact, the merged frame
// collapsing fragIndex/fragTotal to the whole shape while the message's
// flag bits (bit0 included) and channelHash ride verbatim.
func TestFeedOutOfOrderAssembly(t *testing.T) {
	r := NewReassembler(time.Minute, 8)
	payload := make([]byte, 500)
	for i := range payload {
		payload[i] = byte(i)
	}
	frames, err := Encode([32]byte{3}, [8]byte{4}, 0xABCD, FlagDirect, payload)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	var complete Frame
	var ok bool
	for i := len(frames) - 1; i >= 0; i-- {
		f, err := Decode(frames[i])
		if err != nil {
			t.Fatalf("Decode(%d): %v", i, err)
		}
		complete, ok = r.Feed(f, time.Now())
	}
	if !ok {
		t.Fatal("fragments never assembled")
	}
	if !bytes.Equal(complete.Payload, payload) {
		t.Fatalf("assembly mismatch: got %d bytes", len(complete.Payload))
	}
	if complete.FragIndex != 0 || complete.FragTotal != 1 {
		t.Fatalf("merged frag shape: index=%d total=%d, want 0/1", complete.FragIndex, complete.FragTotal)
	}
	if complete.Flags != FlagDirect|FlagFragmented {
		t.Fatalf("merged flags: got %#x", complete.Flags)
	}
	if complete.ChannelHash != 0xABCD {
		t.Fatalf("merged channelHash: got %#x", complete.ChannelHash)
	}
	if r.Pending() != 0 {
		t.Fatalf("assembly left state: %d pending", r.Pending())
	}
}

// TestFeedDuplicateIndexTearsDown proves the garbage rule: a second
// copy of a chunk already stored deletes the partial — the message
// cannot complete until honest frames restart it.
func TestFeedDuplicateIndexTearsDown(t *testing.T) {
	r := NewReassembler(time.Minute, 8)
	frames, err := Encode([32]byte{1}, [8]byte{2}, 0, 0, bytes.Repeat([]byte{0x11}, 2*MBRIPayloadMax))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	first, err := Decode(frames[0])
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if _, ok := r.Feed(first, time.Now()); ok {
		t.Fatal("first fragment completed a message")
	}
	if r.Pending() != 1 {
		t.Fatalf("first fragment not stored: pending=%d", r.Pending())
	}
	if _, ok := r.Feed(first, time.Now()); ok {
		t.Fatal("duplicate chunk completed a message")
	}
	if r.Pending() != 0 {
		t.Fatalf("duplicate left the partial: pending=%d", r.Pending())
	}
}

// TestFeedTotalMismatchTearsDown proves the other garbage rule: a
// fragment that redefines the total deletes the partial.
func TestFeedTotalMismatchTearsDown(t *testing.T) {
	r := NewReassembler(time.Minute, 8)
	frames, err := Encode([32]byte{1}, [8]byte{2}, 0, 0, bytes.Repeat([]byte{0x11}, 3*MBRIPayloadMax))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	f0, err := Decode(frames[0])
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	f1, err := Decode(frames[1])
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if _, ok := r.Feed(f0, time.Now()); ok {
		t.Fatal("first fragment completed")
	}
	f1.FragTotal = 2 // forged total: geometry Decode would reject
	if _, ok := r.Feed(f1, time.Now()); ok {
		t.Fatal("total-mismatch frame completed")
	}
	if r.Pending() != 0 {
		t.Fatalf("mismatch left the partial: pending=%d", r.Pending())
	}
}

// TestFeedStaleStateRestart proves the lazy-expiry contract: a partial
// past its deadline is absent to the next frame, which starts a fresh
// partial that can complete.
func TestFeedStaleStateRestart(t *testing.T) {
	r := NewReassembler(10*time.Millisecond, 8)
	frames, err := Encode([32]byte{1}, [8]byte{2}, 0, 0, bytes.Repeat([]byte{0x11}, 2*MBRIPayloadMax))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	f0, err := Decode(frames[0])
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	f1, err := Decode(frames[1])
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if _, ok := r.Feed(f0, time.Now()); ok {
		t.Fatal("first fragment completed")
	}
	later := time.Now().Add(20 * time.Millisecond)
	if _, ok := r.Feed(f1, later); ok {
		t.Fatal("expired partial assembled with the wrong chunk count")
	}
	// Chunk 0 again after expiry: the fresh partial gets both, completes.
	complete, ok := r.Feed(f0, later)
	if !ok {
		t.Fatal("restart after expiry failed to assemble")
	}
	if len(complete.Payload) != 2*MBRIPayloadMax {
		t.Fatalf("restarted assembly wrong: %d bytes", len(complete.Payload))
	}
}

// TestFeedCapEvictsStalest proves the hard bound: a new key on a full
// table evicts the stalest partial (the one whose deadline is soonest —
// fed longest ago) and is admitted in its place.
func TestFeedCapEvictsStalest(t *testing.T) {
	r := NewReassembler(time.Minute, 2)
	t0 := time.Now()
	seed := func(src byte, key [8]byte, at time.Time) {
		f := Frame{SrcPeerID: [32]byte{src}, MsgID: key, FragIndex: 0, FragTotal: 2}
		if _, ok := r.Feed(f, at); ok {
			t.Fatalf("seed fragment %d completed", src)
		}
	}
	seed(1, [8]byte{1}, t0)                  // deadline t0+ttl: the stalest
	seed(2, [8]byte{2}, t0.Add(time.Second)) // deadline t0+1s+ttl
	if r.Pending() != 2 {
		t.Fatalf("seed: pending=%d, want 2", r.Pending())
	}
	// A third key arrives: the stalest (src 1) is evicted, the new key
	// admitted — the cap holds, nothing is dropped on the floor.
	if _, ok := r.Feed(Frame{SrcPeerID: [32]byte{9}, MsgID: [8]byte{4}, FragIndex: 0, FragTotal: 2}, t0.Add(2*time.Second)); ok {
		t.Fatal("evicting fragment completed")
	}
	if r.Pending() != 2 {
		t.Fatalf("after eviction: pending=%d, want 2", r.Pending())
	}
	// The admitted key completes — proof eviction admits, not drops.
	complete, ok := r.Feed(Frame{SrcPeerID: [32]byte{9}, MsgID: [8]byte{4}, FragIndex: 1, FragTotal: 2}, t0.Add(3*time.Second))
	if !ok {
		t.Fatal("new key not admitted after eviction")
	}
	if complete.FragTotal != 1 {
		t.Fatalf("admitted message merged wrong: total=%d", complete.FragTotal)
	}
	// The evicted message's second fragment starts a fresh partial
	// instead of completing a message.
	if _, ok := r.Feed(Frame{SrcPeerID: [32]byte{1}, MsgID: [8]byte{1}, FragIndex: 1, FragTotal: 2}, t0.Add(3*time.Second)); ok {
		t.Fatal("evicted message completed from a fresh partial")
	}
	if r.Pending() != 2 {
		t.Fatalf("fresh partial: pending=%d, want 2 (cap holds)", r.Pending())
	}
}

// TestSweepExpiresPartials proves the explicit expiry handle: Sweep
// drops every partial past its deadline and returns the count, leaving
// live partials alone.
func TestSweepExpiresPartials(t *testing.T) {
	r := NewReassembler(time.Minute, 8)
	now := time.Now()
	seed := func(key [8]byte, at time.Time) {
		f := Frame{SrcPeerID: [32]byte{1}, MsgID: key, FragIndex: 0, FragTotal: 2}
		if _, ok := r.Feed(f, at); ok {
			t.Fatalf("seed %v completed", key)
		}
	}
	seed([8]byte{1}, now)
	seed([8]byte{2}, now.Add(2*time.Minute)) // deadline far out
	if r.Pending() != 2 {
		t.Fatalf("seed: pending=%d, want 2", r.Pending())
	}
	// now+61s: partial 1 (deadline now+60s) is expired, partial 2 is
	// not (deadline now+2m+60s).
	if dropped := r.Sweep(now.Add(61 * time.Second)); dropped != 1 {
		t.Fatalf("Sweep: dropped=%d, want 1", dropped)
	}
	if r.Pending() != 1 {
		t.Fatalf("after Sweep: pending=%d, want 1", r.Pending())
	}
}

// TestMergedFrameCarriesFirstFragmentMetadata proves the merged
// frame's header comes from the stored fragment set, not the last
// arrival: flags and channelHash pinned by the first stored fragment.
func TestMergedFrameCarriesFirstFragmentMetadata(t *testing.T) {
	r := NewReassembler(time.Minute, 8)
	frames, err := Encode([32]byte{5}, [8]byte{6}, 0x1234, FlagEncryptedRoom, bytes.Repeat([]byte{0x7E}, 3*MBRIPayloadMax))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	var complete Frame
	var ok bool
	// Feed in a middle-first order to vary the "last arrival".
	for _, idx := range []int{1, 0, 2} {
		f, err := Decode(frames[idx])
		if err != nil {
			t.Fatalf("Decode(%d): %v", idx, err)
		}
		complete, ok = r.Feed(f, time.Now())
	}
	if !ok {
		t.Fatal("message never assembled")
	}
	if complete.Flags != FlagEncryptedRoom|FlagFragmented {
		t.Fatalf("merged flags: got %#x", complete.Flags)
	}
	if complete.ChannelHash != 0x1234 {
		t.Fatalf("merged channelHash: got %#x", complete.ChannelHash)
	}
}

// TestReassemblerConcurrentAccess proves the mutex contract: Feed,
// Sweep and Pending from many goroutines at once stay clean under the
// race detector — the pump feeds from its ingress while tests poke the
// same table from other goroutines.
func TestReassemblerConcurrentAccess(t *testing.T) {
	r := NewReassembler(time.Minute, 16)
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			src := [32]byte{byte(i + 1)}
			for j := 0; j < 50; j++ {
				key := [8]byte{byte(j)}
				for idx := uint8(0); idx < 2; idx++ {
					f := Frame{SrcPeerID: src, MsgID: key, FragIndex: idx, FragTotal: 2, Payload: []byte{idx}}
					r.Feed(f, time.Now())
				}
				r.Pending()
			}
			r.Sweep(time.Now())
		}(i)
	}
	wg.Wait()
	if r.Pending() != 0 {
		t.Fatalf("concurrent feeds left partials: %d", r.Pending())
	}
}
