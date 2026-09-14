package tun

import (
	"bytes"
	"testing"
	"time"
)

// fragTestRouter builds a router with a fake clock and a recording send
// function, over a table that has peer-b on 10.66.0.1.
func fragTestRouter(now *time.Time, sent *[][]byte) *Router {
	table, _ := NewTable("10.66.0.0/24")
	_, _ = table.PeerAddr("peer-b")
	router := NewRouter(&fakeIface{}, table, func(_ string, payload []byte) error {
		*sent = append(*sent, append([]byte(nil), payload...))
		return nil
	}, DefaultMTU)
	router.now = func() time.Time { return *now }
	return router
}

// fragFramesOf splits the frames a fragmented packet produced, asserting
// they are all TFRG and share one fragID.
func fragFramesOf(t *testing.T, frames [][]byte) (uint32, [][]byte) {
	t.Helper()
	if len(frames) == 0 {
		t.Fatal("no fragment frames captured")
	}
	first, _ := parseFragFrame(frames[0])
	for i, f := range frames {
		if !IsFragFrame(f) {
			t.Fatalf("frame %d is not a TFRG frame", i)
		}
		h, _ := parseFragFrame(f)
		if h.fragID != first.fragID {
			t.Fatalf("frame %d has fragID %d, want %d", i, h.fragID, first.fragID)
		}
	}
	return first.fragID, frames
}

// TestFragRoundTripOutboundInbound covers the full v2 loop: one 4400-byte
// packet fragments into 3 frames (1488/1488/1424), the frames reassemble out
// of order into one interface write, byte-identical.
func TestFragRoundTripOutboundInbound(t *testing.T) {
	now := time.Now()
	var sent [][]byte
	out := fragTestRouter(&now, &sent)

	packet := ip4Packet("10.66.0.1", 4400)
	out.RouteOutbound(packet)
	fragID, frames := fragFramesOf(t, sent)
	if len(frames) != 3 {
		t.Fatalf("4400B should fragment into 3 frames of 1488/1488/1424, got %d", len(frames))
	}

	// Frame shape: consecutive indices, total 3, declared chunk sizes.
	var got [][]byte
	for _, f := range frames {
		h, chunk := parseFragFrame(f)
		if h.fragTotal != 3 {
			t.Fatalf("fragTotal = %d, want 3", h.fragTotal)
		}
		if h.fragID != fragID {
			t.Fatalf("fragID drifted: %d", h.fragID)
		}
		got = append(got, chunk)
	}
	if !bytes.Equal(bytes.Join(got, nil), packet) {
		t.Fatal("fragments concatenated do not reproduce the packet")
	}

	// Reassembly on a receiving router, out of order (2, 0, 1).
	iface := &fakeIface{}
	table, _ := NewTable("10.66.0.0/24")
	in := NewRouter(iface, table, nil, DefaultMTU)
	in.now = func() time.Time { return now }
	in.RouteInbound(frames[2])
	in.RouteInbound(frames[0])
	if len(iface.writes) != 0 {
		t.Fatal("assembly must not deliver before the last frame")
	}
	in.RouteInbound(frames[1])
	if len(iface.writes) != 1 {
		t.Fatalf("completed assembly should write once, got %d", len(iface.writes))
	}
	if !bytes.Equal(iface.writes[0], packet) {
		t.Fatal("reassembled packet differs from the original")
	}
	if in.Counters().FragReassembled.Load() != 1 {
		t.Fatalf("FragReassembled = %d", in.Counters().FragReassembled.Load())
	}
	if in.Counters().InboundDelivered.Load() != 1 {
		t.Fatalf("InboundDelivered = %d", in.Counters().InboundDelivered.Load())
	}
	if out.Counters().Forwarded.Load() != 1 {
		t.Fatalf("Forwarded = %d", out.Counters().Forwarded.Load())
	}
}

// TestFragDuplicateFrameIgnored pins the duplicate policy: a repeated index
// within one PENDING assembly is counted and ignored, never re-stored or
// double-delivered. (After completion the assembly's state is gone, so a
// full frame-set replay legitimately reassembles a second delivery — the
// same best-effort duplicate semantics IP itself has; the dedup contract
// is the pending window.)
func TestFragDuplicateFrameIgnored(t *testing.T) {
	now := time.Now()
	var sent [][]byte
	out := fragTestRouter(&now, &sent)

	packet := ip4Packet("10.66.0.1", 4400)
	out.RouteOutbound(packet)
	_, frames := fragFramesOf(t, sent)

	iface := &fakeIface{}
	table, _ := NewTable("10.66.0.0/24")
	in := NewRouter(iface, table, nil, DefaultMTU)
	in.now = func() time.Time { return now }
	// 0, 1, then a repeat of 1: the assembly holds 2 unique chunks and
	// one duplicate count.
	in.RouteInbound(frames[0])
	in.RouteInbound(frames[1])
	in.RouteInbound(frames[1])
	if in.Counters().FragDuplicate.Load() != 1 {
		t.Fatalf("FragDuplicate = %d, want 1", in.Counters().FragDuplicate.Load())
	}
	if len(iface.writes) != 0 {
		t.Fatal("duplicate must not complete an assembly early")
	}
	// The last unique chunk completes exactly one delivery.
	in.RouteInbound(frames[2])
	if len(iface.writes) != 1 {
		t.Fatalf("writes = %d, want 1", len(iface.writes))
	}
	if !bytes.Equal(iface.writes[0], packet) {
		t.Fatal("reassembled packet differs from the original")
	}
	// A late duplicate of the final frame finds no state (the assembly
	// was evicted on completion): it starts a fresh pending assembly,
	// counted neither as a duplicate nor as a delivery.
	in.RouteInbound(frames[2])
	if in.Counters().FragDuplicate.Load() != 1 {
		t.Fatalf("late duplicate must not count as an in-assembly duplicate, got %d", in.Counters().FragDuplicate.Load())
	}
	if len(iface.writes) != 1 {
		t.Fatal("late duplicate must not deliver")
	}
}

// TestFragExpiryEvictsPartialAssemblies pins the TTL: a partial assembly
// ages out on the sweep of a later frame's arrival, counted as expired.
func TestFragExpiryEvictsPartialAssemblies(t *testing.T) {
	now := time.Now()
	var sent [][]byte
	out := fragTestRouter(&now, &sent)

	out.RouteOutbound(ip4Packet("10.66.0.1", 4400))
	_, frames := fragFramesOf(t, sent)

	iface := &fakeIface{}
	table, _ := NewTable("10.66.0.0/24")
	in := NewRouter(iface, table, nil, DefaultMTU)
	cur := now
	in.now = func() time.Time { return cur }

	in.RouteInbound(frames[0])
	if in.Counters().FragExpired.Load() != 0 {
		t.Fatal("no assembly may expire before its deadline")
	}
	// 6 seconds later the partial assembly is stale; the next frame's
	// sweep evicts it. A second packet's frames provide the sweep.
	cur = cur.Add(6 * time.Second)
	out.RouteOutbound(ip4Packet("10.66.0.1", 4400))
	frames2 := append([][]byte(nil), sent[len(sent)-3:]...)
	in.RouteInbound(frames2[0])
	if in.Counters().FragExpired.Load() != 1 {
		t.Fatalf("FragExpired = %d, want 1", in.Counters().FragExpired.Load())
	}
	if len(iface.writes) != 0 {
		t.Fatal("expired assembly must not deliver")
	}
	// The new assembly lives: deliver the rest of the second packet.
	in.RouteInbound(frames2[1])
	in.RouteInbound(frames2[2])
	if len(iface.writes) != 1 {
		t.Fatalf("second packet should deliver after expiry sweep, writes = %d", len(iface.writes))
	}
}

// TestFragPendingOverflow pins the pending-assembly cap: 64 half-packets
// fill the table; the 65th distinct ID is dropped as overflow.
func TestFragPendingOverflow(t *testing.T) {
	iface := &fakeIface{}
	table, _ := NewTable("10.66.0.0/24")
	router := NewRouter(iface, table, nil, DefaultMTU)
	now := time.Now()
	router.now = func() time.Time { return now }

	// Build 64 distinct one-chunk-of-two assemblies.
	for id := uint32(1); id <= fragPendingLimit; id++ {
		frames := fragFramesFor(t, id, 2, 0)
		router.RouteInbound(frames[0])
	}
	if router.Counters().FragPendingOverflow.Load() != 0 {
		t.Fatal("the first 64 pending IDs must be accepted")
	}
	// The 65th distinct ID is refused.
	router.RouteInbound(fragFramesFor(t, fragPendingLimit+1, 2, 0)[0])
	if router.Counters().FragPendingOverflow.Load() != 1 {
		t.Fatalf("FragPendingOverflow = %d, want 1", router.Counters().FragPendingOverflow.Load())
	}
	if len(iface.writes) != 0 {
		t.Fatal("overflow must not deliver")
	}
	// An EXISTING pending ID still accepts its missing chunk — the cap
	// bounds distinct packets, not frames.
	router.RouteInbound(fragFramesFor(t, 1, 2, 1)[0])
	if router.Counters().FragPendingOverflow.Load() != 1 {
		t.Fatal("an existing assembly must not be refused")
	}
	if len(iface.writes) != 1 {
		t.Fatal("completing an existing assembly under the cap must deliver")
	}
}

// fragFramesFor builds synthetic TFRG frames for (fragID, total) carrying
// one chunk at index (a minimal 20-byte IPv4 header so a completed assembly
// passes the IPv4 check).
func fragFramesFor(t *testing.T, fragID uint32, total uint16, index uint16) [][]byte {
	t.Helper()
	h := fragHeader{fragID: fragID, fragIndex: index, fragTotal: total}
	return [][]byte{buildFragFrame(h, ip4Packet("10.66.0.1", 20))}
}

// TestFragSpoofedOversizeEvicted pins the sum cap: an assembly whose chunks
// sum past the hard cap is evicted whole, counted as a frag oversize.
func TestFragSpoofedOversizeEvicted(t *testing.T) {
	iface := &fakeIface{}
	table, _ := NewTable("10.66.0.0/24")
	router := NewRouter(iface, table, nil, DefaultMTU)
	now := time.Now()
	router.now = func() time.Time { return now }

	// Chunks within the per-payload MTU but summing past the hard cap:
	// each chunk is small enough to pass the inbound MTU gate; the sum
	// cap is the only defense. 2184 chunks of 30 bytes total 65520 —
	// under the cap; the 2185th pushes to 65550, past 65536, with
	// total still far away (65535).
	h := fragHeader{fragID: 1, fragTotal: 65535}
	chunk := make([]byte, 30)
	for h.fragIndex = 0; h.fragIndex < 2184; h.fragIndex++ {
		router.RouteInbound(buildFragFrame(h, chunk))
	}
	if router.Counters().FragOversize.Load() != 0 {
		t.Fatal("chunks under the sum cap must be accepted")
	}
	h.fragIndex = 2185
	router.RouteInbound(buildFragFrame(h, chunk))
	if router.Counters().FragOversize.Load() != 1 {
		t.Fatalf("FragOversize = %d, want 1", router.Counters().FragOversize.Load())
	}
	if len(iface.writes) != 0 {
		t.Fatal("an evicted assembly must not deliver")
	}
	// The assembly is gone: a re-sent index-0 chunk starts a fresh
	// assembly, not a duplicate drop.
	h.fragIndex = 0
	router.RouteInbound(buildFragFrame(h, chunk))
	if router.Counters().FragDuplicate.Load() != 0 {
		t.Fatal("evicted assembly must leave no state behind")
	}
}

// TestFragMalformedHeaders pins the header policy: valid magic with a zero
// total or an index beyond total is counted malformed, not reassembled.
func TestFragMalformedHeaders(t *testing.T) {
	iface := &fakeIface{}
	table, _ := NewTable("10.66.0.0/24")
	router := NewRouter(iface, table, nil, DefaultMTU)
	now := time.Now()
	router.now = func() time.Time { return now }

	zeroTotal := fragHeader{fragID: 1, fragIndex: 0, fragTotal: 0}
	router.RouteInbound(buildFragFrame(zeroTotal, ip4Packet("10.66.0.1", 20)))
	beyondTotal := fragHeader{fragID: 2, fragIndex: 3, fragTotal: 2}
	router.RouteInbound(buildFragFrame(beyondTotal, ip4Packet("10.66.0.1", 20)))
	// A total mismatch against an existing assembly is also malformed.
	first := fragHeader{fragID: 3, fragIndex: 0, fragTotal: 2}
	router.RouteInbound(buildFragFrame(first, ip4Packet("10.66.0.1", 20)))
	second := fragHeader{fragID: 3, fragIndex: 1, fragTotal: 3}
	router.RouteInbound(buildFragFrame(second, ip4Packet("10.66.0.1", 20)))
	if router.Counters().FragMalformed.Load() != 3 {
		t.Fatalf("FragMalformed = %d, want 3", router.Counters().FragMalformed.Load())
	}
	if len(iface.writes) != 0 {
		t.Fatal("malformed frames must not deliver")
	}
}

// TestFragClassifier pins the byte-level demultiplexer.
func TestFragClassifier(t *testing.T) {
	if IsFragFrame(ip4Packet("10.0.0.1", 20)) {
		t.Fatal("an IPv4 packet must not classify as a fragment frame")
	}
	if IsFragFrame([]byte("TFRG")) {
		t.Fatal("magic without the full header must not classify")
	}
	if IsFragFrame(nil) {
		t.Fatal("nil must not classify")
	}
	h := fragHeader{fragID: 1, fragIndex: 0, fragTotal: 1}
	if !IsFragFrame(buildFragFrame(h, []byte("x"))) {
		t.Fatal("a built fragment frame must classify")
	}
	// A frame built at the exact header length boundary (empty chunk).
	if !IsFragFrame(buildFragFrame(h, nil)) {
		t.Fatal("a zero-chunk frame must still classify")
	}
}
