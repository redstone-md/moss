package tun

import (
	"net/netip"
	"testing"
)

// fuzzClampBytes returns a fresh copy of at most the first n bytes of src.
// The fuzzer must not hand the router multi-megabyte buffers: production
// gates every payload at the MTU (inbound) and the hard cap (outbound), so
// the fuzz harness enforces the same per-iteration ceilings.
func fuzzClampBytes(src []byte, n int) []byte {
	if len(src) > n {
		src = src[:n]
	}
	out := make([]byte, len(src))
	copy(out, src)
	return out
}

// fuzzRouterPair builds the outbound/inbound router pair the round-trip
// checks need: one router over a table that assigns 10.66.0.1 to peer-b
// with a recording send function (frames land in *sent), and a bare inbound
// router whose interface records its writes.
func fuzzRouterPair() (out *Router, in *Router, iface *fakeIface, sent *[][]byte) {
	table, _ := NewTable("10.66.0.0/24")
	assigned, _ := table.PeerAddr("peer-b")
	if assigned != netip.MustParseAddr("10.66.0.1") {
		panic("fuzz table assignment drifted")
	}
	sent = &[][]byte{}
	out = NewRouter(&fakeIface{}, table, func(_ string, payload []byte) error {
		*sent = append(*sent, append([]byte(nil), payload...))
		return nil
	}, DefaultMTU)
	iface = &fakeIface{}
	in = NewRouter(iface, table, nil, DefaultMTU)
	return out, in, iface, sent
}

// FuzzRouterClassifier drives the packet-class demultiplexer (IsIPv4 /
// IsFragFrame / IPv4HeaderLen) and one RouteOutbound call with arbitrary
// bytes. These byte-level classifiers split IP traffic from fragment frames
// from application payloads on the inbound path; a panic here is a remotely
// triggerable crash and a classifier disagreement is a misrouting bug. The
// pinned contracts:
//
//   - no panic on any input, including empty and 0x4X-prefixed buffers;
//   - the two classifiers never both claim one buffer (the TFRG magic's
//     first nibble 0x5 keeps it out of the IPv4 class);
//   - IPv4HeaderLen reports the declared IHL: a multiple of 4 in [0,60].
//   - RouteOutbound consumes any buffer with a clean drop or a send.
func FuzzRouterClassifier(f *testing.F) {
	f.Add([]byte{0x45, 0, 0, 20, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 66, 0, 1})
	f.Add([]byte{0x54, 0x46, 0x52, 0x47, 0, 0, 0, 1, 0, 0, 0, 2})
	f.Add([]byte{0x54, 0x46, 0x52, 0x47})
	f.Add([]byte{0x40})
	f.Add([]byte{0x4f, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	f.Add([]byte{0x55, 0x00, 0x00, 0x14})
	f.Add([]byte("TFRG application payload coincidence"))
	f.Add([]byte(nil))
	f.Add([]byte{0x45})
	f.Fuzz(func(t *testing.T, data []byte) {
		// Fresh copy so a router that wrongly retained the input cannot
		// corrupt the fuzzer's buffer.
		data = append([]byte(nil), data...)

		isV4 := IsIPv4(data)
		isFrag := IsFragFrame(data)
		if isV4 && isFrag {
			t.Fatalf("input % x classified as both IPv4 and fragment frame", data)
		}
		if isFrag {
			h, chunk := parseFragFrame(data)
			if len(chunk) != len(data)-fragHeaderLen {
				t.Fatalf("parseFragFrame chunk length %d, want %d", len(chunk), len(data)-fragHeaderLen)
			}
			_ = h
		}
		hl := IPv4HeaderLen(data)
		// The IHL field is declared, not clamped to the buffer: a
		// 21-byte buffer may declare a 60-byte header, and IsIPv4's
		// version-nibble check does not require IHL >= 5. Callers
		// validate against len(data) themselves, so the pinned
		// contract is the field's arithmetic shape only.
		if hl < 0 || hl > 60 || hl%4 != 0 {
			t.Fatalf("IPv4HeaderLen(% x) = %d, outside [0,60] or not a multiple of 4", data, hl)
		}

		out, _, _, _ := fuzzRouterPair()
		out.RouteOutbound(data)
	})
}

// FuzzRouterInboundFragReassembly feeds arbitrary fragment frames to the
// inbound reassembly path: lied-about headers, duplicate and out-of-order
// chunks, spoofed totals, mid-assembly ID collisions, and whole-packet
// payloads interleaved with fragments. The pinned contracts:
//
//   - RouteInbound never panics on any payload: malformed frames are
//     counted drops, garbage reassemblies are evicted;
//   - the interface only ever receives buffers that pass the IPv4 check;
//   - InboundDelivered counts exactly the interface writes;
//   - no pending assembly ever holds more than fragHardCap chunk bytes.
func FuzzRouterInboundFragReassembly(f *testing.F) {
	// One valid out-of-order 2-frame set for one ID (index bytes 1,0).
	f.Add([]byte{1, 0}, []byte{0x45, 0, 0, 20, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 66, 0, 1}, false)
	// The same set with every frame delivered twice.
	f.Add([]byte{0, 1}, []byte{0x45, 0, 0, 20, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 66, 0, 1}, true)
	// A single frame with index 0 out of a 1-frame total.
	f.Add([]byte{0}, []byte{0x45, 0, 0, 20}, false)
	// An empty chunk: the frame is header-only.
	f.Add([]byte{0}, []byte{}, false)
	// A chunk that doubles as a whole IPv4 packet payload.
	f.Add([]byte{}, []byte{0x45, 0, 0, 20, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 66, 0, 1}, false)
	// Duplicate index bytes past the 8-frame cap.
	f.Add([]byte{9, 8, 7, 6, 5, 4, 3, 2, 1, 0}, []byte{0x45, 0, 0, 20}, true)
	f.Fuzz(func(t *testing.T, idxSeed, chunkSeed []byte, dup bool) {
		_, in, iface, _ := fuzzRouterPair()

		// The chunk rides inside a frame, so clamp it to the largest
		// chunk one frame can carry at the default MTU.
		chunk := fuzzClampBytes(chunkSeed, DefaultMTU-fragHeaderLen)

		// Build at most 8 frames from the seeds: one or two colliding
		// fragIDs, seeded indices, the frame count as the total.
		//
		// idxSeed carries one index byte per frame; byte values wrap
		// into range with a modulo so any seed still forms frames.
		count := len(idxSeed)
		if count > 8 {
			count = 8
		}
		var frames [][]byte
		for i := range count {
			fragID := uint32(1)
			if i%2 == 1 {
				fragID = 2 // a mid-assembly ID collision
			}
			total := uint16(count)
			if total == 0 {
				total = 1
			}
			idx := uint16(idxSeed[i]) % total
			h := fragHeader{fragID: fragID, fragIndex: idx, fragTotal: total}
			frames = append(frames, buildFragFrame(h, chunk))
		}
		if dup {
			frames = append(frames, frames...)
		}
		// A whole-packet payload rides along when the chunk seed
		// doubles as a plausible packet.
		if len(chunk) >= 20 {
			frames = append(frames, append([]byte(nil), chunk...))
		}

		deliveredBefore := in.Counters().InboundDelivered.Load()
		for _, frame := range frames {
			in.RouteInbound(append([]byte(nil), frame...))
		}
		delivered := in.Counters().InboundDelivered.Load() - deliveredBefore

		for i, w := range iface.writes {
			if !IsIPv4(w) {
				t.Fatalf("iface write %d is not an IPv4 packet: % x", i, w)
			}
		}
		if int(delivered) != len(iface.writes) {
			t.Fatalf("InboundDelivered delta %d != %d interface writes", delivered, len(iface.writes))
		}

		in.fragMu.Lock()
		for id, fa := range in.frags {
			if fa.sum > fragHardCap {
				t.Fatalf("assembly %d holds %d chunk bytes, over the %d hard cap", id, fa.sum, fragHardCap)
			}
		}
		in.fragMu.Unlock()
	})
}

// FuzzRouterOutboundRoundTrip drives the outbound path and folds what was
// sent back through a fresh inbound router, in forward or reverse frame
// order. The pinned contracts:
//
//   - RouteOutbound never panics on any input, including non-IPv4 garbage
//     and packets past the fragmentation hard cap;
//   - a packet at or under the MTU is never fragmented;
//   - a fragmented packet's frames reassemble into the exact original
//     bytes regardless of delivery order.
func FuzzRouterOutboundRoundTrip(f *testing.F) {
	f.Add(uint16(20), uint32(1), []byte{0x45, 0, 0, 20, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 66, 0, 1}, false)
	f.Add(uint16(1600), uint32(1), []byte("payload"), true)
	f.Add(uint16(16000), uint32(1), []byte("payload"), true)
	f.Add(uint16(1600), uint32(200), []byte("x"), false) // dst not in the assigned pool
	f.Add(uint16(0), uint32(1), []byte{}, false)
	f.Fuzz(func(t *testing.T, size uint16, dstHost uint32, bodySeed []byte, reverse bool) {
		out, in, inIface, sent := fuzzRouterPair()

		n := int(size) % (fragHardCap + 4096)
		packet := make([]byte, n)
		if n >= 20 {
			packet[0] = 0x45
			packet[2] = byte(n >> 8)
			packet[3] = byte(n)
			host := byte(1) // peer-b's assigned 10.66.0.1
			if dstHost%2 == 1 {
				host = 200 // inside the /24, not assigned
			}
			packet[16], packet[17], packet[18], packet[19] = 10, 66, 0, host
			copy(packet[20:], fuzzClampBytes(bodySeed, n-20))
		}

		out.RouteOutbound(append([]byte(nil), packet...))

		fragFrames, whole := 0, 0
		for _, s := range *sent {
			if IsFragFrame(s) {
				fragFrames++
			} else {
				whole++
			}
		}
		if n <= DefaultMTU && fragFrames > 0 {
			t.Fatalf("packet of %d bytes (<= MTU %d) was fragmented", n, DefaultMTU)
		}
		if fragFrames > 0 && whole > 0 {
			t.Fatalf("router mixed fragments (%d) and whole packets (%d) for one packet", fragFrames, whole)
		}

		if len(*sent) == 0 {
			return // dropped: unknown destination, malformed, or over the hard cap
		}

		// Fold the captured frames back through the inbound router,
		// forward or reversed — out-of-order frames must reassemble
		// into the exact original packet either way.
		order := make([]int, len(*sent))
		for i := range order {
			order[i] = i
		}
		if reverse {
			for i, j := 0, len(order)-1; i < j; i, j = i+1, j-1 {
				order[i], order[j] = order[j], order[i]
			}
		}
		for _, i := range order {
			in.RouteInbound(append([]byte(nil), (*sent)[i]...))
		}

		if fragFrames > 0 {
			if len(inIface.writes) != 1 {
				t.Fatalf("fragmented %d-byte packet produced %d interface writes, want 1", n, len(inIface.writes))
			}
			if got := inIface.writes[0]; string(got) != string(packet) {
				t.Fatalf("reassembled packet differs from the original: %d bytes vs %d", len(got), len(packet))
			}
		}
	})
}

// FuzzFragFrameCodec pins the frame codec itself: any header/chunk pair
// survives buildFragFrame → parseFragFrame exactly, and the builder copies
// the chunk rather than aliasing it.
func FuzzFragFrameCodec(f *testing.F) {
	f.Add(uint32(0), uint16(0), uint16(1), []byte("chunk"))
	f.Add(uint32(0xFFFFFFFF), uint16(0xFFFF), uint16(0xFFFF), []byte{})
	f.Add(uint32(42), uint16(3), uint16(7), []byte{0x45, 0, 0, 20})
	f.Fuzz(func(t *testing.T, fragID uint32, fragIndex, fragTotal uint16, chunk []byte) {
		chunk = fuzzClampBytes(chunk, DefaultMTU)
		h := fragHeader{fragID: fragID, fragIndex: fragIndex, fragTotal: fragTotal}
		frame := buildFragFrame(h, chunk)
		if !IsFragFrame(frame) {
			t.Fatalf("built frame is not classified as a fragment frame: % x", frame)
		}
		got, gotChunk := parseFragFrame(frame)
		if got != h {
			t.Fatalf("header round-trip mismatch: %+v != %+v", got, h)
		}
		if string(gotChunk) != string(chunk) {
			t.Fatal("chunk round-trip mismatch")
		}
		if len(chunk) > 0 {
			saved := append([]byte(nil), chunk...)
			frame[fragHeaderLen] ^= 0xFF
			if string(chunk) != string(saved) {
				t.Fatal("buildFragFrame aliased the caller's chunk")
			}
		}
	})
}
