package tun

import "testing"

// The tun fragmentation hot path, benched as a full round trip: one outbound
// IP packet larger than the MTU is split into TFRG frames, each frame is
// reassembled by RouteInbound, and the completed packet is written to the
// interface. Sizes run at 1x, 2x and 3x the default MTU — 3x (4500 bytes) is
// the packet shape a game tick batch or a sync burst actually produces.
//
// The router is exercised with the real production path: NewTable +
// PeerAddr + NewRouter, a send function that appends each frame (fresh
// allocation per frame, matching the mesh dispatcher's copy-on-delivery
// contract the reassembler's retention relies on), and a fakeIface receiver.
// fragPendingLimit (64) and the fragID counter keep every iteration legal:
// each RouteOutbound allocates a fresh fragID, and each RouteInbound set
// completes and evicts its assembly, so no state accumulates across
// iterations.

// benchFragRouter builds the round-trip fixture: a router with one routed
// peer ("peer-b" owns 10.66.0.1), a send function capturing outgoing frames,
// and the interface that receives reassembled packets.
func benchFragRouter(b *testing.B) (*Router, *[][]byte, *fakeIface) {
	b.Helper()
	table, _ := NewTable("10.66.0.0/24")
	_, _ = table.PeerAddr("peer-b")
	var frames [][]byte
	iface := &fakeIface{}
	router := NewRouter(iface, table, func(peerID string, payload []byte) error {
		if peerID != "peer-b" {
			b.Fatalf("fragment routed to %q, want peer-b", peerID)
		}
		frames = append(frames, append([]byte(nil), payload...))
		return nil
	}, DefaultMTU)
	return router, &frames, iface
}

// benchFragRoundTripOnce drives one outbound packet through fragmentation
// and reassembly, returning the frames produced.
func benchFragRoundTripOnce(router *Router, frames *[][]byte, packet []byte) {
	*frames = (*frames)[:0]
	router.RouteOutbound(packet)
	if len(*frames) == 0 {
		return // dropped: oversized past the hard cap or unknown dst
	}
	for _, frame := range *frames {
		router.RouteInbound(frame)
	}
}

// BenchmarkTunFragRoundTrip measures the MTU×N outbound-fragment +
// inbound-reassemble round trip. Reported per-packet (one iteration is one
// full IP packet), with SetBytes carrying the ORIGINAL packet size — the
// wire bytes are frames + 12-byte headers, so MB/s here is the effective
// goodput of the intranet path.
//
// Baseline (Ryzen 5 3600X, linux, go1.25.9, 2026-09-14):
//
//	mtu1x        ~1023 ns/op   3199 B/op    2 allocs/op
//	mtu2x        ~3004 ns/op  12894 B/op   11 allocs/op
//	mtu3x        ~4673 ns/op  19563 B/op   13 allocs/op
//	mtu1x_plus_1 ~1929 ns/op   6728 B/op    9 allocs/op
func BenchmarkTunFragRoundTrip(b *testing.B) {
	for _, tc := range []struct {
		name string
		size int
	}{
		{"mtu1x", DefaultMTU},            // exactly at the MTU: verbatim forward, no frames
		{"mtu2x", DefaultMTU * 2},        // 3000 bytes: 3 frames (1488+1488+24)
		{"mtu3x", DefaultMTU * 3},        // 4500 bytes: 4 frames (3×1488+36)
		{"mtu1x_plus_1", DefaultMTU + 1}, // smallest fragmenting packet: 2 frames
	} {
		b.Run(tc.name, func(b *testing.B) {
			router, frames, iface := benchFragRouter(b)
			packet := ip4Packet("10.66.0.1", tc.size)

			// Correctness gate before timing: the round trip must deliver
			// byte-identical packets. Run once, verify, reset counters and
			// interface writes so the timed loop measures a steady state.
			benchFragRoundTripOnce(router, frames, packet)
			if got := len(iface.writes); got != 1 {
				b.Fatalf("round trip delivered %d packets, want 1", got)
			}
			if string(iface.writes[0]) != string(packet) {
				b.Fatal("reassembled packet differs from the original")
			}
			iface.writes = nil

			// Frame count expectation: at/below MTU the packet forwards
			// verbatim (0 TFRG frames); above it, ceil over the 1488-byte
			// chunk capacity.
			wantFrames := (tc.size + 1487) / 1488
			if tc.size <= DefaultMTU {
				wantFrames = 0
			}
			if got := router.Counters().FragSent.Load(); uint64(wantFrames) != got {
				b.Fatalf("FragSent = %d, want %d", got, wantFrames)
			}

			b.ReportAllocs()
			b.SetBytes(int64(tc.size))
			reassembledBefore := router.Counters().FragReassembled.Load()
			b.ResetTimer()
			for b.Loop() {
				benchFragRoundTripOnce(router, frames, packet)
			}
			b.StopTimer()

			// Every timed iteration must have completed one reassembly:
			// the delta, not the absolute counter (the pre-timer warm-up
			// above already incremented it).
			if got, want := router.Counters().FragReassembled.Load()-reassembledBefore, uint64(b.N); tc.size > DefaultMTU && got != want {
				b.Fatalf("FragReassembled grew by %d over %d iterations", got, want)
			}
			if tc.size <= DefaultMTU && router.Counters().FragReassembled.Load() != reassembledBefore {
				b.Fatal("at-MTU packet should not enter the reassembler")
			}
		})
	}
}

// BenchmarkTunFragHeaderParse isolates the per-frame demultiplexer cost every
// inbound directed payload pays: IsFragFrame classification plus the header
// split. This is the constant per-frame overhead the round trip above pays
// on top of the chunk copy.
//
// Baseline (Ryzen 5 3600X, linux, go1.25.9, 2026-09-14): ~5.6 ns/op, 0 B/op, 0 allocs/op.
func BenchmarkTunFragHeaderParse(b *testing.B) {
	router, frames, _ := benchFragRouter(b)
	packet := ip4Packet("10.66.0.1", DefaultMTU*3)
	*frames = (*frames)[:0]
	router.RouteOutbound(packet)
	if len(*frames) != 4 {
		b.Fatalf("MTU×3 packet fragmented into %d frames, want 4", len(*frames))
	}
	frame0 := (*frames)[0]
	// A reassembled frame must parse as a fragment (sanity: the fixture is
	// well-formed before we time the classifier on it).
	if !IsFragFrame(frame0) {
		b.Fatal("fixture frame failed IsFragFrame")
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if IsFragFrame(frame0) {
			h, chunk := parseFragFrame(frame0)
			if h.fragTotal != 4 || len(chunk) != 1488 {
				b.Fatalf("parsed header %+v, chunk %d bytes", h, len(chunk))
			}
		}
	}
}
