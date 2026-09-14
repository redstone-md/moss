package tun

import (
	"encoding/binary"
	"time"
)

// Fragmentation wire format (v2). An outbound IP packet larger than the MTU
// is split into fragment frames, each carried as the payload of an ordinary
// directed envelope (gossip.TypeDirect) — the mesh transport is unaware of
// fragmentation. A fragment frame is a 12-byte header followed by one chunk
// of the original packet:
//
//	 0  1  2  3   magic "TFRG" (0x54 0x46 0x52 0x47)
//	 4  5  6  7   fragID   uint32, big-endian — identifies the packet being
//	             reassembled; assigned by the sender, unique per sender
//	 8  9        fragIndex uint16, big-endian — 0-based chunk ordinal
//	10 11        fragTotal uint16, big-endian — chunk count, >= 1
//	12 ...       chunk bytes
//
// The magic's first nibble (0x5) keeps IsIPv4 from classifying a fragment
// frame as an IP packet, so a node without an attached intranet passes it
// to the application callback instead of misrouting it. The reassembly key
// is the bare fragID: a cross-sender collision would need two peers to
// pick the same 32-bit ID inside one fragTTL window, and the pending cap
// plus the per-assembly sum cap bound the damage either way.
const (
	// fragHeaderLen is the byte size of the fragment header.
	fragHeaderLen = 12
	// fragTTL bounds how long the receiving side keeps a partially
	// reassembled packet before dropping it. It is measured from the FIRST
	// fragment of that packet ID, so late duplicates of a completed packet
	// find no state and are dropped as unknown duplicates.
	fragTTL = 5 * time.Second
	// fragPendingLimit bounds how many distinct incomplete packets the
	// reassembler will hold. The (fragID, deadline) state is tiny, but the
	// chunks are not: a peer spraying half-packets is a memory attack
	// vector, so the table is capped and overflowing IDs are dropped.
	fragPendingLimit = 64
	// fragHardCap is the largest IP packet the outbound path will ever
	// fragment. It matches Security.MaxMessageSizeBytes: a reassembled
	// packet can never exceed the directed-payload cap, so a spoofed
	// fragTotal cannot pin more than that per assembly.
	fragHardCap = 65536
)

// fragMagic marks a directed payload as a fragment frame.
var fragMagic = [4]byte{'T', 'F', 'R', 'G'}

// IsFragFrame reports whether data carries a fragment frame: the TFRG magic
// on a buffer at least the size of the header. It is the demultiplexer the
// mesh binding uses to split fragment frames (reassembled into the intranet)
// from whole IP packets (written through) before any header parsing — the
// same split IsIPv4 performs for the IP class. Byte-level classification is
// the contract; a coincidental "TFRG" application payload is indistinguishable
// and must not share the packet callback with an attached intranet (the same
// residual-ambiguity caveat as IsIPv4's 0x4X range).
func IsFragFrame(data []byte) bool {
	return len(data) >= fragHeaderLen && string(data[:4]) == string(fragMagic[:])
}

// fragHeader splits a validated fragment frame's header.
type fragHeader struct {
	fragID    uint32
	fragIndex uint16
	fragTotal uint16
}

// parseFragFrame decodes the header of a known-fragment payload. The caller
// has checked IsFragFrame. The returned chunk is a borrowed subslice of
// frame; the reassembler stores it verbatim — inbound payloads are copied by
// the mesh dispatcher before this point, so retention is safe.
func parseFragFrame(frame []byte) (fragHeader, []byte) {
	var h fragHeader
	h.fragID = binary.BigEndian.Uint32(frame[4:8])
	h.fragIndex = binary.BigEndian.Uint16(frame[8:10])
	h.fragTotal = binary.BigEndian.Uint16(frame[10:12])
	return h, frame[fragHeaderLen:]
}

// buildFragFrame builds one fresh fragment frame for (h, chunk). The frame
// is a newly allocated buffer: every chunk slice handed here is copied in, so
// the mesh's asynchronous outbound queue never aliases the caller's packet —
// the queue marshals payloads after RouteOutbound has returned.
func buildFragFrame(h fragHeader, chunk []byte) []byte {
	frame := make([]byte, 0, fragHeaderLen+len(chunk))
	frame = append(frame, fragMagic[:]...)
	frame = binary.BigEndian.AppendUint32(frame, h.fragID)
	frame = binary.BigEndian.AppendUint16(frame, h.fragIndex)
	frame = binary.BigEndian.AppendUint16(frame, h.fragTotal)
	frame = append(frame, chunk...)
	return frame
}

// fragAssembly is the receiving-side state for one in-progress packet.
type fragAssembly struct {
	total    uint16
	deadline time.Time
	// chunks holds one entry per received index, borrowed from the inbound
	// payload (already a dispatcher-owned copy).
	chunks map[uint16][]byte
	// sum is the total chunk bytes received so far; it enforces the hard
	// cap without iterating the map.
	sum int
}
