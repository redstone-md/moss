package tun

import (
	"net/netip"
	"sync"
	"sync/atomic"
	"time"
)

// SendFunc delivers one outbound payload to one mesh peer. The mesh binding
// implements it with Node.SendToPeer; keeping it as a function type keeps
// this package free of the mesh dependency.
type SendFunc func(peerID string, payload []byte) error

// Counters are the router's monotonic drop/delivery statistics. Every field
// is an atomic that only ever grows; readers snapshot without locking.
type Counters struct {
	// Forwarded counts packets read from the interface and handed to the
	// mesh send function for a known virtual destination — one per packet,
	// including fragmented ones.
	Forwarded atomic.Uint64
	// InboundDelivered counts directed payloads received from the mesh
	// and written to the interface.
	InboundDelivered atomic.Uint64
	// DroppedUnknownDst counts outbound packets whose destination has no
	// peer in the table (unassigned virtual IP, or outside the pool).
	DroppedUnknownDst atomic.Uint64
	// DroppedOversize counts packets dropped for exceeding the MTU on
	// either direction — an INBOUND oversize, or an outbound packet over
	// the fragmentation hard cap. Outbound packets between the MTU and
	// the hard cap are fragmented (v2), not dropped.
	DroppedOversize atomic.Uint64
	// DroppedMalformed counts packets too short to carry an IPv4 header.
	DroppedMalformed atomic.Uint64
	// SendFailed counts mesh send function failures for otherwise valid
	// outbound packets.
	SendFailed atomic.Uint64
	// FragSent counts fragment frames handed to the mesh send function
	// (one per frame, not per packet).
	FragSent atomic.Uint64
	// FragReassembled counts inbound fragment sets completed into one
	// packet and delivered toward the interface.
	FragReassembled atomic.Uint64
	// FragDuplicate counts inbound fragment frames whose (fragID, index)
	// was already received — kept, not re-stored; the duplicate itself is
	// a benign retransmit or a replay probe.
	FragDuplicate atomic.Uint64
	// FragExpired counts reassemblies dropped because their first frame
	// aged past fragTTL without completing.
	FragExpired atomic.Uint64
	// FragPendingOverflow counts inbound fragment frames dropped because
	// the reassembly table already holds fragPendingLimit incomplete
	// packets.
	FragPendingOverflow atomic.Uint64
	// FragOversize counts drops where fragmentation is the reason but
	// oversize is the mechanism: an outbound packet over the hard cap, or
	// an inbound reassembly whose chunks sum past the hard cap (evicted).
	FragOversize atomic.Uint64
	// FragMalformed counts inbound fragment frames with a valid TFRG
	// magic but an invalid header (zero total, index beyond total, or a
	// reassembled buffer that is not an IPv4 packet).
	FragMalformed atomic.Uint64
}

// Router moves IP packets between a local PacketIface and the mesh:
//
//   - outbound: packets read from the interface are parsed for their
//     IPv4 destination, looked up in the virtual-IP table (then, on a
//     miss, in the wired Routes table), and handed to the send function
//     addressed to the owning peer; a packet over the MTU is fragmented
//     into TFRG frames (up to the hard cap), each sent as its own
//     directed payload;
//   - inbound: directed payloads arriving from the mesh are written to
//     the interface, addressed to this node's virtual IP; fragment
//     frames are reassembled first, and only a completed packet is
//     written.
//
// Both directions enforce the MTU on the WIRE: a single payload over the
// MTU is dropped inbound; outbound, oversize up to fragHardCap becomes
// fragments, and beyond it is dropped (v2).
type Router struct {
	iface PacketIface
	table *Table
	send  SendFunc
	mtu   int
	count Counters
	// routes is the beyond-the-pool routing table consulted after the
	// assigned-IP table misses. Nil disables the fallback.
	routes *Routes
	// frags is the inbound reassembly state, keyed by fragID.
	frags map[uint32]*fragAssembly
	// fragMu guards frags.
	fragMu sync.Mutex
	// fragID is the sender-side fragment ID sequence (v2). Atomic: the
	// data plane is the only writer, but readers (tests, diagnostics)
	// must not race it.
	fragID atomic.Uint32
	// dropFn is the optional telemetry sink for fragment drop reasons;
	// the mesh binding wires it to countInbound. Nil-safe.
	dropFn func(reason string)
	// now is swappable for tests; production gets time.Now.
	now func() time.Time
}

// NewRouter builds a router over iface and table, sending outbound packets
// through send. A non-positive mtu falls back to DefaultMTU.
func NewRouter(iface PacketIface, table *Table, send SendFunc, mtu int) *Router {
	if mtu <= 0 {
		mtu = DefaultMTU
	}
	return &Router{
		iface: iface,
		table: table,
		send:  send,
		mtu:   mtu,
		frags: make(map[uint32]*fragAssembly),
		now:   time.Now,
	}
}

// SetRoutes wires the beyond-the-pool routing table consulted when the
// assigned-IP table misses. It must be called before routing begins (the
// mesh binding calls it inside AttachTun, before the pump starts); swapping
// mid-flight is not supported.
func (r *Router) SetRoutes(routes *Routes) {
	r.routes = routes
}

// SetDropCounter wires the optional telemetry sink for fragment drop
// reasons ("__tun_frag_expired__" and friends). The mesh binding wires it to
// the node's countInbound so fragment drops become per-type inbound counts
// with zero new plumbing. Nil disables the sink; it must be set before
// routing begins.
func (r *Router) SetDropCounter(fn func(reason string)) {
	r.dropFn = fn
}

// dropFrag records a fragment-related drop: counter plus telemetry hook.
func (r *Router) dropFrag(c *atomic.Uint64, reason string) {
	c.Add(1)
	if r.dropFn != nil {
		r.dropFn(reason)
	}
}

// Counters returns the router's statistics. The returned pointer is valid
// for the router's lifetime; readers observe the counters atomically.
func (r *Router) Counters() *Counters {
	return &r.count
}

// RouteOutbound processes one packet read from the interface: MTU/frag
// gate, IPv4 header parse, destination lookup, mesh send. The packet is
// consumed synchronously; the send function's timeout policy belongs to the
// caller.
//
// Size policy (v2): a packet at or under the MTU takes the v1 path
// verbatim. A packet over the MTU is fragmented into TFRG frames only when
// its destination resolves; a packet over fragHardCap is dropped — the mesh
// payload gate would reject its chunks anyway at that size.
func (r *Router) RouteOutbound(packet []byte) {
	oversize := len(packet) > r.mtu
	if oversize && len(packet) > fragHardCap {
		// Over the hard cap there is no fragmentation story: the frame
		// would need a single chunk past the directed-payload ceiling.
		r.dropFrag(&r.count.FragOversize, "__tun_frag_hardcap__")
		return
	}
	dst, ok := ipv4Dst(packet)
	if !ok {
		r.count.DroppedMalformed.Add(1)
		return
	}
	peerID, ok := r.resolve(dst)
	if !ok {
		r.count.DroppedUnknownDst.Add(1)
		return
	}
	if !oversize {
		if err := r.send(peerID, packet); err != nil {
			r.count.SendFailed.Add(1)
			return
		}
		r.count.Forwarded.Add(1)
		return
	}
	r.sendFragments(peerID, packet)
}

// resolve finds the peer that owns dst: the assigned-IP table first, then
// the wired Routes table on a miss. A nil Routes is a plain miss.
func (r *Router) resolve(dst netip.Addr) (string, bool) {
	if peerID, ok := r.table.LookupAddr(dst); ok {
		return peerID, true
	}
	if r.routes == nil {
		return "", false
	}
	return r.routes.Lookup(dst)
}

// sendFragments splits packet into TFRG frames and sends them to peerID.
// The first frame failure aborts the packet: half-delivered fragments only
// pin the receiver's reassembly table until fragTTL, and a mid-packet abort
// keeps the receiver's assembly eviction cheap rather than waiting out a
// doomed reassembly.
func (r *Router) sendFragments(peerID string, packet []byte) {
	chunkLen := r.mtu - fragHeaderLen
	if chunkLen <= 0 {
		// A sub-header MTU cannot carry a fragment at all; nothing to
		// do but drop (the interface was misconfigured).
		r.count.DroppedOversize.Add(1)
		return
	}
	total := (len(packet) + chunkLen - 1) / chunkLen
	if total > int(^uint16(0)) {
		// Out of fragIndex range: only reachable with an absurdly tiny
		// MTU (the hard cap otherwise bounds total at 45 with MTU 1500).
		r.dropFrag(&r.count.FragOversize, "__tun_frag_hardcap__")
		return
	}
	fragID := r.fragID.Add(1)
	h := fragHeader{fragID: fragID, fragTotal: uint16(total)}
	for off, idx := 0, uint16(0); off < len(packet); off, idx = off+chunkLen, idx+1 {
		end := off + chunkLen
		if end > len(packet) {
			end = len(packet)
		}
		h.fragIndex = idx
		frame := buildFragFrame(h, packet[off:end])
		if err := r.send(peerID, frame); err != nil {
			// Abort on first failure: the receiver will evict the
			// partial assembly at fragTTL; re-sending the rest would
			// double the traffic of a doomed delivery.
			r.count.SendFailed.Add(1)
			return
		}
		r.count.FragSent.Add(1)
	}
	r.count.Forwarded.Add(1)
}

// RouteInbound processes one directed payload received from the mesh: a
// whole IP packet is written to the interface; a TFRG fragment frame is
// reassembled with its siblings first, and only the completed packet is
// written. The MTU gate applies to each payload (whole packets and fragment
// chunks alike); the sendFunc-side size gate already caps directed payloads
// at Security.MaxMessageSizeBytes, so a malicious oversized packet is
// dropped here rather than trusted.
func (r *Router) RouteInbound(payload []byte) {
	if len(payload) > r.mtu {
		r.count.DroppedOversize.Add(1)
		return
	}
	if IsFragFrame(payload) {
		r.routeInboundFrag(payload)
		return
	}
	if _, ok := ipv4Dst(payload); !ok {
		r.count.DroppedMalformed.Add(1)
		return
	}
	if err := r.iface.WritePacket(payload); err != nil {
		// A closed/failing interface is a delivery failure: count it as
		// a drop rather than crashing the mesh receive path.
		r.count.SendFailed.Add(1)
		return
	}
	r.count.InboundDelivered.Add(1)
}

// routeInboundFrag folds one fragment frame into the reassembly table. The
// reassembly is keyed on fragID alone (the binding's inbound path is
// per-sender at the envelope level; a cross-sender collision would require
// two peers guessing the same 32-bit ID inside the same 5-second window).
// Chunks are retained by reference: the mesh dispatcher copies each inbound
// payload before delivery, so the stored slices are assembly-owned.
func (r *Router) routeInboundFrag(payload []byte) {
	h, chunk := parseFragFrame(payload)
	if h.fragTotal == 0 || h.fragIndex >= h.fragTotal {
		r.dropFrag(&r.count.FragMalformed, "__tun_frag_malformed__")
		return
	}
	now := r.now()
	r.fragMu.Lock()
	// Lazy expiry: no goroutine sweeps the table; every arriving frame
	// pays the sweep, which keeps the reassembly bounded by real traffic.
	for id, fa := range r.frags {
		if now.After(fa.deadline) {
			r.dropFrag(&r.count.FragExpired, "__tun_frag_expired__")
			delete(r.frags, id)
		}
	}
	fa, ok := r.frags[h.fragID]
	if !ok {
		// A completed assembly is evicted on completion; a frame for
		// an evicted ID starts a fresh (usually doomed) assembly that
		// the TTL will clean up.
		if len(r.frags) >= fragPendingLimit {
			r.fragMu.Unlock()
			r.dropFrag(&r.count.FragPendingOverflow, "__tun_frag_pending_overflow__")
			return
		}
		fa = &fragAssembly{
			total:    h.fragTotal,
			deadline: now.Add(fragTTL),
			chunks:   make(map[uint16][]byte),
		}
		r.frags[h.fragID] = fa
	}
	// A spoofed redefinition of total cannot grow the assembly: the
	// reassembly enforces its own sum cap and completes only on the
	// count of stored chunks, not on the header's claim.
	if fa.total != h.fragTotal {
		r.fragMu.Unlock()
		r.dropFrag(&r.count.FragMalformed, "__tun_frag_malformed__")
		return
	}
	if _, dup := fa.chunks[h.fragIndex]; dup {
		r.fragMu.Unlock()
		r.dropFrag(&r.count.FragDuplicate, "__tun_frag_duplicate__")
		return
	}
	if fa.sum+len(chunk) > fragHardCap {
		delete(r.frags, h.fragID)
		r.fragMu.Unlock()
		r.dropFrag(&r.count.FragOversize, "__tun_frag_oversize__")
		return
	}
	fa.chunks[h.fragIndex] = chunk
	fa.sum += len(chunk)
	if len(fa.chunks) < int(fa.total) {
		r.fragMu.Unlock()
		return
	}
	// Complete: assemble outside the lock.
	delete(r.frags, h.fragID)
	r.fragMu.Unlock()
	packet := make([]byte, fa.sum)
	off := 0
	for i := uint16(0); i < fa.total; i++ {
		copy(packet[off:], fa.chunks[i])
		off += len(fa.chunks[i])
	}
	if _, ok := ipv4Dst(packet); !ok {
		// The reassembled buffer must be an IP packet; a peer whose
		// fragments assemble into garbage is counted, not trusted.
		r.dropFrag(&r.count.FragMalformed, "__tun_frag_malformed__")
		return
	}
	r.count.FragReassembled.Add(1)
	if err := r.iface.WritePacket(packet); err != nil {
		r.count.SendFailed.Add(1)
		return
	}
	r.count.InboundDelivered.Add(1)
}

// ipv4Dst extracts the destination address of an IPv4 packet. The minimum
// check is a version-4 first nibble and a 20-byte header; the IP total
// length field is NOT enforced (the mesh may pad, and a real TUN device
// can deliver a packet whose trailing padding exceeds the declared length
// — the MTU gate on the wire bytes is the authority on size).
func ipv4Dst(packet []byte) (netip.Addr, bool) {
	if len(packet) < 20 {
		return netip.Addr{}, false
	}
	if packet[0]>>4 != 4 {
		return netip.Addr{}, false
	}
	var addr [4]byte
	copy(addr[:], packet[16:20])
	return netip.AddrFrom4(addr), true
}

// IsIPv4 reports whether data plausibly carries an IPv4 packet: a version-4
// first nibble on a header-length-minimum buffer. It is the packet-class
// demultiplexer the mesh binding uses to split IP traffic (routed into the
// intranet) from application payloads (passed to a previously registered
// packet callback).
//
// Residual ambiguity, accepted in v1: a non-IP blob whose first byte is
// 0x4X and whose length is at least 20 is classified as IPv4. Real-world
// application payloads that start with 0x40-0x4F and exceed 20 bytes are
// rare (ASCII text starts at 0x20, binary protocols overwhelmingly use a
// magic number or a length prefix in a different range); an application
// whose payloads genuinely live in that range must not share the packet
// callback with an attached intranet.
func IsIPv4(data []byte) bool {
	_, ok := ipv4Dst(data)
	return ok
}

// IPv4HeaderLen returns the declared IPv4 header length in bytes (IHL
// field), or 0 when the buffer cannot carry a valid IHL. Callers can use
// it to validate before parsing transport-layer headers.
func IPv4HeaderLen(data []byte) int {
	if len(data) < 20 || data[0]>>4 != 4 {
		return 0
	}
	return int(data[0]&0x0f) * 4
}
