package tun

import (
	"net/netip"
	"sync/atomic"
)

// SendFunc delivers one outbound payload to one mesh peer. The mesh binding
// implements it with Node.SendToPeer; keeping it as a function type keeps
// this package free of the mesh dependency.
type SendFunc func(peerID string, payload []byte) error

// Counters are the router's monotonic drop/delivery statistics. Every field
// is an atomic that only ever grows; readers snapshot without locking.
type Counters struct {
	// Forwarded counts packets read from the interface and handed to the
	// mesh send function for a known virtual destination.
	Forwarded atomic.Uint64
	// InboundDelivered counts directed payloads received from the mesh
	// and written to the interface.
	InboundDelivered atomic.Uint64
	// DroppedUnknownDst counts outbound packets whose destination has no
	// peer in the table (unassigned virtual IP, or outside the pool).
	DroppedUnknownDst atomic.Uint64
	// DroppedOversize counts packets dropped for exceeding the MTU on
	// either direction. v1 has no fragmentation: oversize is a drop.
	DroppedOversize atomic.Uint64
	// DroppedMalformed counts packets too short to carry an IPv4 header.
	DroppedMalformed atomic.Uint64
	// SendFailed counts mesh send function failures for otherwise valid
	// outbound packets.
	SendFailed atomic.Uint64
}

// Router moves IP packets between a local PacketIface and the mesh:
//
//   - outbound: packets read from the interface are parsed for their
//     IPv4 destination, looked up in the virtual-IP table, and handed to
//     the send function addressed to the owning peer;
//   - inbound: directed payloads arriving from the mesh are written to
//     the interface, addressed to this node's virtual IP.
//
// Both directions enforce the MTU: an oversize packet is dropped and
// counted, never fragmented (v1, fixated).
type Router struct {
	iface PacketIface
	table *Table
	send  SendFunc
	mtu   int
	count Counters
}

// NewRouter builds a router over iface and table, sending outbound packets
// through send. A non-positive mtu falls back to DefaultMTU.
func NewRouter(iface PacketIface, table *Table, send SendFunc, mtu int) *Router {
	if mtu <= 0 {
		mtu = DefaultMTU
	}
	return &Router{iface: iface, table: table, send: send, mtu: mtu}
}

// Counters returns the router's statistics. The returned pointer is valid
// for the router's lifetime; readers observe the counters atomically.
func (r *Router) Counters() *Counters {
	return &r.count
}

// RouteOutbound processes one packet read from the interface: MTU gate,
// IPv4 header parse, destination lookup, mesh send. The packet is consumed
// synchronously; the send function's timeout policy belongs to the caller.
func (r *Router) RouteOutbound(packet []byte) {
	if len(packet) > r.mtu {
		r.count.DroppedOversize.Add(1)
		return
	}
	dst, ok := ipv4Dst(packet)
	if !ok {
		r.count.DroppedMalformed.Add(1)
		return
	}
	peerID, ok := r.table.LookupAddr(dst)
	if !ok {
		r.count.DroppedUnknownDst.Add(1)
		return
	}
	if err := r.send(peerID, packet); err != nil {
		r.count.SendFailed.Add(1)
		return
	}
	r.count.Forwarded.Add(1)
}

// RouteInbound processes one directed payload received from the mesh: the
// payload must be an IP packet within the MTU, and is written to the
// interface. The sendFunc-side size gate already caps directed payloads at
// Security.MaxMessageSizeBytes, so a malicious oversized packet is dropped
// here rather than trusted.
func (r *Router) RouteInbound(payload []byte) {
	if len(payload) > r.mtu {
		r.count.DroppedOversize.Add(1)
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
