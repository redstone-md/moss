// Package tun implements the virtual-intranet data plane of moss: an
// interface abstraction for packet sources/sinks (a real TUN device, a
// loopback, a test double), a virtual-IP→peer table, and a router that
// forwards IP packets between a local interface and mesh peers.
//
// The design is deliberately cgo-free: a real TUN device plugs in as a
// PacketIface implementation (fd reads/writes wrapped in ReadPacket /
// WritePacket), so the mesh package never links against a TUN library and
// the routing logic is testable in userspace.
//
// v2 adds fragmentation and beyond-the-pool routing: a packet larger than
// the MTU is split into TFRG fragment frames carried as ordinary directed
// payloads (reassembled at the far edge, hard-capped at 64KB), and a
// prefix→peers Routes table routes destinations outside the assigned-IP
// pool by lowest RTT. The default MTU of 1500 matches the conservative
// end-to-end assumption of the underlying mesh transport
// (Security.MaxMessageSizeBytes gates the directed payload at 64KB, so a
// 1500-byte packet passes with headroom).
package tun

// PacketIface is a packet source/sink: the local edge of the intranet.
//
// The shape mirrors the transport carrier's packet methods (WritePacket /
// ReadPacket / Close) minus the address bookkeeping: an interface here has
// no remote peer — every packet written to it was routed, and every packet
// read from it wants routing.
type PacketIface interface {
	// ReadPacket blocks until one packet is available, then returns it.
	// Implementations must treat the returned slice as borrowed: the
	// router does not retain it past the call that produced it, but the
	// caller may write to the interface from other goroutines meanwhile.
	// Close must unblock a pending ReadPacket with an error.
	ReadPacket() ([]byte, error)
	// WritePacket delivers one routed packet to the interface.
	WritePacket([]byte) error
	// Close shuts the interface down. Subsequent ReadPacket calls must
	// return an error (net.ErrClosed conventionally), WritePacket must
	// fail, and Close itself must be idempotent.
	Close() error
}

// DefaultMTU is the intranet's maximum IP packet size. A packet larger than
// the active MTU is fragmented on the outbound path (v2) and reassembled on
// the inbound path; a single payload larger than the MTU is dropped and
// counted inbound.
const DefaultMTU = 1500
