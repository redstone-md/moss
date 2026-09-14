//go:build darwin

// meshlan tun_darwin.go: the utun kernel control socket, cgo-free.
//
// macOS has no /dev/net/tun; its TUN device ("utun") is a kernel control
// socket: a socket in the AF_SYSTEM / AF_SYS_CONTROL family, connected to
// the "com.apple.net.utun_control" kernel control. The connection's unit
// number is the utun unit number plus one (utun units count from 1); after
// connect, the socket fd speaks the utun packet protocol: every packet is
// prefixed with a 4-byte host-order protocol family (AF_INET / AF_INET6),
// in both directions.
//
// The interface name is not known until after connect — the kernel
// assigns "utunN" by unit number — so it is fetched with a getsockopt
// (UTUN_OPT_IFNAME) after the socket is up.
//
// The wireguard-go project drives the identical sequence without cgo;
// this implementation follows it (all pieces — CtlInfo, IoctlCtlInfo,
// SockaddrCtl, GetsockoptString — are in golang.org/x/sys/unix). The two
// literals x/sys does not export are defined below: SYSPROTO_CONTROL (the
// protocol of the kernel-control family) and UTUN_OPT_IFNAME (the
// getsockopt that returns the assigned interface name).
package meshlan

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/sys/unix"

	"github.com/redstone-md/moss/internal/tun"
)

// utunControlName is the kernel control id of the utun driver.
const utunControlName = "com.apple.net.utun_control"

// utunProtoOffset is the utun packet preamble: 4 bytes of host-order
// address family before every packet, read AND write.
const utunProtoOffset = 4

// utunSysprotoControl is SYSPROTO_CONTROL from <sys/kern_control.h>: the
// protocol number of the kernel-control socket family. x/sys does not
// export it on darwin; wireguard-go passes the same literal.
const utunSysprotoControl = 2

// utunOptIfname is UTUN_OPT_IFNAME from <net/if_utun.h>: the getsockopt
// option (on the SYSPROTO_CONTROL level) that returns the connected
// interface's name.
const utunOptIfname = 2

// darwinTun is a utun kernel control socket as a tun.PacketIface.
type darwinTun struct {
	f      *os.File
	name   string
	mu     sync.Mutex
	closed bool
}

// openTunOS connects to the utun kernel control. name is "utunN" or
// ""/"utun" (next free unit); the actual assigned name is fetched after
// connect via getsockopt.
func openTunOS(name string) (tun.PacketIface, string, error) {
	unit := -1
	if name != "" && name != "utun" {
		// Strict parse: "utun12abc" must be rejected, not silently
		// read as unit 12 (Sscanf stops at the first non-digit and
		// would accept the trailing garbage).
		digits := strings.TrimPrefix(name, "utun")
		if digits == name {
			return nil, "", fmt.Errorf("moss-lan: interface name %q: must be \"utun\" or \"utunN\"", name)
		}
		n, err := strconv.Atoi(digits)
		if err != nil || n < 0 {
			return nil, "", fmt.Errorf("moss-lan: interface name %q: must be \"utun\" or \"utunN\"", name)
		}
		unit = n
	}

	// SOCK_DGRAM on the kernel-control family. CloseOnExec below keeps
	// the fd out of spawned processes' tables (unix.Socket itself does
	// not set CLOEXEC).
	fd, err := unix.Socket(unix.AF_SYSTEM, unix.SOCK_DGRAM, utunSysprotoControl)
	if err != nil {
		return nil, "", fmt.Errorf("moss-lan: utun control socket (needs root): %w", err)
	}
	unix.CloseOnExec(fd)

	// Resolve the control id from its name, then connect to unit+1
	// (the kernel numbers utun units from 1). Unit 0 picks the next
	// free unit.
	ctlInfo := &unix.CtlInfo{}
	copy(ctlInfo.Name[:], utunControlName)
	if err := unix.IoctlCtlInfo(fd, ctlInfo); err != nil {
		unix.Close(fd)
		return nil, "", fmt.Errorf("moss-lan: resolve utun control %q: %w", utunControlName, err)
	}
	scUnit := uint32(unit + 1)
	if unit < 0 {
		scUnit = 0
	}
	if err := unix.Connect(fd, &unix.SockaddrCtl{ID: ctlInfo.Id, Unit: scUnit}); err != nil {
		unix.Close(fd)
		return nil, "", fmt.Errorf("moss-lan: connect utun (needs root): %w", err)
	}

	// The interface name arrives by getsockopt — UTUN_OPT_IFNAME on the
	// SYSPROTO_CONTROL level.
	ifName, err := unix.GetsockoptString(fd, utunSysprotoControl, utunOptIfname)
	if err != nil {
		unix.Close(fd)
		return nil, "", fmt.Errorf("moss-lan: read utun interface name: %w", err)
	}

	// Nonblocking before NewFile registers the socket with kqueue; a
	// blocking read would tie up a thread and never wake on Close.
	if err := unix.SetNonblock(fd, true); err != nil {
		unix.Close(fd)
		return nil, "", fmt.Errorf("moss-lan: set O_NONBLOCK on %s: %w", ifName, err)
	}

	return &darwinTun{
		f:    os.NewFile(uintptr(fd), ifName),
		name: ifName,
	}, ifName, nil
}

// Name returns the kernel-assigned interface name ("utun5").
func (t *darwinTun) Name() string { return t.name }

// ReadPacket blocks until one packet arrives, strips the utun protocol
// preamble, and returns the IP packet. The slice is freshly allocated
// per read: ownership transfers with the return, the same contract
// tun.Loopback's copy-on-queue provides.
func (t *darwinTun) ReadPacket() ([]byte, error) {
	// One MTU+preamble-sized buffer per packet keeps the copy to one hop:
	// buf includes the preamble, the returned slice is the packet.
	buf := make([]byte, utunProtoOffset+tun.DefaultMTU)
	for {
		n, err := t.f.Read(buf)
		if err != nil {
			return nil, asNetClosed(err)
		}
		if n == 0 {
			// EOF on the control socket: the interface went away.
			return nil, asNetClosed(os.ErrClosed)
		}
		if n <= utunProtoOffset {
			// Preamble-only: not a packet. Loop rather than hand the
			// router a zero-length "packet" it would count as malformed.
			continue
		}
		return buf[utunProtoOffset:n], nil
	}
}

// WritePacket delivers one IP packet to the kernel, prefixing the utun
// protocol preamble (host-order address family derived from the packet's
// IP version nibble).
func (t *darwinTun) WritePacket(packet []byte) error {
	if len(packet) == 0 {
		return nil
	}
	var family byte
	switch packet[0] >> 4 {
	case 4:
		family = unix.AF_INET
	case 6:
		family = unix.AF_INET6
	default:
		return fmt.Errorf("moss-lan: utun write: not an IP packet (version nibble %d)", packet[0]>>4)
	}
	// A single contiguous buffer per write: preamble + payload, one
	// syscall. The first three preamble bytes are zero, the fourth is
	// the host-order family (Apple's protocol; wireguard-go identical).
	buf := make([]byte, utunProtoOffset+len(packet))
	buf[3] = family
	copy(buf[utunProtoOffset:], packet)
	_, err := t.f.Write(buf)
	return asNetClosed(err)
}

// Close shuts the interface down and unblocks any parked ReadPacket. The
// utun interface disappears with the fd. Idempotent: after the first
// call every later Close returns nil.
func (t *darwinTun) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil
	}
	t.closed = true
	return t.f.Close()
}
