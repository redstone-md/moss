package transport

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"syscall"
)

// ErrBindInterfaceUnsupported is returned by the OS-specific helpers when the
// current platform has no equivalent of IP_UNICAST_IF / SO_BINDTODEVICE.
var ErrBindInterfaceUnsupported = errors.New("bind interface unsupported on this platform")

// ResolveBindInterface accepts either the OS interface name (e.g. "Ethernet",
// "en0") or its numeric index (e.g. "3"). It returns the resolved index after
// validating that the interface exists, is up, and is not a loopback.
//
// Returns 0 (and a nil error) for an empty spec — callers should treat that
// as "feature disabled" and skip applying the bind.
func ResolveBindInterface(spec string) (int, error) {
	if spec == "" {
		return 0, nil
	}
	var iface *net.Interface
	var err error
	if idx, parseErr := strconv.Atoi(spec); parseErr == nil {
		iface, err = net.InterfaceByIndex(idx)
	} else {
		iface, err = net.InterfaceByName(spec)
	}
	if err != nil {
		return 0, fmt.Errorf("bind interface %q: %w", spec, err)
	}
	if iface.Flags&net.FlagLoopback != 0 {
		return 0, fmt.Errorf("bind interface %q is loopback", spec)
	}
	if iface.Flags&net.FlagUp == 0 {
		return 0, fmt.Errorf("bind interface %q is down", spec)
	}
	return iface.Index, nil
}

// ApplyBindToUDP applies the interface bind (if any) to a freshly-created
// *net.UDPConn. A zero ifIndex is a no-op so call sites can call this
// unconditionally when configured.
func ApplyBindToUDP(conn *net.UDPConn, ifIndex int) error {
	if ifIndex == 0 {
		return nil
	}
	rc, err := conn.SyscallConn()
	if err != nil {
		return fmt.Errorf("bind interface: SyscallConn: %w", err)
	}
	return applyBindInterface(rc, ifIndex)
}

// applyBindToPacket applies the interface bind to a freshly-created
// net.PacketConn (used by LAN discovery's net.ListenPacket call sites).
func applyBindToPacket(conn net.PacketConn, ifIndex int) error {
	if ifIndex == 0 {
		return nil
	}
	udp, ok := conn.(*net.UDPConn)
	if !ok {
		return fmt.Errorf("bind interface: unsupported PacketConn type %T", conn)
	}
	return ApplyBindToUDP(udp, ifIndex)
}

// DialerWithBind returns a *net.Dialer whose freshly-created sockets are
// pinned to the named interface index via the OS-specific helper. A zero
// ifIndex yields an unmodified Dialer with no Control hook.
//
// Use this for net.Dial-flavoured call sites — UDP trackers, NAT-PMP probes,
// any callers that need outbound traffic to skip the routing table.
func DialerWithBind(base net.Dialer, ifIndex int) *net.Dialer {
	if ifIndex == 0 {
		return &base
	}
	d := base
	prev := d.Control
	d.Control = func(network, address string, c syscall.RawConn) error {
		if prev != nil {
			if err := prev(network, address, c); err != nil {
				return err
			}
		}
		return applyBindInterface(c, ifIndex)
	}
	return &d
}
