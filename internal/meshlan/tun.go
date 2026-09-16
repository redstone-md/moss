// meshlan tun.go: OpenTun, the OS TUN-device constructor for moss-lan.
//
// The core's tun package is deliberately device-free — the intranet data
// plane runs over any tun.PacketIface, so the routing logic never links
// against a TUN library. moss-lan needs the real device, and this file is
// where it plugs in: OpenTun opens a kernel TUN interface on the host OS
// and returns it as a tun.PacketIface ready for mesh.AttachTun.
//
// cgo-free by design, matching the core's constraint (build tags select
// the implementation; no TUN library dependency):
//
//	linux   /dev/net/tun + TUNSETIFF ioctl (tun_linux.go)
//	darwin  utun kernel control socket (tun_darwin.go)
//	windows ErrNotImplemented — wintun.dll adapter (tun_windows.go)
//	other   ErrNotImplemented (tun_other.go)
//
// The OS files each define openTunOS(name); the linker picks exactly one,
// so this file carries no build tag of its own.
package meshlan

import (
	"errors"
	"fmt"
	"net"
	"os"

	"github.com/redstone-md/moss/internal/tun"
)

// ErrNotImplemented is returned by OpenTun on platforms whose TUN support
// is not built yet. It is a distinct sentinel (not a generic error) so
// callers can branch on it: a moss-lan binary on an unsupported OS can
// fall back to a tun.Loopback-based edge or print an actionable message
// instead of reporting a mystery failure.
var ErrNotImplemented = errors.New("moss-lan: TUN support not implemented for this OS")

// tunNameMax is the longest interface name any supported OS accepts:
// IFNAMSIZ-1 on Linux (15 chars + NUL). Darwin's utunN is shorter in
// practice; the OS files tighten further (the darwin parser only accepts
// "utun" or "utunN").
const tunNameMax = 15

// OpenTun opens the host OS's kernel TUN device and returns it as a
// tun.PacketIface. name is the requested interface name — "" requests the
// next free name ("tun0" on Linux; on macOS the name is dictated by the
// kernel: "utun" or "" allocates the next free unit, "utunN" requests
// unit N). The actual allocated name is available via the TunIface
// assertion's Name().
//
// The returned interface satisfies the full PacketIface contract: Close
// unblocks a parked ReadPacket, after Close ReadPacket and WritePacket
// return net.ErrClosed, and Close is idempotent.
//
// Opening a TUN device needs CAP_NET_ADMIN (Linux) / root (macOS utun);
// a caller without it gets an error wrapping os.ErrPermission. On
// platforms with no implementation OpenTun returns ErrNotImplemented.
func OpenTun(name string) (tun.PacketIface, error) {
	if len(name) > tunNameMax {
		return nil, fmt.Errorf("moss-lan: interface name %q exceeds %d characters", name, tunNameMax)
	}
	iface, _, err := openTunOS(name)
	if err != nil {
		return nil, err
	}
	return iface, nil
}

// TunIface is a tun.PacketIface that knows its kernel interface name —
// every implemented OS returns one, so callers can log the name the
// kernel actually assigned (useful when the caller passed "").
type TunIface interface {
	tun.PacketIface
	// Name returns the kernel name of the interface ("tun3", "utun5").
	Name() string
}

// AddressConfigurer is the optional capability of a real OS interface: give
// it an intranet address and bring it up. OpenTun's device exists after
// creation but is administratively down with no address, so nothing routes to
// it until ConfigureAddress runs. A loopback edge does not implement it —
// there is no OS interface to configure — which is how callers distinguish a
// real device from an in-process one without a type registry.
type AddressConfigurer interface {
	// ConfigureAddress assigns ip/bits to the interface and marks it up.
	// Needs CAP_NET_ADMIN (Linux), root (macOS), or administrator
	// (Windows); a caller without it gets an error wrapping
	// os.ErrPermission.
	ConfigureAddress(ip net.IP, bits int) error
}

// interfaceName is the kernel name of a real device, or "loopback" for an
// injected edge with no name — the string that goes in operator-facing errors.
func interfaceName(iface tun.PacketIface) string {
	if named, ok := iface.(TunIface); ok {
		return named.Name()
	}
	return "loopback"
}

// asNetClosed maps the os package's closing error to the net.ErrClosed
// sentinel the PacketIface contract names: an os.File reports
// os.ErrClosed ("file already closed") after Close, while the contract
// (and tun.Loopback) speak net.ErrClosed. Everything else passes
// through unwrapped.
func asNetClosed(err error) error {
	if err != nil && errors.Is(err, os.ErrClosed) {
		return net.ErrClosed
	}
	return err
}
