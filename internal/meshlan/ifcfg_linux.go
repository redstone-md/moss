//go:build linux

// meshlan ifcfg_linux.go: program the kernel interface with its intranet
// address and bring it up, with ioctls only (no cgo, no shell-out).
//
// Opening a TUN device (tun_linux.go) yields an interface that exists but is
// administratively down and has no address: the OS will not route anything to
// it until both are set. That is the difference between "the mesh is up" and
// "ping 10.66.0.x reaches a peer".
//
// The three ioctls go over a throwaway AF_INET datagram socket — the standard
// way to talk to the network configuration plane for a named interface; the
// TUN fd itself is only a packet pipe. IFF_RUNNING is deliberately not forced:
// the kernel sets it for TUN devices once the fd is open.
package meshlan

import (
	"fmt"
	"net"

	"golang.org/x/sys/unix"
)

// ConfigureAddress gives the interface ip/bits and marks it up. It satisfies
// AddressConfigurer; the receiver is the same Linux device the edge uses.
func (t *linuxTun) ConfigureAddress(ip net.IP, bits int) error {
	return configureInterfaceLinux(t.name, ip, bits)
}

func configureInterfaceLinux(name string, ip net.IP, bits int) error {
	v4 := ip.To4()
	if v4 == nil {
		return fmt.Errorf("moss-lan: %q address %v is not IPv4", name, ip)
	}
	if bits < 1 || bits > 30 {
		return fmt.Errorf("moss-lan: prefix length %d out of the supported 1..30 range", bits)
	}

	sock, err := unix.Socket(unix.AF_INET, unix.SOCK_DGRAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("moss-lan: configuration socket: %w", err)
	}
	defer unix.Close(sock)

	// SIOCSIFADDR / SIOCSIFNETMASK take an ifreq whose union is a sockaddr_in.
	ifr, err := unix.NewIfreq(name)
	if err != nil {
		return fmt.Errorf("moss-lan: ifreq for %q: %w", name, err)
	}
	if err := ifr.SetInet4Addr(v4); err != nil {
		return fmt.Errorf("moss-lan: set address: %w", err)
	}
	if err := unix.IoctlIfreq(sock, unix.SIOCSIFADDR, ifr); err != nil {
		return fmt.Errorf("moss-lan: SIOCSIFADDR %q %s/%d (needs CAP_NET_ADMIN): %w", name, v4, bits, err)
	}

	if err := ifr.SetInet4Addr(net.IP(net.CIDRMask(bits, 32)).To4()); err != nil {
		return fmt.Errorf("moss-lan: set netmask: %w", err)
	}
	if err := unix.IoctlIfreq(sock, unix.SIOCSIFNETMASK, ifr); err != nil {
		return fmt.Errorf("moss-lan: SIOCSIFNETMASK %q /%d: %w", name, bits, err)
	}

	// SIOCGIFFLAGS / SIOCSIFFLAGS carry the flag word as a uint16, and the
	// get must be merged with IFF_UP: writing IFF_UP alone clears whatever the
	// kernel already had set.
	ifr.SetUint16(0)
	if err := unix.IoctlIfreq(sock, unix.SIOCGIFFLAGS, ifr); err != nil {
		return fmt.Errorf("moss-lan: SIOCGIFFLAGS %q: %w", name, err)
	}
	ifr.SetUint16(ifr.Uint16() | unix.IFF_UP)
	if err := unix.IoctlIfreq(sock, unix.SIOCSIFFLAGS, ifr); err != nil {
		return fmt.Errorf("moss-lan: SIOCSIFFLAGS %q up: %w", name, err)
	}
	return nil
}
