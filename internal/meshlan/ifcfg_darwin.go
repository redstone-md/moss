//go:build darwin

// meshlan ifcfg_darwin.go: program the utun interface with its intranet
// address and bring it up, with ioctls only (no cgo, no shell-out) —
// the darwin twin of ifcfg_linux.go.
//
// A freshly connected utun exists but is administratively down and has no
// address: nothing routes to it until both are set. ifconfig(8) does this
// with SIOCAIFADDR (an ifaliasreq carrying address, destination, and mask)
// followed by SIOCSIFFLAGS; x/sys/unix exports the ioctl constants on
// darwin but not the ifreq/ifaliasreq structs, so they are defined here
// with the exact layouts <net/if.h> declares.
//
// utun is a point-to-point interface: the kernel wants a destination
// address, which wg-quick sets to the address itself ("ifconfig utunN a a")
// — the ioctls below set ifra_dstaddr identically, so the programmed state
// matches what the macOS tooling would produce, minus the shell-out.
package meshlan

import (
	"errors"
	"fmt"
	"net"
	"os"
	"unsafe"

	"golang.org/x/sys/unix"
)

// darwinIfaliasreq is ifaliasreq from <net/if.h>: SIOCAIFADDR's argument.
// Three sockaddr-sized slots (16 bytes each) follow the IFNAMSIZ name,
// making 64 bytes — exactly the size the SIOCAIFADDR request number
// encodes, so the kernel's copyin sees a complete struct.
type darwinIfaliasreq struct {
	Name [unix.IFNAMSIZ]byte
	Addr unix.RawSockaddrInet4
	Dst  unix.RawSockaddrInet4
	Mask unix.RawSockaddrInet4
}

// darwinIfreqFlags is the SIOC[GS]IFFLAGS ifreq: IFNAMSIZ name plus a
// 16-byte union whose first two bytes are the flag word. 32 bytes total,
// the size both flag ioctls encode.
type darwinIfreqFlags struct {
	Name  [unix.IFNAMSIZ]byte
	Flags uint16
	_     [14]byte
}

// darwinInet4Sockaddr packs an IPv4 address into the sockaddr_in layout
// the alias ioctls expect: sin_len set (sockaddr_in is 16 bytes), family
// AF_INET, port zero, address in network order.
func darwinInet4Sockaddr(v4 net.IP) unix.RawSockaddrInet4 {
	var sa unix.RawSockaddrInet4
	sa.Len = 16
	sa.Family = unix.AF_INET
	copy(sa.Addr[:], v4)
	return sa
}

// darwinIoctl issues one ioctl and maps errno to error. err == nil on
// success; err is the raw syscall.Errno otherwise (never a wrapped one,
// so callers can branch on it).
func darwinIoctl(fd int, req uintptr, arg unsafe.Pointer) error {
	if _, _, errno := unix.Syscall(unix.SYS_IOCTL, uintptr(fd), req, uintptr(arg)); errno != 0 {
		return errno
	}
	return nil
}

// darwinCfgErr wraps an ioctl failure for the operator, naming the
// interface and the privilege story. syscall.Errno's Is already maps
// EPERM/EACCES to os.ErrPermission, so the wrapped error satisfies the
// AddressConfigurer contract ("a caller without privilege gets an error
// wrapping os.ErrPermission") for a rootless run.
func darwinCfgErr(op, name string, err error) error {
	if errors.Is(err, os.ErrPermission) {
		return fmt.Errorf("moss-lan: %s %s (needs root): %w", op, name, err)
	}
	return fmt.Errorf("moss-lan: %s %s: %w", op, name, err)
}

// ConfigureAddress gives the utun interface ip/bits and marks it up. It
// satisfies AddressConfigurer; the receiver is the same utun device the
// edge uses, and t.name is the kernel-assigned name.
func (t *darwinTun) ConfigureAddress(ip net.IP, bits int) error {
	return configureInterfaceDarwin(t.name, ip, bits)
}

func configureInterfaceDarwin(name string, ip net.IP, bits int) error {
	v4 := ip.To4()
	if v4 == nil {
		return fmt.Errorf("moss-lan: %q address %v is not IPv4", name, ip)
	}
	if bits < 1 || bits > 30 {
		return fmt.Errorf("moss-lan: prefix length %d out of the supported 1..30 range", bits)
	}

	sock, err := unix.Socket(unix.AF_INET, unix.SOCK_DGRAM, 0)
	if err != nil {
		return fmt.Errorf("moss-lan: configuration socket: %w", err)
	}
	defer unix.Close(sock)

	// SIOCAIFADDR with the full alias triple. Dst = Addr because utun is
	// point-to-point: the destination address ifconfig/wg-quick give it
	// for an overlay net is the interface's own address.
	ifra := darwinIfaliasreq{
		Name: [unix.IFNAMSIZ]byte{},
		Addr: darwinInet4Sockaddr(v4),
		Dst:  darwinInet4Sockaddr(v4),
		Mask: darwinInet4Sockaddr(net.IP(net.CIDRMask(bits, 32)).To4()),
	}
	copy(ifra.Name[:], name)
	if err := darwinIoctl(sock, unix.SIOCAIFADDR, unsafe.Pointer(&ifra)); err != nil {
		return darwinCfgErr(fmt.Sprintf("SIOCAIFADDR %s %s/%d", name, v4, bits), name, err)
	}

	// Bring the interface up, merging IFF_UP into the current flags —
	// writing IFF_UP alone would clear whatever the kernel already set.
	var ifr darwinIfreqFlags
	copy(ifr.Name[:], name)
	if err := darwinIoctl(sock, unix.SIOCGIFFLAGS, unsafe.Pointer(&ifr)); err != nil {
		return darwinCfgErr(fmt.Sprintf("SIOCGIFFLAGS %s", name), name, err)
	}
	ifr.Flags |= unix.IFF_UP
	if err := darwinIoctl(sock, unix.SIOCSIFFLAGS, unsafe.Pointer(&ifr)); err != nil {
		return darwinCfgErr(fmt.Sprintf("SIOCSIFFLAGS %s up", name), name, err)
	}
	return nil
}
