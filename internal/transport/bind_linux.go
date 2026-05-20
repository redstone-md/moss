//go:build linux

package transport

import (
	"fmt"
	"net"
	"syscall"

	"golang.org/x/sys/unix"
)

// applyBindInterface uses SO_BINDTODEVICE to lock the socket to a specific
// network interface. This requires CAP_NET_RAW; without it the setsockopt
// call returns EPERM and we surface it as a hard error.
func applyBindInterface(rc syscall.RawConn, ifIndex int) error {
	if ifIndex <= 0 {
		return nil
	}
	iface, err := net.InterfaceByIndex(ifIndex)
	if err != nil {
		return fmt.Errorf("bind interface: lookup index %d: %w", ifIndex, err)
	}
	var sockErr error
	ctrlErr := rc.Control(func(fd uintptr) {
		sockErr = unix.SetsockoptString(int(fd), unix.SOL_SOCKET, unix.SO_BINDTODEVICE, iface.Name)
	})
	if ctrlErr != nil {
		return fmt.Errorf("bind interface: RawConn.Control: %w", ctrlErr)
	}
	if sockErr != nil {
		return fmt.Errorf("bind interface: setsockopt(SO_BINDTODEVICE, %q): %w", iface.Name, sockErr)
	}
	return nil
}
