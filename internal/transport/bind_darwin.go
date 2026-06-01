//go:build darwin

package transport

import (
	"fmt"
	"syscall"

	"golang.org/x/sys/unix"
)

// IP_BOUND_IF is the macOS equivalent of Windows' IP_UNICAST_IF: it locks
// outbound unicast packets to a specific interface index, overriding the
// routing table. Defined in <netinet/in.h> as option 25 at IPPROTO_IP.
const darwinIPBoundIF = 25

func applyBindInterface(rc syscall.RawConn, ifIndex int) error {
	if ifIndex <= 0 {
		return nil
	}
	var sockErr error
	ctrlErr := rc.Control(func(fd uintptr) {
		sockErr = unix.SetsockoptInt(int(fd), unix.IPPROTO_IP, darwinIPBoundIF, ifIndex)
	})
	if ctrlErr != nil {
		return fmt.Errorf("bind interface: RawConn.Control: %w", ctrlErr)
	}
	if sockErr != nil {
		return fmt.Errorf("bind interface: setsockopt(IP_BOUND_IF, %d): %w", ifIndex, sockErr)
	}
	return nil
}
