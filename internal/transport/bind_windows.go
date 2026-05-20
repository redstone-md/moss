//go:build windows

package transport

import (
	"encoding/binary"
	"fmt"
	"syscall"

	"golang.org/x/sys/windows"
)

// IP_UNICAST_IF is the Winsock option that forces outbound unicast packets to
// leave through a specific interface, overriding the routing table. This is
// what lets us bypass a VPN's default route without touching system routing.
//
// Winsock value: 31 at level IPPROTO_IP. The option takes the interface
// index as an int, *in network byte order* (a documented Windows quirk).
const winIPUnicastIF = 31

// applyBindInterface attaches the IP_UNICAST_IF socket option to the given
// raw connection. Errors from the inner setsockopt are surfaced so that a
// failing bind (e.g. interface offline at socket-creation time) becomes a
// hard error instead of silently routing traffic through the VPN tunnel.
func applyBindInterface(rc syscall.RawConn, ifIndex int) error {
	if ifIndex <= 0 {
		return nil
	}
	// Windows demands the interface index in network byte order.
	beIndex := make([]byte, 4)
	binary.BigEndian.PutUint32(beIndex, uint32(ifIndex))
	value := int(binary.LittleEndian.Uint32(beIndex))

	var sockErr error
	ctrlErr := rc.Control(func(fd uintptr) {
		sockErr = windows.SetsockoptInt(
			windows.Handle(fd),
			windows.IPPROTO_IP,
			winIPUnicastIF,
			value,
		)
	})
	if ctrlErr != nil {
		return fmt.Errorf("bind interface: RawConn.Control: %w", ctrlErr)
	}
	if sockErr != nil {
		return fmt.Errorf("bind interface: setsockopt(IP_UNICAST_IF, %d): %w", ifIndex, sockErr)
	}
	return nil
}
