//go:build windows

package transport

import (
	"net"
	"testing"

	"golang.org/x/sys/windows"
)

// TestApplyBindToUDPSetsUnicastIF confirms the helper actually flips
// IP_UNICAST_IF on the socket so callers can't be fooled by a silent no-op
// (a previous draft swapped the byte order and getsockopt would have
// returned a different index than asked for).
func TestApplyBindToUDPSetsUnicastIF(t *testing.T) {
	iface, err := findUpNonLoopback()
	if err != nil {
		t.Skipf("no usable interface: %v", err)
	}
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero})
	if err != nil {
		t.Fatalf("ListenUDP: %v", err)
	}
	defer conn.Close()
	if err := ApplyBindToUDP(conn, iface.Index); err != nil {
		t.Fatalf("ApplyBindToUDP: %v", err)
	}
	rc, err := conn.SyscallConn()
	if err != nil {
		t.Fatalf("SyscallConn: %v", err)
	}
	var got int
	var sockErr error
	if err := rc.Control(func(fd uintptr) {
		got, sockErr = windows.GetsockoptInt(
			windows.Handle(fd),
			windows.IPPROTO_IP,
			winIPUnicastIF,
		)
	}); err != nil {
		t.Fatalf("Control: %v", err)
	}
	if sockErr != nil {
		t.Fatalf("getsockopt(IP_UNICAST_IF): %v", sockErr)
	}
	// Asymmetric Winsock quirk: setsockopt expects the value in network
	// byte order but getsockopt returns it in host byte order, so the
	// observed int should match the raw interface index.
	if got != iface.Index {
		t.Fatalf("IP_UNICAST_IF = %d, want %d", got, iface.Index)
	}
}
