//go:build windows

package transport

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"

	"golang.org/x/sys/windows"
)

// rawBlockingUDP is a UDP socket built directly on blocking Winsock calls,
// bypassing Go's netpoller and its reliance on I/O completion ports. It exists
// as a fallback for hosts where net.ListenUDP fails because the socket cannot be
// associated with an IOCP — chiefly older Wine/Proton (Steam Deck and other
// Proton users). Native Winsock apps run fine there with plain blocking sockets,
// and so does this.
//
// Shutting a blocked Recvfrom down is the tricky part: on Windows, calling
// Closesocket on a socket that has a blocking recvfrom in flight does NOT
// interrupt it — the call parks forever (verified) — and SO_RCVTIMEO does not
// fire either. So Close instead wakes a parked reader with a loopback datagram
// to the socket's own bound port; the reader observes the closed flag and shuts
// the socket down itself. When no read is in flight there is nothing to unblock,
// so Close closes the socket directly.
type rawBlockingUDP struct {
	fd      windows.Handle
	local   *net.UDPAddr
	closed  atomic.Bool // set once shutdown begins
	reading atomic.Bool // true while a Recvfrom is in flight
	fdOnce  sync.Once
	fdErr   error
}

func newRawBlockingUDP(port int, ifIndex int) (udpPacketConn, error) {
	if port < 0 || port > 65535 {
		return nil, fmt.Errorf("raw udp: invalid port %d", port)
	}
	// dwFlags = 0 yields a non-overlapped socket, i.e. classic blocking semantics
	// that Wine supports without an IOCP.
	fd, err := windows.WSASocket(windows.AF_INET, windows.SOCK_DGRAM, windows.IPPROTO_UDP, nil, 0, 0)
	if err != nil {
		return nil, fmt.Errorf("raw udp: WSASocket: %w", err)
	}
	// Bind-to-interface (the VPN-escape feature) is not honoured on the raw
	// fallback path: it is a niche option and this path only runs on hosts where
	// the normal socket could not even bind. Ignore ifIndex rather than fail.
	_ = ifIndex

	sa := &windows.SockaddrInet4{Port: port} // Addr left zero → 0.0.0.0 (all NICs)
	if err := windows.Bind(fd, sa); err != nil {
		_ = windows.Closesocket(fd)
		return nil, fmt.Errorf("raw udp: bind :%d: %w", port, err)
	}
	local, err := rawLocalUDPAddr(fd)
	if err != nil {
		_ = windows.Closesocket(fd)
		return nil, err
	}
	return &rawBlockingUDP{fd: fd, local: local}, nil
}

func rawLocalUDPAddr(fd windows.Handle) (*net.UDPAddr, error) {
	sa, err := windows.Getsockname(fd)
	if err != nil {
		return nil, fmt.Errorf("raw udp: getsockname: %w", err)
	}
	sa4, ok := sa.(*windows.SockaddrInet4)
	if !ok {
		return nil, fmt.Errorf("raw udp: unexpected local sockaddr %T", sa)
	}
	return &net.UDPAddr{IP: net.IPv4(sa4.Addr[0], sa4.Addr[1], sa4.Addr[2], sa4.Addr[3]), Port: sa4.Port}, nil
}

func (c *rawBlockingUDP) ReadFromUDP(b []byte) (int, *net.UDPAddr, error) {
	for {
		if c.closed.Load() {
			c.closeFD()
			return 0, nil, net.ErrClosed
		}
		c.reading.Store(true)
		n, from, err := windows.Recvfrom(c.fd, b, 0)
		c.reading.Store(false)
		if c.closed.Load() {
			// Woke via the loopback datagram from Close (or a late real packet).
			c.closeFD()
			return 0, nil, net.ErrClosed
		}
		if err != nil {
			return 0, nil, err
		}
		if n < 0 {
			n = 0
		}
		sa4, ok := from.(*windows.SockaddrInet4)
		if !ok {
			// An AF_INET socket should only yield IPv4 sources; treat anything else
			// as an unaddressed datagram rather than crashing the read loop.
			return n, nil, nil
		}
		return n, &net.UDPAddr{IP: net.IPv4(sa4.Addr[0], sa4.Addr[1], sa4.Addr[2], sa4.Addr[3]), Port: sa4.Port}, nil
	}
}

func (c *rawBlockingUDP) WriteToUDP(b []byte, addr *net.UDPAddr) (int, error) {
	ip4 := addr.IP.To4()
	if ip4 == nil {
		return 0, fmt.Errorf("raw udp: non-IPv4 destination %v", addr.IP)
	}
	sa := &windows.SockaddrInet4{Port: addr.Port}
	copy(sa.Addr[:], ip4)
	if err := windows.Sendto(c.fd, b, 0, sa); err != nil {
		return 0, err
	}
	return len(b), nil
}

func (c *rawBlockingUDP) LocalAddr() net.Addr { return c.local }

// closeFD closes the underlying socket exactly once. It must only run when no
// Recvfrom is parked on the socket (either from the reader after a wake, or from
// Close when no read is in flight), since Closesocket cannot interrupt a blocked
// recvfrom on Windows.
func (c *rawBlockingUDP) closeFD() {
	c.fdOnce.Do(func() {
		c.fdErr = windows.Closesocket(c.fd)
	})
}

func (c *rawBlockingUDP) Close() error {
	if c.closed.Swap(true) {
		return c.fdErr
	}
	if c.reading.Load() {
		// A blocking Recvfrom is parked; wake it with a loopback datagram to our
		// own port. The reader sees closed==true and calls closeFD itself. We must
		// not Closesocket here — it would not interrupt the parked recvfrom.
		c.sendWake()
		return nil
	}
	// No read in flight: closing the socket is safe and won't strand anything.
	// (Any read that starts after this observes closed and never calls recvfrom.)
	c.closeFD()
	return c.fdErr
}

// sendWake delivers a one-byte datagram to our own bound port from a throwaway
// socket, so a reader parked in Recvfrom returns immediately.
func (c *rawBlockingUDP) sendWake() {
	sfd, err := windows.WSASocket(windows.AF_INET, windows.SOCK_DGRAM, windows.IPPROTO_UDP, nil, 0, 0)
	if err != nil {
		return
	}
	defer windows.Closesocket(sfd)
	_ = windows.Sendto(sfd, []byte{0}, 0, &windows.SockaddrInet4{Port: c.local.Port, Addr: [4]byte{127, 0, 0, 1}})
}
