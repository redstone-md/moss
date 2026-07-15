//go:build !windows

package transport

import "errors"

// newRawBlockingUDP is Windows-only. Everywhere else net.ListenUDP does not hit
// the IOCP-association failure that the raw fallback exists to work around, so
// this path is never taken; it returns an error purely to satisfy the call site.
func newRawBlockingUDP(port int, ifIndex int) (udpPacketConn, error) {
	return nil, errors.New("raw-socket UDP fallback is only implemented on Windows")
}
