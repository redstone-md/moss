//go:build !windows && !linux && !darwin

package transport

import "syscall"

// Stub for unsupported platforms (FreeBSD, OpenBSD, etc). Returns the
// sentinel so call sites can decide whether to fail or fall back.
func applyBindInterface(_ syscall.RawConn, ifIndex int) error {
	if ifIndex <= 0 {
		return nil
	}
	return ErrBindInterfaceUnsupported
}
