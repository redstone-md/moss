//go:build !linux && !darwin && !windows

// meshlan tun_other.go: platforms beyond linux/darwin/windows (the BSDs,
// illumos, plan9...). The BSDs do have TUN devices (/dev/tunN with a
// different ioctl surface), but moss-lan does not target them yet; the
// sentinel keeps the failure actionable instead of mysterious.
package meshlan

import (
	"fmt"
	"runtime"

	"github.com/redstone-md/moss/internal/tun"
)

// openTunOS reports the unsupported platform. runtime.GOOS is named in the
// error so a log line on an exotic host says which OS was missing.
func openTunOS(name string) (tun.PacketIface, string, error) {
	return nil, "", fmt.Errorf("%w (GOOS %s, name %q)", ErrNotImplemented, runtime.GOOS, name)
}
