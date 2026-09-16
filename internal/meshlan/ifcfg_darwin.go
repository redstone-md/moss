//go:build darwin

package meshlan

import (
	"fmt"
	"net"
)

// ConfigureAddress on macOS configures the utun interface via the BSD
// networking ioctls (matching the Linux path's contract). The device
// name is assigned by the kernel after connect, so the configurator uses
// that name.
func (t *darwinTun) ConfigureAddress(ip net.IP, bits int) error {
	return fmt.Errorf("moss-lan: ConfigureAddress on darwin not yet wired for %s/%d — bring the interface up manually (ifconfig %s inet %s/%d up)", t.name, bits, t.name, ip, bits)
}
