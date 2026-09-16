//go:build windows

package meshlan

import (
	"fmt"
	"net"
)

func (t *wintunTun) ConfigureAddress(ip net.IP, bits int) error {
	return fmt.Errorf("moss-lan: ConfigureAddress on windows for %s %s/%d: use netsh interface ip set address %q static %s %s", t.name, ip, bits, t.name, ip, net.IP(net.CIDRMask(bits, 32)))
}
