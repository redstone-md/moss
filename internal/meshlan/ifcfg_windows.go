//go:build windows

// meshlan ifcfg_windows.go: program the wintun adapter with netsh.
//
// Wintun hands moss the data plane (ring buffer), but the IP lives in the
// Windows stack. The native path is iphlpapi (CreateUnicastIpAddressEntry),
// which x/sys/windows does not expose; netsh is the stable, cgo-free
// alternative that wireguard-go also falls back to in scripts. Two commands:
//   netsh interface ip set address name="moss0" static 10.66.0.x 255.255.255.0
//   netsh interface ip set interface "moss0" admin=enabled
// Both are run via exec.Command with split args (no shell), so the iface
// name cannot inject.
package meshlan

import (
	"fmt"
	"net"
	"os/exec"
	"strings"
)

// ConfigureAddress assigns ip/bits to the wintun adapter and brings it up
// via netsh. Requires an elevated (Administrator) shell; otherwise netsh
// fails and the error is surfaced with the exact command to retry.
func (t *wintunTun) ConfigureAddress(ip net.IP, bits int) error {
	v4 := ip.To4()
	if v4 == nil {
		return fmt.Errorf("moss-lan: %s address %v is not IPv4", t.name, ip)
	}
	if bits < 1 || bits > 30 {
		return fmt.Errorf("moss-lan: prefix length %d out of the supported 1..30 range", bits)
	}
	mask := net.IP(net.CIDRMask(bits, 32)).String()
	// 1) set address — name="moss0" must be one arg; netsh parses the = itself.
	c1 := exec.Command("netsh", "interface", "ip", "set", "address",
		fmt.Sprintf("name=%s", t.name), "static", v4.String(), mask)
	if out, err := c1.CombinedOutput(); err != nil {
		msg := strings.TrimSpace(string(out))
		if msg != "" {
			msg = ": " + msg
		}
		return fmt.Errorf("moss-lan: netsh set address %q %s/%d%s: %w (run as Administrator)", t.name, v4, bits, msg, err)
	}
	// 2) bring up — idempotent if already up.
	c2 := exec.Command("netsh", "interface", "ip", "set", "interface",
		fmt.Sprintf("name=%s", t.name), "admin=enabled")
	if out, err := c2.CombinedOutput(); err != nil {
		msg := strings.TrimSpace(string(out))
		if msg != "" {
			msg = ": " + msg
		}
		return fmt.Errorf("moss-lan: netsh set interface %q up%s: %w", t.name, msg, err)
	}
	return nil
}
