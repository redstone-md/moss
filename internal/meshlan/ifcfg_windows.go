//go:build windows

// meshlan ifcfg_windows.go: the wintun adapter's address configuration.
//
// Wintun hands moss the data plane (the ring buffer), but the adapter's
// IP address lives in the Windows network stack, programmed through
// iphlpapi (CreateUnicastIpAddressEntry + interface activation) — the
// surface wireguard-windows wraps in its winipcfg package. x/sys/windows
// ships only the iphlpapi *row types, not the entry syscalls, so this
// edge reports the exact manual command instead of shelling out to
// netsh: the error names the adapter, the address, and the mask, and
// stays distinct from ErrNotImplemented (the OS is implemented; only
// the admin-gated address step is not).
package meshlan

import (
	"fmt"
	"net"
)

// ConfigureAddress on Windows reports the exact netsh command an
// administrator must run to program the adapter, since the native
// iphlpapi path is not wired into the edge yet. It satisfies
// AddressConfigurer (so LanNode.Start surfaces this error at startup
// instead of running an addressless adapter).
func (t *wintunTun) ConfigureAddress(ip net.IP, bits int) error {
	v4 := ip.To4()
	if v4 == nil {
		return fmt.Errorf("moss-lan: %s address %v is not IPv4", t.name, ip)
	}
	if bits < 1 || bits > 30 {
		return fmt.Errorf("moss-lan: prefix length %d out of the supported 1..30 range", bits)
	}
	mask := net.IP(net.CIDRMask(bits, 32))
	return fmt.Errorf(
		"moss-lan: %s %s/%d: address setup not wired (needs iphlpapi); run as administrator: "+
			"netsh interface ip set address name=%q static %s %s && "+
			"netsh interface ip set interface %q admin=enabled",
		t.name, v4, bits, t.name, v4, mask, t.name)
}
