package mesh

import (
	"net"
	"testing"
)

func TestSelectAdvertiseHostPrefersPrivateIPv4(t *testing.T) {
	addrs := []net.Addr{
		&net.IPNet{IP: net.ParseIP("127.0.0.1"), Mask: net.CIDRMask(8, 32)},
		&net.IPNet{IP: net.ParseIP("fe80::1"), Mask: net.CIDRMask(64, 128)},
		&net.IPNet{IP: net.ParseIP("192.168.1.42"), Mask: net.CIDRMask(24, 32)},
		&net.IPNet{IP: net.ParseIP("8.8.8.8"), Mask: net.CIDRMask(24, 32)},
	}
	host, ok := selectAdvertiseHost(addrs)
	if !ok {
		t.Fatal("expected advertise host to be selected")
	}
	if got := host.String(); got != "192.168.1.42" {
		t.Fatalf("expected private IPv4 host, got %s", got)
	}
}

func TestSelectAdvertiseHostFallsBackToGlobalIPv4(t *testing.T) {
	addrs := []net.Addr{
		&net.IPNet{IP: net.ParseIP("127.0.0.1"), Mask: net.CIDRMask(8, 32)},
		&net.IPNet{IP: net.ParseIP("8.8.8.8"), Mask: net.CIDRMask(24, 32)},
	}
	host, ok := selectAdvertiseHost(addrs)
	if !ok {
		t.Fatal("expected advertise host to be selected")
	}
	if got := host.String(); got != "8.8.8.8" {
		t.Fatalf("expected global IPv4 fallback, got %s", got)
	}
}

func TestSelectAdvertiseHostForInterfacesSkipsVirtualOverlays(t *testing.T) {
	ifaces := []net.Interface{
		{Name: "ZeroTier One", Flags: net.FlagUp},
		{Name: "Tailscale", Flags: net.FlagUp},
		{Name: "Wi-Fi", Flags: net.FlagUp},
	}
	addrsByName := map[string][]net.Addr{
		"ZeroTier One": {
			&net.IPNet{IP: net.ParseIP("172.30.1.2"), Mask: net.CIDRMask(24, 32)},
		},
		"Tailscale": {
			&net.IPNet{IP: net.ParseIP("100.90.10.20"), Mask: net.CIDRMask(16, 32)},
		},
		"Wi-Fi": {
			&net.IPNet{IP: net.ParseIP("192.168.1.42"), Mask: net.CIDRMask(24, 32)},
		},
	}

	host, ok := selectAdvertiseHostForInterfacesFunc(ifaces, func(iface net.Interface) ([]net.Addr, error) {
		return addrsByName[iface.Name], nil
	})
	if !ok {
		t.Fatal("expected advertise host to be selected")
	}
	if got := host.String(); got != "192.168.1.42" {
		t.Fatalf("expected physical LAN host, got %s", got)
	}
}

func TestVirtualOverlayInterfaceNames(t *testing.T) {
	for _, name := range []string{
		"ZeroTier One",
		"Tailscale",
		"WireGuard Tunnel",
		"Wintun Userspace Tunnel",
		"Radmin VPN",
		"OpenVPN Connect",
		"vEthernet (WSL)",
		"VirtualBox Host-Only Ethernet Adapter",
		"VMware Network Adapter",
		"utun7",
		"wg0",
		"tun0",
		"tap1",
	} {
		if !isVirtualOverlayInterfaceName(name) {
			t.Fatalf("expected %q to be treated as virtual overlay interface", name)
		}
	}
	for _, name := range []string{"Wi-Fi", "Ethernet", "en0", "wlan0"} {
		if isVirtualOverlayInterfaceName(name) {
			t.Fatalf("expected %q to remain eligible as a normal interface", name)
		}
	}
}

func TestIsLoopbackHost(t *testing.T) {
	for _, host := range []string{"127.0.0.1", "::1", "localhost"} {
		if !isLoopbackHost(host) {
			t.Fatalf("expected %s to be recognized as loopback", host)
		}
	}
	if isLoopbackHost("192.168.1.10") {
		t.Fatal("expected private LAN address not to be recognized as loopback")
	}
}
