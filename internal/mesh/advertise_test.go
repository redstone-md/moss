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
