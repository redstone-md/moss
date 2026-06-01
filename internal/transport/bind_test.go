package transport

import (
	"net"
	"strconv"
	"strings"
	"testing"
)

func TestResolveBindInterfaceEmpty(t *testing.T) {
	idx, err := ResolveBindInterface("")
	if err != nil {
		t.Fatalf("empty spec must not error, got %v", err)
	}
	if idx != 0 {
		t.Fatalf("empty spec must resolve to 0, got %d", idx)
	}
}

func TestResolveBindInterfaceUnknownName(t *testing.T) {
	_, err := ResolveBindInterface("this-interface-should-never-exist-9999")
	if err == nil {
		t.Fatalf("unknown name must error")
	}
}

func TestResolveBindInterfaceLoopbackRejected(t *testing.T) {
	loopback, err := findLoopback()
	if err != nil {
		t.Skipf("no loopback found: %v", err)
	}
	if _, err := ResolveBindInterface(loopback.Name); err == nil {
		t.Fatalf("loopback name must be rejected")
	}
	if _, err := ResolveBindInterface(strconv.Itoa(loopback.Index)); err == nil {
		t.Fatalf("loopback index must be rejected")
	}
}

func TestResolveBindInterfaceAcceptsLiveNIC(t *testing.T) {
	iface, err := findUpNonLoopback()
	if err != nil {
		t.Skipf("no usable interface: %v", err)
	}
	gotIdx, err := ResolveBindInterface(iface.Name)
	if err != nil {
		t.Fatalf("name %q rejected: %v", iface.Name, err)
	}
	if gotIdx != iface.Index {
		t.Fatalf("name %q resolved to index %d, want %d", iface.Name, gotIdx, iface.Index)
	}
	gotIdx, err = ResolveBindInterface(strconv.Itoa(iface.Index))
	if err != nil {
		t.Fatalf("index %d rejected: %v", iface.Index, err)
	}
	if gotIdx != iface.Index {
		t.Fatalf("numeric spec resolved to %d, want %d", gotIdx, iface.Index)
	}
}

// findLoopback returns the first loopback interface available, or an error
// if none is enumerable (this should not happen on supported platforms).
func findLoopback() (*net.Interface, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	for i := range ifaces {
		if ifaces[i].Flags&net.FlagLoopback != 0 {
			return &ifaces[i], nil
		}
	}
	return nil, errNoInterface
}

// findUpNonLoopback returns the first interface that is up and not a
// loopback — the kind of NIC ResolveBindInterface should accept.
func findUpNonLoopback() (*net.Interface, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	for i := range ifaces {
		if ifaces[i].Flags&net.FlagLoopback != 0 {
			continue
		}
		if ifaces[i].Flags&net.FlagUp == 0 {
			continue
		}
		// Skip interfaces whose name contains "tunnel" / "vEthernet" hint at
		// virtual adapters — those still resolve fine but vary across CI
		// hosts and we want a stable pick.
		lower := strings.ToLower(ifaces[i].Name)
		if strings.Contains(lower, "tunnel") {
			continue
		}
		return &ifaces[i], nil
	}
	return nil, errNoInterface
}

var errNoInterface = &netError{msg: "no matching interface"}

type netError struct{ msg string }

func (e *netError) Error() string { return e.msg }
