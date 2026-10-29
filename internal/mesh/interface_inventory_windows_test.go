package mesh

import (
	"errors"
	"net"
	"reflect"
	"syscall"
	"testing"
	"unsafe"

	"golang.org/x/sys/windows"
)

func TestWindowsInventoryQueriesOnceAndOwnsValues(t *testing.T) {
	first := windowsAdapterFixture()
	calls := 0
	view, err := readWindowsInterfaceView(func(family, flags uint32, reserved uintptr, out *windows.IpAdapterAddresses, size *uint32) error {
		calls++
		if family != windows.AF_UNSPEC || flags != windows.GAA_FLAG_INCLUDE_PREFIX|windows.GAA_FLAG_INCLUDE_GATEWAYS || reserved != 0 {
			t.Fatal("adapter query changed family, flags or reserved argument")
		}
		*out = *first
		return nil
	})
	if err != nil || calls != 1 || len(view.interfaces) != 2 {
		t.Fatalf("calls=%d interfaces=%d err=%v", calls, len(view.interfaces), err)
	}
	iface := view.interfaces[0]
	want := net.Interface{Index: 7, MTU: 1500, Name: "Ethernet", HardwareAddr: net.HardwareAddr{1, 2, 3, 4, 5, 6}, Flags: net.FlagUp | net.FlagRunning | net.FlagBroadcast | net.FlagMulticast}
	if !reflect.DeepEqual(iface, want) || view.interfaces[1].Index != 9 {
		t.Fatalf("metadata differs: %+v", view.interfaces)
	}
	addresses, err := view.addresses(iface)
	wantAddresses := []net.Addr{
		&net.IPNet{IP: net.IPv4(192, 168, 1, 42), Mask: net.CIDRMask(24, 32)},
		&net.IPNet{IP: net.ParseIP("::ffff:192.0.2.7"), Mask: net.CIDRMask(120, 128)},
		&net.IPAddr{IP: net.ParseIP("2001:db8::7")},
	}
	if err != nil || !reflect.DeepEqual(addresses, wantAddresses) {
		t.Fatalf("address types, order or masks differ: %#v err=%v", addresses, err)
	}
	*first.FriendlyName = 'X'
	first.PhysicalAddress[0] = 99
	first.FirstUnicastAddress.Address.Sockaddr.Addr.Family = 0
	if !reflect.DeepEqual(iface, want) || !reflect.DeepEqual(addresses, wantAddresses) {
		t.Fatal("inventory aliases native storage")
	}
	if _, err := view.addresses(view.interfaces[1]); err != nil || calls != 1 {
		t.Fatalf("per-interface lookup queried again: calls=%d err=%v", calls, err)
	}
}

func TestWindowsInventoryBufferGrowthAndErrors(t *testing.T) {
	for _, mode := range []string{"grow", "denied", "overflow-without-growth", "empty"} {
		t.Run(mode, func(t *testing.T) {
			calls := 0
			view, err := readWindowsInterfaceView(func(_, _ uint32, _ uintptr, out *windows.IpAdapterAddresses, size *uint32) error {
				calls++
				switch mode {
				case "grow":
					if calls == 1 {
						*size = 30000
						return windows.ERROR_BUFFER_OVERFLOW
					}
					if *size != 30000 {
						t.Fatal("buffer did not grow")
					}
				case "denied":
					return windows.ERROR_ACCESS_DENIED
				case "overflow-without-growth":
					return windows.ERROR_BUFFER_OVERFLOW
				}
				*size = 0
				return nil
			})
			wantCalls := 1
			if mode == "grow" {
				wantCalls = 2
			}
			wantError := mode == "denied" || mode == "overflow-without-growth"
			if calls != wantCalls || (err != nil) != wantError || len(view.interfaces) != 0 {
				t.Fatalf("calls=%d err=%v interfaces=%v", calls, err, view.interfaces)
			}
		})
	}
}

func TestWindowsInventoryKeepsAddressFailureLocal(t *testing.T) {
	first := windowsAdapterFixture()
	first.FirstUnicastAddress.Address.Sockaddr.Addr.Family = 0xffff
	view := windowsInterfaceView(first)
	_, err := view.addresses(view.interfaces[0])
	var route *net.OpError
	if !errors.As(err, &route) || !errors.Is(err, syscall.EAFNOSUPPORT) {
		t.Fatalf("address failure changed shape: %v", err)
	}
	if addrs, err := view.addresses(view.interfaces[1]); err != nil || len(addrs) != 0 {
		t.Fatalf("one address failure invalidated another adapter: %v", err)
	}
}

func windowsAdapterFixture() *windows.IpAdapterAddresses {
	name, _ := windows.UTF16PtrFromString("Ethernet")
	first := &windows.IpAdapterAddresses{IfIndex: 7, FriendlyName: name, Mtu: 1500, OperStatus: windows.IfOperStatusUp, IfType: windows.IF_TYPE_ETHERNET_CSMACD, PhysicalAddressLength: 6, PhysicalAddress: [8]byte{1, 2, 3, 4, 5, 6}}
	first.Next = &windows.IpAdapterAddresses{Ipv6IfIndex: 9, IfType: windows.IF_TYPE_TUNNEL}
	first.FirstUnicastAddress = &windows.IpAdapterUnicastAddress{Address: windowsSocket4([4]byte{192, 168, 1, 42}), OnLinkPrefixLength: 24}
	first.FirstUnicastAddress.Next = &windows.IpAdapterUnicastAddress{Address: windowsSocket6(net.ParseIP("::ffff:192.0.2.7")), OnLinkPrefixLength: 120}
	first.FirstAnycastAddress = &windows.IpAdapterAnycastAddress{Address: windowsSocket6(net.ParseIP("2001:db8::7"))}
	return first
}

func windowsSocket4(ip [4]byte) windows.SocketAddress {
	raw := new(syscall.RawSockaddrAny)
	*(*syscall.RawSockaddrInet4)(unsafe.Pointer(raw)) = syscall.RawSockaddrInet4{Family: windows.AF_INET, Addr: ip}
	return windows.SocketAddress{Sockaddr: raw, SockaddrLength: int32(unsafe.Sizeof(syscall.RawSockaddrInet4{}))}
}

func windowsSocket6(ip net.IP) windows.SocketAddress {
	raw := new(syscall.RawSockaddrAny)
	value := (*syscall.RawSockaddrInet6)(unsafe.Pointer(raw))
	value.Family = windows.AF_INET6
	copy(value.Addr[:], ip)
	return windows.SocketAddress{Sockaddr: raw, SockaddrLength: int32(unsafe.Sizeof(*value))}
}
