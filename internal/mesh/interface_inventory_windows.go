// Copyright 2011 The Go Authors. All rights reserved.
// Windows mappings adapted from Go's net package; see docs/licenses/Go-BSD.txt.

package mesh

import (
	"errors"
	"net"
	"os"
	"runtime"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

type adapterQuery func(uint32, uint32, uintptr, *windows.IpAdapterAddresses, *uint32) error

func readInterfaceView() (interfaceView, error) {
	view, err := readWindowsInterfaceView(windows.GetAdaptersAddresses)
	if err != nil {
		err = &net.OpError{Op: "route", Net: "ip+net", Err: err}
	}
	return view, err
}

// One native inventory includes both interface metadata and all addresses.
// Convert to Go-owned values before the native backing buffer leaves scope.
func readWindowsInterfaceView(query adapterQuery) (interfaceView, error) {
	size := uint32(15000)
	for {
		buffer := make([]byte, size)
		// The OS writes aligned IP_ADAPTER_ADDRESSES records into this buffer;
		// Go allocations are aligned and nonmoving during the synchronous call.
		first := (*windows.IpAdapterAddresses)(unsafe.Pointer(&buffer[0]))
		flags := uint32(windows.GAA_FLAG_INCLUDE_PREFIX | windows.GAA_FLAG_INCLUDE_GATEWAYS)
		err := query(windows.AF_UNSPEC, flags, 0, first, &size)
		if err != nil {
			if errors.Is(err, windows.ERROR_BUFFER_OVERFLOW) && size > uint32(len(buffer)) {
				continue
			}
			return interfaceView{}, os.NewSyscallError("getadaptersaddresses", err)
		}
		if size == 0 {
			first = nil
		}
		view := windowsInterfaceView(first)
		runtime.KeepAlive(buffer)
		return view, nil
	}
}

func windowsInterfaceView(first *windows.IpAdapterAddresses) interfaceView {
	var interfaces []net.Interface
	addresses := make(map[int]interfaceAddresses)
	for adapter := first; adapter != nil; adapter = adapter.Next {
		iface := windowsInterface(adapter)
		interfaces = append(interfaces, iface)
		addrs, err := windowsInterfaceAddresses(adapter)
		if err != nil {
			err = &net.OpError{Op: "route", Net: "ip+net", Err: err}
		}
		addresses[iface.Index] = interfaceAddresses{addrs, err}
	}
	return interfaceView{interfaces, func(iface net.Interface) ([]net.Addr, error) {
		result := addresses[iface.Index]
		return result.addresses, result.err
	}}
}

func windowsInterface(adapter *windows.IpAdapterAddresses) net.Interface {
	index := adapter.IfIndex
	if index == 0 {
		index = adapter.Ipv6IfIndex
	}
	iface := net.Interface{Index: int(index), Name: windows.UTF16PtrToString(adapter.FriendlyName), MTU: int(adapter.Mtu)}
	if adapter.Mtu == 0xffffffff {
		iface.MTU = -1
	}
	if adapter.OperStatus == windows.IfOperStatusUp {
		iface.Flags = net.FlagUp | net.FlagRunning
	}
	switch adapter.IfType {
	case windows.IF_TYPE_ETHERNET_CSMACD, windows.IF_TYPE_ISO88025_TOKENRING, windows.IF_TYPE_IEEE80211, windows.IF_TYPE_IEEE1394:
		iface.Flags |= net.FlagBroadcast | net.FlagMulticast
	case windows.IF_TYPE_PPP, windows.IF_TYPE_TUNNEL:
		iface.Flags |= net.FlagPointToPoint | net.FlagMulticast
	case windows.IF_TYPE_SOFTWARE_LOOPBACK:
		iface.Flags |= net.FlagLoopback | net.FlagMulticast
	case windows.IF_TYPE_ATM:
		iface.Flags |= net.FlagBroadcast | net.FlagPointToPoint | net.FlagMulticast
	}
	if adapter.PhysicalAddressLength > 0 {
		iface.HardwareAddr = make(net.HardwareAddr, adapter.PhysicalAddressLength)
		copy(iface.HardwareAddr, adapter.PhysicalAddress[:])
	}
	return iface
}

func windowsInterfaceAddresses(adapter *windows.IpAdapterAddresses) ([]net.Addr, error) {
	var addresses []net.Addr
	for unicast := adapter.FirstUnicastAddress; unicast != nil; unicast = unicast.Next {
		ip, bits, err := windowsAddressIP(unicast.Address)
		if err != nil {
			return nil, err
		}
		if ip == nil {
			continue
		}
		addresses = append(addresses, &net.IPNet{IP: ip, Mask: net.CIDRMask(int(unicast.OnLinkPrefixLength), bits)})
	}
	for anycast := adapter.FirstAnycastAddress; anycast != nil; anycast = anycast.Next {
		ip, _, err := windowsAddressIP(anycast.Address)
		if err != nil {
			return nil, err
		}
		if ip != nil {
			addresses = append(addresses, &net.IPAddr{IP: ip})
		}
	}
	return addresses, nil
}

func windowsAddressIP(address windows.SocketAddress) (net.IP, int, error) {
	if address.Sockaddr == nil {
		return nil, 0, os.NewSyscallError("sockaddr", syscall.EINVAL)
	}
	parsed, err := address.Sockaddr.Sockaddr()
	if err != nil {
		return nil, 0, os.NewSyscallError("sockaddr", err)
	}
	switch value := parsed.(type) {
	case *syscall.SockaddrInet4:
		return net.IPv4(value.Addr[0], value.Addr[1], value.Addr[2], value.Addr[3]), 32, nil
	case *syscall.SockaddrInet6:
		return append(net.IP(nil), value.Addr[:]...), 128, nil
	default:
		return nil, 0, nil
	}
}
