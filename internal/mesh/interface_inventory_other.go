//go:build !windows

package mesh

import "net"

func readInterfaceView() (interfaceView, error) {
	ifaces, err := net.Interfaces()
	return interfaceView{ifaces, func(iface net.Interface) ([]net.Addr, error) {
		return iface.Addrs()
	}}, err
}
