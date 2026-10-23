package mesh

import "net"

// interfaceSnapshot is private to one address-selection or beacon operation.
type interfaceSnapshot struct {
	read func() (interfaceView, error)
	view interfaceView
}

type interfaceView struct {
	interfaces []net.Interface
	addresses  func(net.Interface) ([]net.Addr, error)
}

func (s *interfaceSnapshot) list() ([]net.Interface, error) {
	read := s.read
	if read == nil {
		read = readInterfaceView
	}
	var err error
	s.view, err = read()
	return s.view.interfaces, err
}

func (s *interfaceSnapshot) addrs(iface net.Interface) ([]net.Addr, error) {
	return s.view.addresses(iface)
}

func (s *interfaceSnapshot) multicast() []net.Interface {
	ifaces, _ := s.list()
	return lanDiscoveryInterfacesFrom(ifaces)
}

func readInterfaceView() (interfaceView, error) {
	ifaces, err := net.Interfaces()
	return interfaceView{ifaces, func(iface net.Interface) ([]net.Addr, error) {
		return iface.Addrs()
	}}, err
}
