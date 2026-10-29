package mesh

import "net"

// interfaceSnapshot is private to one address-selection or beacon operation.
type interfaceSnapshot struct {
	read         func() (interfaceView, error)
	view         interfaceView
	loaded       bool
	addrsByIndex map[int]interfaceAddresses
}

type interfaceView struct {
	interfaces []net.Interface
	addresses  func(net.Interface) ([]net.Addr, error)
}

type interfaceAddresses struct {
	addresses []net.Addr
	err       error
}

func (s *interfaceSnapshot) list() ([]net.Interface, error) {
	if s.loaded {
		return s.view.interfaces, nil
	}
	read := s.read
	if read == nil {
		read = readInterfaceView
	}
	view, err := read()
	if err != nil {
		return nil, err
	}
	s.view, s.loaded = view, true
	return view.interfaces, nil
}

func (s *interfaceSnapshot) addrs(iface net.Interface) ([]net.Addr, error) {
	if _, err := s.list(); err != nil {
		return nil, err
	}
	if previous, ok := s.addrsByIndex[iface.Index]; ok {
		return previous.addresses, previous.err
	}
	addresses, err := s.view.addresses(iface)
	if s.addrsByIndex == nil {
		s.addrsByIndex = make(map[int]interfaceAddresses)
	}
	s.addrsByIndex[iface.Index] = interfaceAddresses{addresses, err}
	return addresses, err
}

func (s *interfaceSnapshot) multicast() []net.Interface {
	ifaces, _ := s.list()
	return lanDiscoveryInterfacesFrom(ifaces)
}
