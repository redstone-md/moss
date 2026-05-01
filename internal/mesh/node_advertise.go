package mesh

import (
	"encoding/hex"
	"net"
	"net/netip"
	"strconv"
	"strings"

	"moss/internal/nat"
)

func (n *Node) localPeerID() string {
	pub := n.identity.PublicKey()
	return hex.EncodeToString(pub[:])
}

func (n *Node) advertisedListenAddr() string {
	profile := n.natProfile.Load().(nat.Profile)
	if profile.ExternalAddress != "" {
		host, port, err := net.SplitHostPort(profile.ExternalAddress)
		if err == nil && host != "" && host != "::" && host != "[::]" {
			return net.JoinHostPort(host, port)
		}
	}
	if n.shouldAdvertiseLoopback() {
		return net.JoinHostPort("127.0.0.1", strconv.Itoa(n.listenPort))
	}
	if host, ok := bestLocalAdvertiseHost(); ok {
		return net.JoinHostPort(host, strconv.Itoa(n.listenPort))
	}
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(n.listenPort))
}

func (n *Node) announcePort() int {
	addr := n.advertisedListenAddr()
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return n.listenPort
	}
	parsed, err := strconv.Atoi(port)
	if err != nil || parsed <= 0 {
		return n.listenPort
	}
	return parsed
}

func (n *Node) shouldAdvertiseLoopback() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if len(n.peers) > 0 {
		allLoopback := true
		for _, peer := range n.peers {
			host, _, err := net.SplitHostPort(peer.addr)
			if err != nil || !isLoopbackHost(host) {
				allLoopback = false
				break
			}
		}
		if allLoopback {
			return true
		}
	}
	if len(n.config.StaticPeers) == 0 {
		return false
	}
	for _, peer := range n.config.StaticPeers {
		host, _, err := net.SplitHostPort(peer)
		if err != nil || !isLoopbackHost(host) {
			return false
		}
	}
	return true
}

func bestLocalAdvertiseHost() (string, bool) {
	ifaces, err := net.Interfaces()
	if err != nil {
		addrs, err := net.InterfaceAddrs()
		if err != nil {
			return "", false
		}
		best, ok := selectAdvertiseHost(addrs)
		if !ok {
			return "", false
		}
		return best.String(), true
	}
	best, ok := selectAdvertiseHostForInterfaces(ifaces)
	if !ok {
		return "", false
	}
	return best.String(), true
}

func selectAdvertiseHost(addrs []net.Addr) (netip.Addr, bool) {
	var private4 netip.Addr
	var global4 netip.Addr
	var private6 netip.Addr
	var global6 netip.Addr
	for _, addr := range addrs {
		parsed, ok := addrToNetip(addr)
		if !ok {
			continue
		}
		if parsed.IsLoopback() || parsed.IsLinkLocalUnicast() || parsed.IsLinkLocalMulticast() || parsed.IsMulticast() || parsed.IsUnspecified() {
			continue
		}
		switch {
		case parsed.Is4() && parsed.IsPrivate():
			if !private4.IsValid() {
				private4 = parsed
			}
		case parsed.Is4() && parsed.IsGlobalUnicast():
			if !global4.IsValid() {
				global4 = parsed
			}
		case parsed.Is6() && parsed.IsPrivate():
			if !private6.IsValid() {
				private6 = parsed
			}
		case parsed.Is6() && parsed.IsGlobalUnicast():
			if !global6.IsValid() {
				global6 = parsed
			}
		}
	}
	switch {
	case private4.IsValid():
		return private4, true
	case global4.IsValid():
		return global4, true
	case private6.IsValid():
		return private6, true
	case global6.IsValid():
		return global6, true
	default:
		return netip.Addr{}, false
	}
}

func selectAdvertiseHostForInterfaces(ifaces []net.Interface) (netip.Addr, bool) {
	return selectAdvertiseHostForInterfacesFunc(ifaces, func(iface net.Interface) ([]net.Addr, error) {
		return iface.Addrs()
	})
}

func selectAdvertiseHostForInterfacesFunc(ifaces []net.Interface, addrFn func(net.Interface) ([]net.Addr, error)) (netip.Addr, bool) {
	addrs := make([]net.Addr, 0, len(ifaces)*2)
	for _, iface := range ifaces {
		if !eligibleLocalInterface(iface) {
			continue
		}
		ifaceAddrs, err := addrFn(iface)
		if err != nil {
			continue
		}
		addrs = append(addrs, ifaceAddrs...)
	}
	return selectAdvertiseHost(addrs)
}

func addrToNetip(addr net.Addr) (netip.Addr, bool) {
	switch value := addr.(type) {
	case *net.IPNet:
		ip, ok := netip.AddrFromSlice(value.IP)
		return ip.Unmap(), ok
	case *net.IPAddr:
		ip, ok := netip.AddrFromSlice(value.IP)
		return ip.Unmap(), ok
	default:
		prefix, err := netip.ParsePrefix(addr.String())
		if err != nil {
			ip, err := netip.ParseAddr(addr.String())
			if err != nil {
				return netip.Addr{}, false
			}
			return ip.Unmap(), true
		}
		return prefix.Addr().Unmap(), true
	}
}

func eligibleLocalInterface(iface net.Interface) bool {
	if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 || iface.Flags&net.FlagPointToPoint != 0 {
		return false
	}
	return !isVirtualOverlayInterfaceName(iface.Name)
}

func isVirtualOverlayInterfaceName(name string) bool {
	normalized := strings.ToLower(strings.TrimSpace(name))
	switch {
	case normalized == "":
		return false
	case strings.Contains(normalized, "radmin"):
		return true
	case strings.Contains(normalized, "openvpn"):
		return true
	case strings.Contains(normalized, "vpn"):
		return true
	case strings.Contains(normalized, "zerotier"):
		return true
	case strings.Contains(normalized, "tailscale"):
		return true
	case strings.Contains(normalized, "wireguard"):
		return true
	case strings.Contains(normalized, "wintun"):
		return true
	case strings.Contains(normalized, "hamachi"):
		return true
	case strings.Contains(normalized, "virtualbox"):
		return true
	case strings.Contains(normalized, "vmware"):
		return true
	case strings.Contains(normalized, "hyper-v"):
		return true
	case strings.Contains(normalized, "vethernet"):
		return true
	case strings.Contains(normalized, "docker"):
		return true
	case strings.Contains(normalized, "wsl"):
		return true
	case strings.HasPrefix(normalized, "utun"):
		return true
	case strings.HasPrefix(normalized, "wg"):
		return true
	case strings.HasPrefix(normalized, "tun"):
		return true
	case strings.HasPrefix(normalized, "tap"):
		return true
	default:
		return false
	}
}

func isLoopbackHost(host string) bool {
	if host == "localhost" {
		return true
	}
	addr, err := netip.ParseAddr(host)
	if err != nil {
		return false
	}
	return addr.IsLoopback()
}
