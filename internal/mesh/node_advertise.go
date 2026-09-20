package mesh

import (
	"encoding/hex"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"time"

	"github.com/redstone-md/moss/internal/nat"
	"github.com/redstone-md/moss/internal/transport"
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
	// The default route's source address leads the local chain. Interface
	// preference is private-before-global (selectAdvertiseHost — deliberate,
	// LAN pairs), so on a public box with any bridge or tunnel up, the bridge
	// wins the advertise: a public stand box told the whole fleet it lived on
	// 172.26.0.1:43155, an address only its own containers could route to.
	// The egress probe names the interface the default route actually leaves
	// through, which is exactly the address a stranger must dial. A non-global
	// source (LAN egress, CGNAT) falls back to the chain below unchanged.
	if host, ok := n.defaultRouteAdvertiseHost(); ok {
		return net.JoinHostPort(host, strconv.Itoa(n.listenPort))
	}
	if host, ok := n.peerSubnetAdvertiseHost(); ok {
		return net.JoinHostPort(host, strconv.Itoa(n.listenPort))
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
	case strings.Contains(normalized, "netbird"):
		return true
	case strings.Contains(normalized, "nordlynx"):
		return true
	case strings.Contains(normalized, "mullvad"):
		return true
	case strings.Contains(normalized, "protonvpn"):
		return true
	case strings.Contains(normalized, "warp"):
		return true
	case strings.HasPrefix(normalized, "utun"):
		return true
	case strings.HasPrefix(normalized, "wg"):
		return true
	case strings.HasPrefix(normalized, "tun"):
		return true
	case strings.HasPrefix(normalized, "tap"):
		return true
	case strings.HasPrefix(normalized, "ppp"):
		return true
	case strings.HasPrefix(normalized, "xfrm"):
		return true
	case strings.HasPrefix(normalized, "ipsec"):
		return true
	case strings.HasPrefix(normalized, "zt"):
		return true
	case strings.HasPrefix(normalized, "br-"):
		// Docker user-defined networks: the default bridge is docker0 (caught
		// by the "docker" substring above), but a compose or custom network
		// creates br-<network id>. On the stand that bridge's 172.26.0.1 was
		// exactly what a public box advertised as its own address.
		return true
	case strings.HasPrefix(normalized, "veth"):
		// Container veth ends and Calico's cali* veths: host-side pair ends
		// of container links.
		return true
	case strings.HasPrefix(normalized, "cali"):
		return true
	case strings.HasPrefix(normalized, "virbr"):
		// libvirt's default NAT bridge — 192.168.122.1 on every KVM host.
		return true
	case strings.Contains(normalized, "podman"):
		return true
	case strings.Contains(normalized, "netavark"):
		return true
	case strings.Contains(normalized, "weave"):
		return true
	case strings.Contains(normalized, "flannel"):
		return true
	case strings.Contains(normalized, "cilium"):
		return true
	case strings.Contains(normalized, "cni"):
		// flannel's cni0 bridge and podman's cni-podman0.
		return true
	case strings.Contains(normalized, "ztnet"):
		return true
	default:
		return false
	}
}

// defaultRouteProbeEndpoint is the vantage point the egress probe connects
// to. A connected UDP socket sends no packet: the kernel resolves the route
// and LocalAddr reports the source it picked, so the probe is a local
// syscall pair, never a network round trip — and never evidence anyone was
// contacted.
const defaultRouteProbeEndpoint = "8.8.8.8:80"

// defaultRouteAdvertiseHostFn is the seam behind the advertise chain and the
// NAT gate. Production consults the OS default route via a bound UDP dial;
// tests substitute it to fabricate routing tables (the units stay hermetic).
var defaultRouteAdvertiseHostFn = func(bindIfIndex int) (string, bool) {
	// Pin the probe to the mesh's NIC when one is configured: the egress
	// must be the interface mesh traffic actually leaves through (the same
	// rule probeTCPAddress applies to reachability dials).
	conn, err := transport.DialerWithBind(net.Dialer{Timeout: 2 * time.Second}, bindIfIndex).Dial("udp", defaultRouteProbeEndpoint)
	if err != nil {
		return "", false
	}
	defer conn.Close()
	host, _, err := net.SplitHostPort(conn.LocalAddr().String())
	if err != nil || host == "" {
		return "", false
	}
	addr, err := netip.ParseAddr(host)
	if err != nil {
		return "", false
	}
	return addr.Unmap().String(), true
}

// defaultRouteAdvertiseHost returns the address a stranger's reply would
// come back through: the source the default route picks. Only a public
// source may lead the chain — a private one is a LAN egress that must keep
// the local chain in charge, and CGNAT/loopback are not dialable by anyone.
func (n *Node) defaultRouteAdvertiseHost() (string, bool) {
	host, ok := defaultRouteAdvertiseHostFn(n.bindIfIndex)
	if !ok || host == "" {
		return "", false
	}
	addr, err := netip.ParseAddr(host)
	if err != nil || !eligibleDefaultRouteAdvertiseHost(addr) {
		return "", false
	}
	return addr.String(), true
}

// eligibleDefaultRouteAdvertiseHost reports whether a default-route source
// address may lead the advertise chain.
func eligibleDefaultRouteAdvertiseHost(addr netip.Addr) bool {
	addr = addr.Unmap()
	return addr.IsValid() &&
		addr.IsGlobalUnicast() &&
		!addr.IsPrivate() &&
		!isCarrierGradeAddr(addr) &&
		!addr.IsLoopback() &&
		!addr.IsLinkLocalUnicast()
}

// observedOwnEgressAddr reports whether an observed binding address is this
// node's own egress endpoint: the host its default route leaves from, at the
// port its own listener sits on. A third party reporting back that exact
// endpoint is not evidence of a NAT mapping — nothing translated anything —
// and folding it into the binding classifier upgraded directly public hosts
// to port_restricted_cone forever: the public label only ever promotes from
// Unknown, so one cone downgrade locked it out. The host matching with a
// DIFFERENT port is a real mapping (a middlebox rewrote something) and is
// deliberately not covered by this gate.
func (n *Node) observedOwnEgressAddr(observed string) bool {
	host, port, err := net.SplitHostPort(observed)
	if err != nil || host == "" {
		return false
	}
	observedAddr, err := netip.ParseAddr(host)
	if err != nil {
		return false
	}
	observedPort, err := strconv.Atoi(port)
	if err != nil || observedPort != n.listenPort {
		return false
	}
	egress, ok := n.defaultRouteAdvertiseHost()
	if !ok {
		return false
	}
	egressAddr, err := netip.ParseAddr(egress)
	if err != nil {
		return false
	}
	return observedAddr.Unmap() == egressAddr
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

// snapshotDirectPeerHostAddrs returns the bare IP hosts of every connected
// direct peer. Callers must not hold n.mu.
func (n *Node) snapshotDirectPeerHostAddrs() []netip.Addr {
	n.mu.RLock()
	defer n.mu.RUnlock()
	hosts := make([]netip.Addr, 0, len(n.peers))
	for _, peer := range n.peers {
		if peer == nil || peer.relayed {
			continue
		}
		host, _, err := net.SplitHostPort(peer.addr)
		if err != nil || host == "" {
			continue
		}
		addr, err := netip.ParseAddr(host)
		if err != nil {
			continue
		}
		hosts = append(hosts, addr.Unmap())
	}
	return hosts
}

// peerSubnetAdvertiseHost picks the local address of the interface that hosts
// the most directly connected peers, when that choice is unambiguous. On a
// multi-homed box the "best" address by generic preference can sit on an
// interface the peers cannot route to; matching the interface to the subnet
// the peers actually come from is what makes the advertised address dialable
// for them.
func (n *Node) peerSubnetAdvertiseHost() (string, bool) {
	peerHosts := n.snapshotDirectPeerHostAddrs()
	if len(peerHosts) == 0 {
		return "", false
	}
	ifaces, err := net.Interfaces()
	if err != nil {
		return "", false
	}
	return selectAdvertiseHostForPeersFunc(ifaces, func(iface net.Interface) ([]net.Addr, error) {
		return iface.Addrs()
	}, peerHosts)
}

// selectAdvertiseHostForPeersFunc picks the address of the eligible interface
// whose subnets contain the most connected peers. A tie at the top is refused
// (falling back to the generic preference) and so is a match set of zero: the
// evidence must point at exactly one interface before it may override the
// default choice.
func selectAdvertiseHostForPeersFunc(
	ifaces []net.Interface,
	addrFn func(net.Interface) ([]net.Addr, error),
	peerHosts []netip.Addr,
) (string, bool) {
	bestHost := ""
	bestMatches := 0
	for _, iface := range ifaces {
		if !eligibleLocalInterface(iface) {
			continue
		}
		ifaceAddrs, err := addrFn(iface)
		if err != nil {
			continue
		}
		prefixes := make([]netip.Prefix, 0, len(ifaceAddrs))
		for _, addr := range ifaceAddrs {
			value, err := netip.ParsePrefix(addr.String())
			if err != nil {
				continue
			}
			prefixes = append(prefixes, value)
		}
		if len(prefixes) == 0 {
			continue
		}
		matches := 0
		for _, peerHost := range peerHosts {
			for _, prefix := range prefixes {
				if prefix.Contains(peerHost) {
					matches++
					break
				}
			}
		}
		if matches == 0 {
			continue
		}
		host, ok := selectAdvertiseHost(ifaceAddrs)
		if !ok {
			continue
		}
		if matches > bestMatches {
			bestMatches = matches
			bestHost = host.String()
		} else if matches == bestMatches && bestMatches > 0 {
			// Two interfaces host the same number of peers: the evidence does
			// not point at one interface, so it must not pick one for them.
			return "", false
		}
	}
	if bestMatches == 0 {
		return "", false
	}
	return bestHost, true
}
