package nat

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	ethnat "github.com/ethereum/go-ethereum/p2p/nat"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

func newUPnPBackend() routerInterface {
	return ethnat.UPnP()
}

func newNATPMPBackend() routerInterface {
	return ethnat.PMP(nil)
}

func newPCPBackend() routerInterface {
	return &pcpRouter{
		gateways: pcpPotentialGateways(),
		nonces:   make(map[string][12]byte),
	}
}

const (
	pcpVersion       = 2
	pcpOpcodeMap     = 1
	pcpResponseBit   = 0x80
	pcpProtocolTCP   = 6
	pcpProtocolUDP   = 17
	pcpMapPacketSize = 60
)

var (
	pcpResponseTimeout   = 2 * time.Second
	pcpServerPort        = 5351
	pcpPotentialGateways = potentialGateways
)

type pcpRouter struct {
	mu         sync.Mutex
	gateways   []net.IP
	chosen     net.IP
	externalIP net.IP
	nonces     map[string][12]byte
}

func (p *pcpRouter) AddMapping(protocol string, extport, intport int, name string, lifetime time.Duration) (uint16, error) {
	if lifetime <= 0 {
		return 0, errors.New("lifetime must not be <= 0")
	}
	proto, err := pcpProtocolNumber(protocol)
	if err != nil {
		return 0, err
	}
	if extport == 0 {
		extport = intport
	}
	nonce, err := randomPCPNonce()
	if err != nil {
		return 0, err
	}
	result, gateway, err := p.requestMap(proto, extport, intport, uint32(lifetime/time.Second), nonce)
	if err != nil {
		return 0, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.chosen = append(net.IP(nil), gateway...)
	p.externalIP = append(net.IP(nil), result.externalIP...)
	p.nonces[pcpNonceKey(protocol, result.externalPort, intport)] = nonce
	return uint16(result.externalPort), nil
}

func (p *pcpRouter) DeleteMapping(protocol string, extport, intport int) error {
	proto, err := pcpProtocolNumber(protocol)
	if err != nil {
		return err
	}
	p.mu.Lock()
	nonce, ok := p.nonces[pcpNonceKey(protocol, extport, intport)]
	gateway := append(net.IP(nil), p.chosen...)
	p.mu.Unlock()
	if !ok {
		return errors.New("pcp mapping nonce is unknown")
	}
	if len(gateway) == 0 {
		return errors.New("pcp gateway is unknown")
	}
	_, err = p.sendMapRequest(gateway, proto, extport, intport, 0, nonce)
	if err != nil {
		return err
	}
	p.mu.Lock()
	delete(p.nonces, pcpNonceKey(protocol, extport, intport))
	p.mu.Unlock()
	return nil
}

func (p *pcpRouter) ExternalIP() (net.IP, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.externalIP) == 0 {
		return nil, errors.New("pcp external ip is unknown")
	}
	return append(net.IP(nil), p.externalIP...), nil
}

type pcpMapResult struct {
	externalPort int
	externalIP   net.IP
}

func (p *pcpRouter) requestMap(proto byte, extport, intport int, lifetime uint32, nonce [12]byte) (pcpMapResult, net.IP, error) {
	p.mu.Lock()
	gateways := append([]net.IP(nil), p.gateways...)
	if len(p.chosen) != 0 {
		gateways = append([]net.IP{append(net.IP(nil), p.chosen...)}, gateways...)
	}
	p.mu.Unlock()
	tried := make(map[string]struct{}, len(gateways))
	for _, gateway := range gateways {
		if len(gateway) == 0 {
			continue
		}
		key := gateway.String()
		if _, ok := tried[key]; ok {
			continue
		}
		tried[key] = struct{}{}
		result, err := p.sendMapRequest(gateway, proto, extport, intport, lifetime, nonce)
		if err == nil {
			return result, gateway, nil
		}
	}
	return pcpMapResult{}, nil, errors.New("pcp mapping failed on all gateways")
}

func (p *pcpRouter) sendMapRequest(gateway net.IP, proto byte, extport, intport int, lifetime uint32, nonce [12]byte) (pcpMapResult, error) {
	remote := &net.UDPAddr{IP: gateway, Port: pcpServerPort}
	conn, err := net.DialUDP("udp4", nil, remote)
	if err != nil {
		return pcpMapResult{}, err
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(pcpResponseTimeout)); err != nil {
		return pcpMapResult{}, err
	}
	clientIP, err := pcpClientIP(conn.LocalAddr())
	if err != nil {
		return pcpMapResult{}, err
	}
	packet := make([]byte, pcpMapPacketSize)
	packet[0] = pcpVersion
	packet[1] = pcpOpcodeMap
	binary.BigEndian.PutUint32(packet[4:8], lifetime)
	copy(packet[8:24], clientIP)
	copy(packet[24:36], nonce[:])
	packet[36] = proto
	binary.BigEndian.PutUint16(packet[40:42], uint16(intport))
	binary.BigEndian.PutUint16(packet[42:44], uint16(extport))
	if _, err := conn.Write(packet); err != nil {
		return pcpMapResult{}, err
	}
	response := make([]byte, 1024)
	n, err := conn.Read(response)
	if err != nil {
		return pcpMapResult{}, err
	}
	result, _, err := parsePCPMapResponse(response[:n], proto, intport, nonce)
	return result, err
}

func parsePCPMapResponse(response []byte, proto byte, intport int, nonce [12]byte) (pcpMapResult, net.IP, error) {
	if len(response) < pcpMapPacketSize || len(response)%4 != 0 {
		return pcpMapResult{}, nil, errors.New("short pcp response")
	}
	if response[0] != pcpVersion {
		return pcpMapResult{}, nil, errors.New("unexpected pcp version")
	}
	if response[1] != (pcpResponseBit | pcpOpcodeMap) {
		return pcpMapResult{}, nil, errors.New("unexpected pcp opcode")
	}
	if response[3] != 0 {
		return pcpMapResult{}, nil, errors.New("pcp mapping rejected")
	}
	if !equalNonce(response[24:36], nonce) {
		return pcpMapResult{}, nil, errors.New("pcp nonce mismatch")
	}
	if response[36] != proto {
		return pcpMapResult{}, nil, errors.New("pcp protocol mismatch")
	}
	if got := int(binary.BigEndian.Uint16(response[40:42])); got != intport {
		return pcpMapResult{}, nil, errors.New("pcp internal port mismatch")
	}
	externalPort := int(binary.BigEndian.Uint16(response[42:44]))
	externalIP := fromPCPIP(response[44:60])
	if externalPort == 0 || externalIP == nil {
		return pcpMapResult{}, nil, errors.New("pcp mapping missing external endpoint")
	}
	return pcpMapResult{
		externalPort: externalPort,
		externalIP:   externalIP,
	}, nil, nil
}

func pcpProtocolNumber(protocol string) (byte, error) {
	switch strings.ToLower(protocol) {
	case "tcp":
		return pcpProtocolTCP, nil
	case "udp":
		return pcpProtocolUDP, nil
	default:
		return 0, errors.New("unsupported pcp protocol")
	}
}

func randomPCPNonce() ([12]byte, error) {
	var nonce [12]byte
	_, err := rand.Read(nonce[:])
	return nonce, err
}

func pcpClientIP(addr net.Addr) ([]byte, error) {
	udpAddr, ok := addr.(*net.UDPAddr)
	if !ok {
		return nil, errors.New("unexpected local udp address")
	}
	ip := udpAddr.IP
	if ip == nil {
		return nil, errors.New("local udp address has no ip")
	}
	return toPCPIP(ip), nil
}

func toPCPIP(ip net.IP) []byte {
	if ipv4 := ip.To4(); ipv4 != nil {
		out := make([]byte, net.IPv6len)
		out[10] = 0xff
		out[11] = 0xff
		copy(out[12:], ipv4)
		return out
	}
	ipv6 := ip.To16()
	if ipv6 == nil {
		return make([]byte, net.IPv6len)
	}
	return append([]byte(nil), ipv6...)
}

func fromPCPIP(raw []byte) net.IP {
	if len(raw) != net.IPv6len {
		return nil
	}
	ip := net.IP(append([]byte(nil), raw...))
	if v4 := ip.To4(); v4 != nil {
		return v4
	}
	return ip
}

func equalNonce(raw []byte, want [12]byte) bool {
	if len(raw) != len(want) {
		return false
	}
	for i, b := range want {
		if raw[i] != b {
			return false
		}
	}
	return true
}

func pcpNonceKey(protocol string, extport, intport int) string {
	return strings.ToLower(protocol) + "|" + strconv.Itoa(extport) + "|" + strconv.Itoa(intport)
}

func potentialGateways() (gws []net.IP) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil
	}
	for _, iface := range ifaces {
		ifaddrs, err := iface.Addrs()
		if err != nil {
			return gws
		}
		for _, addr := range ifaddrs {
			network, ok := addr.(*net.IPNet)
			if !ok || !network.IP.IsPrivate() {
				continue
			}
			ip := network.IP.Mask(network.Mask).To4()
			if ip == nil {
				continue
			}
			ip[3] |= 0x01
			gws = append(gws, ip)
		}
	}
	return gws
}
