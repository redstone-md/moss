package mesh

import (
	"context"
	"encoding/json"
	"net"
	"strconv"
	"time"

	"golang.org/x/net/ipv4"

	"moss/internal/nat"
)

const lanDiscoveryGroup = "239.255.77.77"

type lanBeacon struct {
	MeshID          string `json:"mesh_id"`
	PeerID          string `json:"peer_id"`
	ListenPort      int    `json:"listen_port"`
	NATType         string `json:"nat_type,omitempty"`
	PublicReachable bool   `json:"public_reachable,omitempty"`
	RelayCapable    bool   `json:"relay_capable,omitempty"`
}

func (n *Node) lanDiscoveryLoop(ctx context.Context) {
	defer n.wg.Done()
	groupAddr := &net.UDPAddr{IP: net.ParseIP(lanDiscoveryGroup), Port: n.config.LANDiscoveryPort}

	recvConn, err := lanDiscoveryReceiver(groupAddr)
	if err != nil {
		return
	}
	defer recvConn.Close()

	sendConn, err := net.ListenPacket("udp4", ":0")
	if err != nil {
		return
	}
	defer sendConn.Close()
	sendPacket := ipv4.NewPacketConn(sendConn)
	_ = sendPacket.SetMulticastTTL(1)
	_ = sendPacket.SetMulticastLoopback(true)

	done := make(chan struct{})
	go func() {
		defer close(done)
		n.lanDiscoveryReadLoop(ctx, recvConn)
	}()

	n.sendLANBeacon(sendPacket, groupAddr)
	ticker := time.NewTicker(time.Duration(n.config.LANDiscoveryMS) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			_ = recvConn.Close()
			_ = sendConn.Close()
			<-done
			return
		case <-ticker.C:
			n.sendLANBeacon(sendPacket, groupAddr)
		}
	}
}

func lanDiscoveryReceiver(groupAddr *net.UDPAddr) (net.PacketConn, error) {
	recvConn, err := net.ListenPacket("udp4", ":"+strconv.Itoa(groupAddr.Port))
	if err != nil {
		return nil, err
	}
	packetConn := ipv4.NewPacketConn(recvConn)
	for _, iface := range lanDiscoveryInterfaces() {
		_ = packetConn.JoinGroup(&iface, groupAddr)
	}
	return recvConn, nil
}

func lanDiscoveryInterfaces() []net.Interface {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil
	}
	selected := make([]net.Interface, 0, len(ifaces))
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 || iface.Flags&net.FlagMulticast == 0 {
			continue
		}
		selected = append(selected, iface)
	}
	return selected
}

func lanDiscoveryBroadcastAddrs() []*net.UDPAddr {
	ifaces := lanDiscoveryInterfaces()
	addrs := make([]*net.UDPAddr, 0, len(ifaces))
	seen := make(map[string]struct{})
	for _, iface := range ifaces {
		ifaceAddrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range ifaceAddrs {
			ipNet, ok := addr.(*net.IPNet)
			if !ok || ipNet.IP == nil {
				continue
			}
			ip4 := ipNet.IP.To4()
			mask := ipNet.Mask
			if ip4 == nil || len(mask) != 4 {
				continue
			}
			broadcast := net.IPv4(
				ip4[0]|^mask[0],
				ip4[1]|^mask[1],
				ip4[2]|^mask[2],
				ip4[3]|^mask[3],
			)
			key := broadcast.String()
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			addrs = append(addrs, &net.UDPAddr{IP: broadcast})
		}
	}
	return addrs
}

func (n *Node) lanDiscoveryReadLoop(ctx context.Context, conn net.PacketConn) {
	buffer := make([]byte, 2048)
	for {
		_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		length, src, err := conn.ReadFrom(buffer)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			return
		}
		udpSrc, ok := src.(*net.UDPAddr)
		if !ok {
			continue
		}
		var beacon lanBeacon
		if err := json.Unmarshal(buffer[:length], &beacon); err != nil {
			continue
		}
		n.handleLANBeacon(udpSrc, beacon)
	}
}

func (n *Node) sendLANBeacon(conn *ipv4.PacketConn, groupAddr *net.UDPAddr) {
	if conn == nil || groupAddr == nil {
		return
	}
	profile := n.natProfile.Load().(nat.Profile)
	payload, err := json.Marshal(lanBeacon{
		MeshID:          n.meshID,
		PeerID:          n.localPeerID(),
		ListenPort:      n.listenPort,
		NATType:         string(profile.Type),
		PublicReachable: profile.PublicReachable,
		RelayCapable:    n.supernodeReady(profile),
	})
	if err != nil {
		return
	}
	ifaces := lanDiscoveryInterfaces()
	if len(ifaces) == 0 {
		_, _ = conn.WriteTo(payload, nil, groupAddr)
		return
	}
	for _, iface := range ifaces {
		if err := conn.SetMulticastInterface(&iface); err != nil {
			continue
		}
		_, _ = conn.WriteTo(payload, nil, groupAddr)
	}
	for _, broadcast := range lanDiscoveryBroadcastAddrs() {
		broadcast.Port = groupAddr.Port
		_, _ = conn.WriteTo(payload, nil, broadcast)
	}
}

func (n *Node) handleLANBeacon(src *net.UDPAddr, beacon lanBeacon) {
	if src == nil || src.IP == nil || src.IP.IsUnspecified() {
		return
	}
	if beacon.MeshID != n.meshID || beacon.PeerID == "" || beacon.PeerID == n.localPeerID() || beacon.ListenPort <= 0 {
		return
	}
	addr := net.JoinHostPort(src.IP.String(), strconv.Itoa(beacon.ListenPort))
	shouldDial := false
	n.mu.Lock()
	current := n.knownPeers[beacon.PeerID]
	chosenAddr := preferredKnownPeerAddr(current, addr)
	acceptedLANAddr := chosenAddr == addr
	direct := current.direct
	lan := current.lan
	if acceptedLANAddr {
		direct = true
		lan = true
	}
	n.knownPeers[beacon.PeerID] = knownPeer{
		id:              beacon.PeerID,
		addr:            chosenAddr,
		direct:          direct,
		lan:             lan,
		natType:         nat.Type(beacon.NATType),
		publicReachable: beacon.PublicReachable,
		relayCapable:    beacon.RelayCapable,
		lastSeen:        time.Now(),
		observations:    appendObservation(current.observations, addr),
		noiseStatic:     append([]byte(nil), current.noiseStatic...),
	}
	if acceptedLANAddr && n.started && len(n.peers) < n.config.MaxPeers {
		if _, connected := n.peers[beacon.PeerID]; !connected {
			cooldown := n.config.HandshakeTimeout()
			if cooldown < n.config.Heartbeat() {
				cooldown = n.config.Heartbeat()
			}
			if cooldown <= 0 {
				cooldown = time.Second
			}
			lastDial := n.peerDials[beacon.PeerID]
			if lastDial.IsZero() || time.Since(lastDial) >= cooldown {
				n.peerDials[beacon.PeerID] = time.Now()
				shouldDial = true
			}
		}
	}
	n.mu.Unlock()
	if shouldDial {
		go n.dialKnownPeer(beacon.PeerID, chosenAddr)
	}
}
