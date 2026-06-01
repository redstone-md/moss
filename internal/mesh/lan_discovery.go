package mesh

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"net"
	"sort"
	"strconv"
	"time"

	"golang.org/x/net/ipv4"

	"moss/internal/nat"
	"moss/internal/transport"
)

const lanDiscoveryGroup = "239.255.77.77"

const (
	lanPeerIDBytes             = 32
	lanBeaconGlobalBurst       = 128
	lanBeaconGlobalRate        = 32
	lanBeaconSourceBurst       = 16
	lanBeaconSourceRate        = 4
	lanBeaconMaxSources        = 256
	lanBeaconBucketTTL         = 10 * time.Minute
	lanDiscoveredPeerTTL       = 10 * time.Minute
	lanDiscoveredPeerCapFactor = 2
	lanDiscoveredPeerMinCap    = 16
)

type lanBeaconRateBucket struct {
	bucket   *nat.TokenBucket
	lastSeen time.Time
}

type lanBeacon struct {
	MeshID          string `json:"mesh_id"`
	PeerID          string `json:"peer_id"`
	ListenPort      int    `json:"listen_port"`
	AdvertisedAddr  string `json:"advertised_addr,omitempty"`
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
	if udpConn, ok := sendConn.(*net.UDPConn); ok {
		if bindErr := transport.ApplyBindToUDP(udpConn, n.bindIfIndex); bindErr != nil {
			_ = sendConn.Close()
			return
		}
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
	return lanDiscoveryInterfacesFrom(netInterfaces())
}

func lanDiscoveryInterfacesFrom(ifaces []net.Interface) []net.Interface {
	selected := make([]net.Interface, 0, len(ifaces))
	for _, iface := range ifaces {
		if !eligibleLocalInterface(iface) || iface.Flags&net.FlagMulticast == 0 {
			continue
		}
		selected = append(selected, iface)
	}
	return selected
}

func netInterfaces() []net.Interface {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil
	}
	return ifaces
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
		if n.handleLANBeacon(udpSrc, beacon) {
			n.sendLANBeaconReply(conn, &net.UDPAddr{IP: udpSrc.IP, Port: n.config.LANDiscoveryPort})
		}
	}
}

func (n *Node) lanDiscoveryPayload() ([]byte, error) {
	profile := n.natProfile.Load().(nat.Profile)
	return json.Marshal(lanBeacon{
		MeshID:          n.meshID,
		PeerID:          n.localPeerID(),
		ListenPort:      n.listenPort,
		AdvertisedAddr:  n.advertisedListenAddr(),
		NATType:         string(profile.Type),
		PublicReachable: profile.PublicReachable,
		RelayCapable:    n.supernodeReady(profile),
	})
}

func (n *Node) sendLANBeacon(conn *ipv4.PacketConn, groupAddr *net.UDPAddr) {
	if conn == nil || groupAddr == nil {
		return
	}
	payload, err := n.lanDiscoveryPayload()
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

func (n *Node) sendLANBeaconReply(conn net.PacketConn, dst *net.UDPAddr) {
	if conn == nil || dst == nil || dst.IP == nil || dst.IP.IsUnspecified() {
		return
	}
	payload, err := n.lanDiscoveryPayload()
	if err != nil {
		return
	}
	_, _ = conn.WriteTo(payload, dst)
}

func (n *Node) handleLANBeacon(src *net.UDPAddr, beacon lanBeacon) bool {
	if src == nil || src.IP == nil || src.IP.IsUnspecified() {
		return false
	}
	if !n.validLANBeacon(beacon) {
		return false
	}
	observedAddr := net.JoinHostPort(src.IP.String(), strconv.Itoa(beacon.ListenPort))
	candidateAddr := observedAddr
	shouldDial := false
	now := time.Now()
	n.mu.Lock()
	if !n.allowLANBeaconRateLocked(src.IP, now) {
		n.mu.Unlock()
		return false
	}
	current := n.knownPeers[beacon.PeerID]
	if current.id == "" && !n.allowNewLANPeerLocked(now) {
		n.mu.Unlock()
		return false
	}
	chosenAddr := preferredKnownPeerAddr(current, candidateAddr)
	lan := chosenAddr == observedAddr
	observations := appendObservation(current.observations, observedAddr)
	n.knownPeers[beacon.PeerID] = knownPeer{
		id:              beacon.PeerID,
		addr:            chosenAddr,
		direct:          current.direct,
		verified:        current.verified,
		lan:             lan,
		natType:         nat.Type(beacon.NATType),
		publicReachable: beacon.PublicReachable,
		relayCapable:    beacon.RelayCapable,
		lastSeen:        now,
		observations:    observations,
		noiseStatic:     append([]byte(nil), current.noiseStatic...),
	}
	if n.started && len(n.peers) < n.config.MaxPeers {
		if _, connected := n.peers[beacon.PeerID]; !connected {
			cooldown := n.config.HandshakeTimeout()
			if cooldown < n.config.Heartbeat() {
				cooldown = n.config.Heartbeat()
			}
			if cooldown <= 0 {
				cooldown = time.Second
			}
			lastDial := n.peerDials[beacon.PeerID]
			if lastDial.IsZero() || now.Sub(lastDial) >= cooldown {
				n.peerDials[beacon.PeerID] = now
				shouldDial = true
			}
		}
	}
	n.mu.Unlock()
	if shouldDial {
		go n.dialKnownPeer(beacon.PeerID, chosenAddr)
	}
	return true
}

func (n *Node) shouldReplyToLANBeacon(beacon lanBeacon) bool {
	return beacon.MeshID == n.meshID && validLANPeerID(beacon.PeerID) && beacon.PeerID != n.localPeerID() && beacon.ListenPort > 0
}

func (n *Node) validLANBeacon(beacon lanBeacon) bool {
	return n.shouldReplyToLANBeacon(beacon)
}

func validLANPeerID(peerID string) bool {
	if len(peerID) != lanPeerIDBytes*2 {
		return false
	}
	decoded, err := hex.DecodeString(peerID)
	return err == nil && len(decoded) == lanPeerIDBytes
}

func (n *Node) allowNewLANPeerLocked(now time.Time) bool {
	n.pruneLANPeersLocked(now)
	return n.lanPeerCountLocked() < lanPeerCap(n.config.MaxPeers)
}

func (n *Node) allowLANBeaconRateLocked(srcIP net.IP, now time.Time) bool {
	if n.lanBeaconGlobal == nil {
		n.lanBeaconGlobal = nat.NewTokenBucket(lanBeaconGlobalBurst, lanBeaconGlobalRate)
	}
	if !n.lanBeaconGlobal.Allow(1) {
		return false
	}
	if n.lanBeaconBuckets == nil {
		n.lanBeaconBuckets = make(map[string]*lanBeaconRateBucket)
	}
	for source, bucket := range n.lanBeaconBuckets {
		if now.Sub(bucket.lastSeen) > lanBeaconBucketTTL {
			delete(n.lanBeaconBuckets, source)
		}
	}
	source := srcIP.String()
	bucket := n.lanBeaconBuckets[source]
	if bucket == nil {
		if len(n.lanBeaconBuckets) >= lanBeaconMaxSources {
			return false
		}
		bucket = &lanBeaconRateBucket{bucket: nat.NewTokenBucket(lanBeaconSourceBurst, lanBeaconSourceRate)}
		n.lanBeaconBuckets[source] = bucket
	}
	bucket.lastSeen = now
	return bucket.bucket.Allow(1)
}

func (n *Node) pruneLANPeersLocked(now time.Time) {
	limit := lanPeerCap(n.config.MaxPeers)
	type candidate struct {
		peerID   string
		lastSeen time.Time
	}
	candidates := make([]candidate, 0, limit+1)
	for peerID, info := range n.knownPeers {
		if !prunableLANPeer(info) {
			continue
		}
		if now.Sub(info.lastSeen) > lanDiscoveredPeerTTL {
			delete(n.knownPeers, peerID)
			delete(n.peerDials, peerID)
			continue
		}
		candidates = append(candidates, candidate{peerID: peerID, lastSeen: info.lastSeen})
	}
	if len(candidates) <= limit {
		return
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].lastSeen.Before(candidates[j].lastSeen)
	})
	for _, candidate := range candidates[:len(candidates)-limit] {
		delete(n.knownPeers, candidate.peerID)
		delete(n.peerDials, candidate.peerID)
	}
}

func (n *Node) lanPeerCountLocked() int {
	count := 0
	for _, info := range n.knownPeers {
		if prunableLANPeer(info) {
			count++
		}
	}
	return count
}

func prunableLANPeer(info knownPeer) bool {
	return info.lan && !info.verified && !info.direct && !info.bootstrap
}

func lanPeerCap(maxPeers int) int {
	limit := maxPeers * lanDiscoveredPeerCapFactor
	if limit < lanDiscoveredPeerMinCap {
		return lanDiscoveredPeerMinCap
	}
	return limit
}
