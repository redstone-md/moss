package mesh

import (
	"encoding/json"
	"net"
	"testing"

	"moss/internal/nat"
)

func TestHandleLANBeaconUpdatesKnownPeerWithSourceIP(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	node, err := NewNode("mesh-lan-beacon", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}

	node.handleLANBeacon(&net.UDPAddr{IP: net.ParseIP("192.168.50.20"), Port: 44445}, lanBeacon{
		MeshID:          "mesh-lan-beacon",
		PeerID:          "peer-1",
		ListenPort:      41030,
		NATType:         string(nat.TypePortRestricted),
		PublicReachable: false,
		RelayCapable:    true,
	})

	node.mu.RLock()
	defer node.mu.RUnlock()
	info, ok := node.knownPeers["peer-1"]
	if !ok {
		t.Fatal("expected known peer to be stored")
	}
	if info.addr != "192.168.50.20:41030" {
		t.Fatalf("unexpected known peer addr %q", info.addr)
	}
	if info.direct {
		t.Fatal("expected LAN-discovered peer to remain non-direct until handshake succeeds")
	}
	if info.natType != nat.TypePortRestricted {
		t.Fatalf("unexpected nat type %q", info.natType)
	}
	if !info.relayCapable {
		t.Fatal("expected relay-capable metadata to be preserved")
	}
}

func TestHandleLANBeaconPrefersAdvertisedExternalAddress(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	node, err := NewNode("mesh-lan-beacon", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}

	node.handleLANBeacon(&net.UDPAddr{IP: net.ParseIP("172.30.1.2"), Port: 44445}, lanBeacon{
		MeshID:         "mesh-lan-beacon",
		PeerID:         "peer-1",
		ListenPort:     41030,
		AdvertisedAddr: "100.64.74.9:41030",
	})

	node.mu.RLock()
	defer node.mu.RUnlock()
	info, ok := node.knownPeers["peer-1"]
	if !ok {
		t.Fatal("expected known peer to be stored")
	}
	if info.addr != "100.64.74.9:41030" {
		t.Fatalf("expected advertised endpoint to be preferred, got %q", info.addr)
	}
	if info.lan {
		t.Fatal("expected LAN marker to be cleared when advertised endpoint wins")
	}
}

func TestHandleLANBeaconDoesNotOverrideKnownPublicEndpoint(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	node, err := NewNode("mesh-lan-beacon", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}

	node.mu.Lock()
	node.knownPeers["peer-1"] = knownPeer{
		id:              "peer-1",
		addr:            "185.242.25.75:24598",
		publicReachable: true,
	}
	node.mu.Unlock()

	node.handleLANBeacon(&net.UDPAddr{IP: net.ParseIP("172.30.1.2"), Port: 44445}, lanBeacon{
		MeshID:          "mesh-lan-beacon",
		PeerID:          "peer-1",
		ListenPort:      41030,
		PublicReachable: false,
	})

	node.mu.RLock()
	defer node.mu.RUnlock()
	info := node.knownPeers["peer-1"]
	if info.addr != "185.242.25.75:24598" {
		t.Fatalf("expected public known peer addr to be preserved, got %q", info.addr)
	}
	if info.lan {
		t.Fatal("expected LAN marker to be cleared when public endpoint is preferred")
	}
	if info.direct {
		t.Fatal("expected peer to stay non-direct when LAN beacon was not accepted")
	}
}

func TestHandleLANBeaconIgnoresDifferentMeshAndSelf(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	node, err := NewNode("mesh-lan-beacon", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	selfID := node.localPeerID()

	node.handleLANBeacon(&net.UDPAddr{IP: net.ParseIP("10.1.2.3"), Port: 44445}, lanBeacon{
		MeshID:     "other-mesh",
		PeerID:     "peer-1",
		ListenPort: 41030,
	})
	node.handleLANBeacon(&net.UDPAddr{IP: net.ParseIP("10.1.2.4"), Port: 44445}, lanBeacon{
		MeshID:     "mesh-lan-beacon",
		PeerID:     selfID,
		ListenPort: 41031,
	})

	node.mu.RLock()
	defer node.mu.RUnlock()
	if len(node.knownPeers) != 0 {
		t.Fatalf("expected no known peers, got %d", len(node.knownPeers))
	}
}

func TestLANDiscoveryBroadcastCalculation(t *testing.T) {
	ip := net.IPv4(192, 168, 10, 24)
	mask := net.CIDRMask(24, 32)
	broadcast := net.IPv4(
		ip[12]|^mask[0],
		ip[13]|^mask[1],
		ip[14]|^mask[2],
		ip[15]|^mask[3],
	)
	if got := broadcast.String(); got != "192.168.10.255" {
		t.Fatalf("unexpected broadcast %q", got)
	}
}

func TestLANDiscoveryPayloadUsesAdvertisedAddr(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	node, err := NewNode("mesh-lan-beacon", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	node.listenPort = 41030
	node.natProfile.Store(nat.Profile{
		Type:            nat.TypePortRestricted,
		ExternalAddress: "100.64.74.9:41030",
		PublicReachable: false,
	})

	payload, err := node.lanDiscoveryPayload()
	if err != nil {
		t.Fatalf("lanDiscoveryPayload failed: %v", err)
	}
	var beacon lanBeacon
	if err := json.Unmarshal(payload, &beacon); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}
	if beacon.AdvertisedAddr != "100.64.74.9:41030" {
		t.Fatalf("expected advertised addr in beacon, got %q", beacon.AdvertisedAddr)
	}
}

func TestLANDiscoveryInterfacesSkipVirtualOverlays(t *testing.T) {
	ifaces := []net.Interface{
		{Name: "ZeroTier One", Flags: net.FlagUp | net.FlagMulticast},
		{Name: "Wintun", Flags: net.FlagUp | net.FlagMulticast},
		{Name: "Radmin VPN", Flags: net.FlagUp | net.FlagMulticast},
		{Name: "vEthernet (WSL)", Flags: net.FlagUp | net.FlagMulticast},
		{Name: "Wi-Fi", Flags: net.FlagUp | net.FlagMulticast},
		{Name: "Ethernet", Flags: net.FlagUp},
		{Name: "Loopback", Flags: net.FlagUp | net.FlagLoopback | net.FlagMulticast},
	}

	selected := lanDiscoveryInterfacesFrom(ifaces)
	if len(selected) != 1 {
		t.Fatalf("expected exactly one LAN discovery interface, got %d", len(selected))
	}
	if selected[0].Name != "Wi-Fi" {
		t.Fatalf("expected Wi-Fi to remain eligible, got %q", selected[0].Name)
	}
}
