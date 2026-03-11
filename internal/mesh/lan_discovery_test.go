package mesh

import (
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
	if !info.direct {
		t.Fatal("expected LAN-discovered peer to be marked direct")
	}
	if info.natType != nat.TypePortRestricted {
		t.Fatalf("unexpected nat type %q", info.natType)
	}
	if !info.relayCapable {
		t.Fatal("expected relay-capable metadata to be preserved")
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
