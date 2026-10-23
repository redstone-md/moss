package mesh

import (
	"errors"
	"net"
	"testing"

	"github.com/redstone-md/moss/internal/nat"
)

func TestInterfaceSnapshotPreservesPeerFallbackAndFilters(t *testing.T) {
	fakeDefaultRouteEgress(t, "", false)
	node, err := NewNode("snapshot-policy", nil, DefaultOfflineConfig())
	if err != nil {
		t.Fatal(err)
	}
	node.peers["peer"] = &peerConn{addr: "10.9.0.15:44"}
	reads, addressReads := 0, map[int]int{}
	snapshot := &interfaceSnapshot{read: func() (interfaceView, error) {
		reads++
		return interfaceView{[]net.Interface{
			{Index: 1, Name: "WireGuard", Flags: net.FlagUp},
			{Index: 2, Name: "Ethernet", Flags: net.FlagUp},
			{Index: 3, Name: "Wi-Fi", Flags: net.FlagUp},
		}, func(iface net.Interface) ([]net.Addr, error) {
			addressReads[iface.Index]++
			if iface.Index == 2 {
				return nil, errors.New("adapter unavailable")
			}
			return []net.Addr{&net.IPNet{IP: net.ParseIP("192.168.4.42"), Mask: net.CIDRMask(24, 32)}}, nil
		}}, nil
	}}
	if got := node.advertisedListenAddrWithInterfaces(snapshot); got != "192.168.4.42:0" {
		t.Fatalf("fallback selected %s", got)
	}
	if reads != 1 || addressReads[1] != 0 || addressReads[2] != 1 || addressReads[3] != 1 {
		t.Fatalf("inventory=%d address reads=%v", reads, addressReads)
	}
}

func TestInterfaceSnapshotRetriesFailedInventory(t *testing.T) {
	reads := 0
	snapshot := &interfaceSnapshot{read: func() (interfaceView, error) {
		reads++
		if reads == 1 {
			return interfaceView{}, errors.New("temporary failure")
		}
		return interfaceView{[]net.Interface{{Index: 1}}, func(net.Interface) ([]net.Addr, error) {
			return nil, nil
		}}, nil
	}}
	if got := snapshot.multicast(); len(got) != 0 {
		t.Fatal("failed inventory supplied interfaces")
	}
	if _, err := snapshot.addrs(net.Interface{Index: 1}); err != nil || reads != 2 {
		t.Fatalf("failed read was retained: reads=%d err=%v", reads, err)
	}
	if _, err := snapshot.list(); err != nil || reads != 2 {
		t.Fatalf("successful inventory reread: reads=%d err=%v", reads, err)
	}
}

func TestAdvertisedFastPathsDoNotReadInterfaceInventory(t *testing.T) {
	for _, path := range []string{"external", "loopback", "public-route"} {
		t.Run(path, func(t *testing.T) {
			node, err := NewNode("snapshot-fast", nil, DefaultOfflineConfig())
			if err != nil {
				t.Fatal(err)
			}
			switch path {
			case "external":
				node.natProfile.Store(nat.Profile{ExternalAddress: "203.0.113.4:1234"})
			case "loopback":
				node.config.StaticPeers = []string{"127.0.0.1:1234"}
			case "public-route":
				fakeDefaultRouteEgress(t, "203.0.113.4", true)
			}
			snapshot := &interfaceSnapshot{read: func() (interfaceView, error) {
				t.Fatal("fast path enumerated interfaces")
				return interfaceView{}, nil
			}}
			if got := node.advertisedListenAddrWithInterfaces(snapshot); got == "" {
				t.Fatal("empty fast-path address")
			}
		})
	}
}
